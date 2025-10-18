package controllers

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/internal/reboot"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	rpmostreeclient "github.com/openshift-kni/lifecycle-agent/lca-cli/ostreeclient"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type IPConfigRollbackHandlerInterface interface {
	PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error)
	PostPivot(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error)
}

type IPConfigRollbackHandler struct {
	Client          client.Client
	NoncachedClient client.Reader
	RPMOstreeClient rpmostreeclient.IClient
	Executor        ops.Execute
	Ops             ops.Ops
	RebootClient    reboot.RebootIntf
}

func NewIPConfigRollbackHandler(
	client client.Client,
	noncachedClient client.Reader,
	rpmostreeClient rpmostreeclient.IClient,
	executor ops.Execute,
	ops ops.Ops,
	rebootClient reboot.RebootIntf,
) IPConfigRollbackHandlerInterface {
	return &IPConfigRollbackHandler{
		Client:          client,
		NoncachedClient: noncachedClient,
		RPMOstreeClient: rpmostreeClient,
		Executor:        executor,
		Ops:             ops,
		RebootClient:    rebootClient,
	}
}

func (r *IPConfigRollbackHandler) PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	if r.RPMOstreeClient == nil {
		return requeueWithError(fmt.Errorf("rpm-ostree client is not set"))
	}

	stateroot, err := r.RPMOstreeClient.GetUnbootedStaterootName()
	if err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Rollback),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			controllerutils.RollbackFailed+": "+err.Error(),
			ipc.Generation,
		)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Rollback),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			controllerutils.RollbackFailed+": "+err.Error(),
			ipc.Generation,
		)

		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(fmt.Errorf("failed to determine unbooted stateroot: %w", err))
	}

	if err := controllerutils.CopyLcaCliToHost(logger); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Rollback),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to copy lca-cli binary: %s", err.Error()),
			ipc.Generation,
		)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Rollback),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to copy lca-cli binary: %s", err.Error()),
			ipc.Generation,
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return doNotRequeue(), fmt.Errorf("failed to copy lca-cli binary: %w", err)
	}

	logger.Info("Scheduling lca-cli ip-config rollback via systemd-run", "stateroot", stateroot)

	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Rollback),
		controllerutils.ConditionReasons.InProgress,
		metav1.ConditionTrue,
		"lca-cli ip-config rollback scheduled",
		ipc.Generation,
	)
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	args := []string{
		"--property", "ExitType=cgroup",
		"--unit", "lca-ipconfig-rollback",
		"--description", "lifecycle-agent: ip-config rollback",
		"lca-cli", "ip-config", "rollback",
		"--stateroot", stateroot,
	}
	if _, err := r.Executor.Execute("systemd-run", args...); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Rollback),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			controllerutils.RollbackFailed+": "+err.Error(),
			ipc.Generation,
		)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Rollback),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			controllerutils.RollbackFailed+": "+err.Error(),
			ipc.Generation,
		)

		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(fmt.Errorf("failed to schedule ip-config rollback: %w", err))
	}

	return requeueWithShortInterval(), nil
}

func (r *IPConfigRollbackHandler) PostPivot(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	log.FromContext(ctx).Info("Starting health check after rollback")
	if err := CheckHealth(ctx, r.NoncachedClient, log.FromContext(ctx)); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Rollback),
			controllerutils.ConditionReasons.InProgress,
			metav1.ConditionTrue,
			fmt.Sprintf("Waiting for system to stabilize: %s", err.Error()),
			ipc.Generation,
		)

		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithHealthCheckInterval(), nil
	}

	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Rollback),
		controllerutils.ConditionReasons.Completed,
		metav1.ConditionFalse,
		controllerutils.RollbackCompleted,
		ipc.Generation,
	)
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Rollback),
		controllerutils.ConditionReasons.Completed,
		metav1.ConditionTrue,
		controllerutils.RollbackCompleted,
		ipc.Generation,
	)
	ipc.Status.ValidNextStages = validNextStages(ipc, r.RPMOstreeClient)

	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	return doNotRequeue(), nil
}

func (r *IPConfigReconciler) handleRollback(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("IPConfigRollback")
	logger.Info("Starting handleRollback")

	isBeforePivot := isTargetStaterootBooted(ipc, r.RPMOstreeClient)
	if isBeforePivot {
		phase, message, err := common.ReadIPConfigStatus(common.PathOutsideChroot(common.IPConfigRollbackStatusFile))
		if err != nil {
			return requeueWithError(fmt.Errorf("failed to read ip-config rollback status: %w", err))
		}

		switch phase {
		case common.IPConfigRunPhaseUnknown:
			return r.handleRollbackUnknown(ctx, ipc, logger)
		case common.IPConfigRunPhaseRunning:
			return r.handleRollbackRunning(logger)
		case common.IPConfigRunPhaseFailed:
			return r.handleRollbackFailed(ctx, ipc, message)
		case common.IPConfigRunPhaseSucceeded:
			return r.handleRollbackSucceeded(ctx, ipc)
		default:
			return requeueWithShortInterval(), nil
		}
	}

	logger.Info("Running PostPivot handler")
	result, err := r.RollbackHandler.PostPivot(ctx, ipc)
	if err != nil {
		return result, fmt.Errorf("failed to run post pivot: %w", err)
	}

	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Rollback),
		controllerutils.ConditionReasons.Completed,
		metav1.ConditionFalse,
		"Rollback completed",
		ipc.Generation,
	)
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Rollback),
		controllerutils.ConditionReasons.Completed,
		metav1.ConditionTrue,
		"Rollback completed",
		ipc.Generation,
	)

	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Info("PostPivot completed successfully")

	return result, nil
}

// per-phase handlers for IPConfig rollback status
func (r *IPConfigReconciler) handleRollbackUnknown(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	logger.Info("Rollback status unknown; scheduling rollback")
	return r.RollbackHandler.PrePivot(ctx, ipc, logger)
}

func (r *IPConfigReconciler) handleRollbackRunning(logger logr.Logger) (ctrl.Result, error) {
	logger.Info("ip-config rollback in progress; requeueing")
	return requeueWithShortInterval(), nil
}

func (r *IPConfigReconciler) handleRollbackFailed(ctx context.Context, ipc *ipcv1.IPConfig, message string) (ctrl.Result, error) {
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Rollback),
		controllerutils.ConditionReasons.Failed,
		metav1.ConditionFalse,
		fmt.Sprintf("ip-config rollback failed: %s", message),
		ipc.Generation,
	)
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Rollback),
		controllerutils.ConditionReasons.Failed,
		metav1.ConditionFalse,
		fmt.Sprintf("ip-config rollback failed: %s", message),
		ipc.Generation,
	)
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}
	return doNotRequeue(), fmt.Errorf("ip-config rollback failed: %s", message)
}

func (r *IPConfigReconciler) handleRollbackSucceeded(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Rollback),
		controllerutils.ConditionReasons.InProgress,
		metav1.ConditionTrue,
		"ip-config rollback completed. Rebooting to previous stateroot",
		ipc.Generation,
	)
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}
	// The CLI schedules the reboot; wait and requeue
	return requeueWithShortInterval(), nil
}
