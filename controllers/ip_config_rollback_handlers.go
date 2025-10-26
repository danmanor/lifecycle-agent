package controllers

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/go-logr/logr"
	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/internal/reboot"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	rpmostreeclient "github.com/openshift-kni/lifecycle-agent/lca-cli/ostreeclient"
	lcautils "github.com/openshift-kni/lifecycle-agent/utils"
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
		controllerutils.SetIPRollbackStatusFailed(ipc, controllerutils.RollbackFailed+": "+err.Error())
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(fmt.Errorf("failed to determine unbooted stateroot: %w", err))
	}

	if err := r.Ops.RemountSysroot(); err != nil {
		controllerutils.SetIPRollbackStatusFailed(ipc, controllerutils.RollbackFailed+": "+err.Error())
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to remount sysroot: %w", err))
	}

	logger.Info("Save the IPConfig CR to the old state root before pivot")
	ipcsavePath := common.PathOutsideChroot(filepath.Join(common.GetStaterootPath(stateroot), controllerutils.IPCFilePath))
	if err := lcautils.MarshalToFile(ipc, ipcsavePath); err != nil {
		controllerutils.SetIPRollbackStatusFailed(ipc, controllerutils.RollbackFailed+": "+err.Error())
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to save IPConfig CR before pivot: %w", err))
	}

	logger.Info("Scheduling lca-cli ip-config rollback via systemd-run", "stateroot", stateroot)

	controllerutils.SetIPRollbackStatusInProgress(ipc, "IP configuration rollback is in progress")
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
		controllerutils.SetIPRollbackStatusFailed(
			ipc,
			controllerutils.RollbackFailed+": "+err.Error(),
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(fmt.Errorf("failed to schedule ip-config rollback: %w", err))
	}

	// should not reach here on successful ip-config rollback

	return doNotRequeue(), nil
}

func (r *IPConfigRollbackHandler) PostPivot(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	log.FromContext(ctx).Info("Starting health check after rollback")
	if err := CheckHealth(ctx, r.NoncachedClient, log.FromContext(ctx)); err != nil {
		controllerutils.SetIPRollbackStatusInProgress(
			ipc,
			fmt.Sprintf("Waiting for system to stabilize: %s", err.Error()),
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithHealthCheckInterval(), nil
	}

	controllerutils.SetIPRollbackStatusCompleted(ipc, controllerutils.RollbackCompleted)
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	return doNotRequeue(), nil
}

func (r *IPConfigReconciler) handleRollback(ctx context.Context, ipc *ipcv1.IPConfig) (res ctrl.Result, err error) {
	logger := log.FromContext(ctx).WithName("IPConfigRollback")
	logger.Info("Starting handleRollback")

	if isIPTransitionRequested(ipc) {
		if err := r.validateIPConfigStage(ipc); err != nil {
			controllerutils.SetIPRollbackStatusFailed(
				ipc,
				"invalid transition: "+string(ipc.Spec.Stage),
			)
			if err := r.Client.Status().Update(ctx, ipc); err != nil {
				return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
			}
			return doNotRequeue(), nil
		}
	}

	isBeforePivot := isTargetStaterootBooted(ipc, r.RPMOstreeClient)
	if isBeforePivot {
		phase, message, err := common.ReadIPConfigStatus(
			common.PathOutsideChroot(common.IPConfigRollbackStatusFile),
		)
		if err != nil {
			return requeueWithError(fmt.Errorf("failed to read ip-config rollback status: %w", err))
		}

		switch phase {
		case common.IPConfigRunPhaseUnknown:
			return r.handleRollbackUnknown(ctx, ipc, logger)
		case common.IPConfigRunPhaseRunning:
			return r.handleRollbackRunning(logger)
		case common.IPConfigRunPhaseFailed:
			return r.handleRollbackFailed(ctx, ipc, logger, message)
		case common.IPConfigRunPhaseSucceeded:
			return r.handleRollbackSucceeded(ctx, ipc)
		default:
			return requeueWithShortInterval(), nil
		}
	}

	logger.Info("Running PostPivot handler")
	result, err := r.RollbackHandler.PostPivot(ctx, ipc)
	if err != nil {
		return result, fmt.Errorf("failed to run rollback post pivot: %w", err)
	}

	controllerutils.SetIPRollbackStatusCompleted(ipc, "Rollback completed")
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Info("rollback completed successfully")

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

func (r *IPConfigReconciler) handleRollbackFailed(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
	message string,
) (ctrl.Result, error) {
	controllerutils.SetIPRollbackStatusFailed(ipc, fmt.Sprintf("ip-config rollback failed: %s", message))
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Error(fmt.Errorf("ip-config rollback failed: %s", message), "ip-config rollback failed")
	return doNotRequeue(), nil
}

func (r *IPConfigReconciler) handleRollbackSucceeded(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	controllerutils.SetIPRollbackStatusInProgress(
		ipc,
		"ip-config rollback completed",
	)
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	return requeueWithShortInterval(), nil
}
