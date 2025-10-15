package controllers

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/internal/prep"
	"github.com/openshift-kni/lifecycle-agent/internal/reboot"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	kbatch "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type IPConfigPrepareHandlerInterface interface {
	PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error)
	PostPivot(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error)
}

type IPConfigPrepareHandler struct {
	Client          client.Client
	NoncachedClient client.Reader
	Executor        ops.Execute
	Ops             ops.Ops
	RebootClient    reboot.RebootIntf
	Scheme          *runtime.Scheme
	Clientset       *kubernetes.Clientset
}

func NewIPConfigPrepareHandler(
	client client.Client,
	noncachedClient client.Reader,
	executor ops.Execute,
	ops ops.Ops,
	rebootClient reboot.RebootIntf,
	scheme *runtime.Scheme,
	clientset *kubernetes.Clientset,
) IPConfigPrepareHandlerInterface {
	return &IPConfigPrepareHandler{
		Client:          client,
		NoncachedClient: noncachedClient,
		Executor:        executor,
		Ops:             ops,
		RebootClient:    rebootClient,
		Scheme:          scheme,
		Clientset:       clientset,
	}
}

func (r *IPConfigReconciler) handlePrepare(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("IPConfigPrepare")
	logger.Info("Starting handlePrepare")

	isBeforePivot := !isTargetStaterootBooted(ipc, r.RPMOstreeClient)

	if isBeforePivot {
		logger.Info("Running prepare PrePivot handler")
		return r.PrepareHandler.PrePivot(ctx, ipc, logger)
	}

	logger.Info("Running prepare PostPivot handler")
	result, err := r.PrepareHandler.PostPivot(ctx, ipc)
	if err != nil {
		return result, fmt.Errorf("failed to run PostPivot: %w", err)
	}

	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.Completed,
		metav1.ConditionFalse,
		"Preparation completed",
		ipc.Generation,
	)
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.Completed,
		metav1.ConditionTrue,
		"Preparation completed",
		ipc.Generation,
	)

	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Info("Prepare completed successfully")

	return result, nil
}

func (p *IPConfigPrepareHandler) PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	if err := CheckHealth(ctx, p.NoncachedClient, logger.WithName("HealthCheck")); err != nil {
		msg := fmt.Sprintf("Waiting for system to stabilize before starting preparation: %s", err.Error())
		logger.Info(msg)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.InProgress,
			metav1.ConditionTrue,
			msg,
			ipc.Generation,
		)
		return requeueWithHealthCheckInterval(), nil
	}

	ipv4Addr, ipv6Addr := getIPAddresses(ipc)

	logger.Info("Fetching ip-config prepare job")
	job, err := prep.GetIPConfigPrepareJob(ctx, p.Client, logger)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Launching a new ip-config prepare job")
			if _, err := prep.LaunchIPConfigPrepareJob(ctx, p.Client, ipc, p.Scheme, logger, ipv4Addr, ipv6Addr); err != nil {
				return requeueWithError(fmt.Errorf("failed to launch ip-config prepare job: %w", err))
			}

			controllerutils.SetStatusCondition(&ipc.Status.Conditions,
				controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
				controllerutils.ConditionReasons.InProgress,
				metav1.ConditionTrue,
				controllerutils.InProgress,
				ipc.Generation,
			)
			if err := p.Client.Status().Update(ctx, ipc); err != nil {
				return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
			}

			return requeueWithShortInterval(), nil
		}

		return requeueWithError(fmt.Errorf("failed to get ip-config prepare job: %w", err))
	}

	logger.Info("Verifying ip-config prepare job status")
	if job.GetDeletionTimestamp() != nil {
		return p.handlePrepareJobMarkedForDeletion(ctx, ipc, job)
	}

	_, finishedType := common.IsJobFinished(job)
	switch finishedType {
	case "":
		return p.handlePrepareJobInProgress(ctx, ipc, logger, job)
	case kbatch.JobFailed:
		return p.handlePrepareJobFailed(ctx, ipc, job)
	case kbatch.JobComplete:
		return p.handlePrepareJobCompleted(ctx, ipc, logger)
	}

	// shouldn't happen
	return requeueWithShortInterval(), nil
}

// Splits PrePivot job status handling for clarity
func (p *IPConfigPrepareHandler) handlePrepareJobMarkedForDeletion(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	job *kbatch.Job,
) (ctrl.Result, error) {
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.Failed,
		metav1.ConditionFalse,
		fmt.Sprintf("ip-config prepare job is marked for deletion. This is not allowed. %s", getJobMetadataString(job)),
		ipc.Generation,
	)

	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.Failed,
		metav1.ConditionFalse,
		fmt.Sprintf("ip-config prepare job is marked for deletion. This is not allowed. %s", getJobMetadataString(job)),
		ipc.Generation,
	)

	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	return requeueWithError(fmt.Errorf("ip-config prepare job is marked for deletion"))
}

func (p *IPConfigPrepareHandler) handlePrepareJobInProgress(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
	job *kbatch.Job,
) (ctrl.Result, error) {
	common.LogPodLogs(job, logger, p.Clientset)
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.InProgress,
		metav1.ConditionTrue,
		fmt.Sprintf("ip-config prepare job in progress. %s", getJobMetadataString(job)),
		ipc.Generation,
	)

	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	return requeueWithShortInterval(), nil
}

func (p *IPConfigPrepareHandler) handlePrepareJobFailed(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	job *kbatch.Job,
) (ctrl.Result, error) {
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.Failed,
		metav1.ConditionFalse,
		"ip-config prepare job failed",
		ipc.Generation,
	)
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.Failed,
		metav1.ConditionFalse,
		"ip-config prepare job failed",
		ipc.Generation,
	)

	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	return doNotRequeue(), fmt.Errorf("ip-config prepare job failed")
}

func (p *IPConfigPrepareHandler) handlePrepareJobCompleted(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.InProgress,
		metav1.ConditionTrue,
		"ip-config prepare job completed. Rebooting to new stateroot",
		ipc.Generation,
	)

	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	if p.RebootClient == nil {
		return requeueWithError(fmt.Errorf("reboot client is not set"))
	}

	logger.Info("PrePivot Completed. Rebooting to new stateroot")
	if err := p.RebootClient.RebootToNewStateRoot("ip-config"); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to reboot to new stateroot: %s", err.Error()),
			ipc.Generation,
		)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to reboot to new stateroot: %s", err.Error()),
			ipc.Generation,
		)

		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(fmt.Errorf("failed to reboot to new stateroot: %w", err))
	}
	// We should not reach here on successful reboot
	return doNotRequeue(), nil
}

func (p *IPConfigPrepareHandler) PostPivot(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	log.FromContext(ctx).Info("Starting health check for different components")
	if err := CheckHealth(ctx, p.NoncachedClient, log.FromContext(ctx)); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.InProgress,
			metav1.ConditionTrue,
			fmt.Sprintf("Waiting for system to stabilize in new stateroot: %s", err.Error()),
			ipc.Generation,
		)

		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithHealthCheckInterval(), nil
	}

	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.InProgress,
		metav1.ConditionTrue,
		"Cluster has stabilized in new stateroot",
		ipc.Generation,
	)

	return doNotRequeue(), nil
}

func getIPAddresses(ipc *ipcv1.IPConfig) (string, string) {
	ipv4Addr := ""
	if ipc.Spec.IPv4 != nil && ipc.Spec.IPv4.Address != "" {
		ipv4Addr = strings.Split(ipc.Spec.IPv4.Address, "/")[0]
	}
	ipv6Addr := ""
	if ipc.Spec.IPv6 != nil && ipc.Spec.IPv6.Address != "" {
		ipv6Addr = strings.Split(ipc.Spec.IPv6.Address, "/")[0]
	}
	return ipv4Addr, ipv6Addr
}
