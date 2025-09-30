package controllers

import (
	"context"
	"fmt"
	"strings"

	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/internal/prep"
	kbatch "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *IPConfigReconciler) handlePrepare(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("IPConfigPrepare")
	logger.Info("Starting handlePrepare")

	if err := CheckHealth(ctx, r.NoncachedClient, logger.WithName("HealthCheck")); err != nil {
		msg := fmt.Sprintf("Waiting for system to stabilize before Prepare stage can continue: %s", err.Error())
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
	job, err := prep.GetIPConfigPrepareJob(ctx, r.Client, logger)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Launching a new ip-config prepare job")
			if _, err := prep.LaunchIPConfigPrepareJob(ctx, r.Client, ipc, r.Scheme, logger, ipv4Addr, ipv6Addr); err != nil {
				return requeueWithError(fmt.Errorf("failed to launch ip-config prepare job: %w", err))
			}

			controllerutils.SetStatusCondition(&ipc.Status.Conditions,
				controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
				controllerutils.ConditionReasons.InProgress,
				metav1.ConditionTrue,
				controllerutils.InProgress,
				ipc.Generation,
			)
			if err := r.Client.Status().Update(ctx, ipc); err != nil {
				return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
			}

			return requeueWithShortInterval(), nil
		}

		return requeueWithError(fmt.Errorf("failed to get ip-config prepare job: %w", err))
	}

	logger.Info("Verifying ip-config prepare job status")
	if job.GetDeletionTimestamp() != nil {
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

		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(fmt.Errorf("ip-config prepare job is marked for deletion"))
	}

	_, finishedType := common.IsJobFinished(job)
	switch finishedType {
	case "":
		common.LogPodLogs(job, logger, r.Clientset)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.InProgress,
			metav1.ConditionTrue,
			fmt.Sprintf("ip-config prepare job in progress. %s", getJobMetadataString(job)),
			ipc.Generation,
		)

		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithShortInterval(), nil
	case kbatch.JobFailed:
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionTrue,
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

		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(fmt.Errorf("ip-config prepare job failed"))
	case kbatch.JobComplete:
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.Completed,
			metav1.ConditionTrue,
			"ip-config prepare job completed",
			ipc.Generation,
		)

		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
	}

	if err := r.installIPConfigServiceToNewStateroot(ipc, logger); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionTrue,
			fmt.Sprintf("failed to install ip-config service to new stateroot: %s", err.Error()),
			ipc.Generation,
		)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to install ip-config service to new stateroot: %s", err.Error()),
			ipc.Generation,
		)

		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(err)
	}

	ipc.Status.ValidNextStages = validNextStages(ipc, r.RPMOstreeClient)
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

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
