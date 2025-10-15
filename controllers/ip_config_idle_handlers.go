package controllers

import (
	"context"
	"fmt"
	"os"

	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/internal/prep"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *IPConfigReconciler) handleIdle(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("IPConfigIdle")

	isAfterPivot := isTargetStaterootBooted(ipc, r.RPMOstreeClient)
	prepCond := controllerutils.GetIPInProgressCondition(ipc, ipcv1.IPStages.Prepare)
	confCond := controllerutils.GetIPInProgressCondition(ipc, ipcv1.IPStages.Configure)

	finalizeRequested := isAfterPivot ||
		(confCond != nil &&
			confCond.Status == metav1.ConditionTrue &&
			confCond.Reason == string(controllerutils.ConditionReasons.Completed))
	abortRequested := !finalizeRequested && (prepCond != nil || confCond != nil)

	if finalizeRequested {
		logger.Info("Idle requested for finalize; running health checks")
		if err := CheckHealth(ctx, r.NoncachedClient, logger); err != nil {
			msg := fmt.Sprintf("Waiting for system to stabilize: %s", err.Error())
			controllerutils.SetStatusCondition(&ipc.Status.Conditions,
				controllerutils.ConditionTypes.Idle,
				controllerutils.ConditionReasons.Finalizing,
				metav1.ConditionFalse,
				msg,
				ipc.Generation,
			)
			if err := r.Client.Status().Update(ctx, ipc); err != nil {
				return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
			}
			return requeueWithHealthCheckInterval(), nil
		}
	}

	if err := prep.DeleteIPConfigPrepareJob(ctx, r.Client, logger); err != nil {
		reason := controllerutils.ConditionReasons.FinalizeFailed
		if abortRequested && !finalizeRequested {
			reason = controllerutils.ConditionReasons.AbortFailed
		}
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.ConditionTypes.Idle,
			reason,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to clean up ip-config prepare job: %v", err),
			ipc.Generation,
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithLongInterval(), nil
	}

	if err := r.Ops.RemountSysroot(); err != nil {
		reason := controllerutils.ConditionReasons.FinalizeFailed
		if abortRequested && !finalizeRequested {
			reason = controllerutils.ConditionReasons.AbortFailed
		}
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.ConditionTypes.Idle,
			reason,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to remount sysroot: %v", err),
			ipc.Generation,
		)
		return requeueWithError(fmt.Errorf("failed to remount sysroot: %w", err))
	}

	if err := CleanupUnbootedStateroots(logger, r.Ops, r.OstreeClient, r.RPMOstreeClient); err != nil {
		reason := controllerutils.ConditionReasons.FinalizeFailed
		if abortRequested && !finalizeRequested {
			reason = controllerutils.ConditionReasons.AbortFailed
		}
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.ConditionTypes.Idle,
			reason,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to clean up unbooted stateroots: %v", err),
			ipc.Generation,
		)

		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithLongInterval(), nil
	}

	lcaHostCopy := common.PathOutsideChroot("/var/usrlocal/bin/lca-cli")
	if err := os.Remove(lcaHostCopy); err != nil && !os.IsNotExist(err) {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.ConditionTypes.Idle,
			controllerutils.ConditionReasons.FinalizeFailed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to remove temporary lca-cli host copy: %v", err),
			ipc.Generation,
		)

		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithShortInterval(), nil
	}

	controllerutils.ResetStatusConditions(&ipc.Status.Conditions, ipc.Generation)
	ipc.Status.ValidNextStages = validNextStages(ipc, r.RPMOstreeClient)
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	return doNotRequeue(), nil
}
