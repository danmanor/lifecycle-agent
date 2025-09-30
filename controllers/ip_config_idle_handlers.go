package controllers

import (
	"context"
	"fmt"
	"os"

	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/internal/healthcheck"
	"github.com/openshift-kni/lifecycle-agent/internal/prep"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *IPConfigReconciler) handleIdle(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	if err := healthcheck.HealthChecks(ctx, r.NoncachedClient, log.FromContext(ctx)); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.Finalizing,
			metav1.ConditionFalse,
			controllerutils.Finalizing+": "+err.Error(),
			ipc.Generation,
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithHealthCheckInterval(), nil
	}

	if err := prep.DeleteIPConfigPrepareJob(ctx, r.Client, log.FromContext(ctx)); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.FinalizeFailed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to clean up ip-config prepare job: %v", err),
			ipc.Generation,
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithLongInterval(), nil
	}

	if err := CleanupUnbootedStateroots(log.FromContext(ctx), r.Ops, r.OstreeClient, r.RPMOstreeClient); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.FinalizeFailed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to clean up unbooted stateroots: %v", err),
			ipc.Generation,
		)

		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithLongInterval(), nil
	}

	// Best-effort cleanup of temporary lca-cli copy placed on the host during PrePivot
	lcaHostCopy := common.PathOutsideChroot("/var/usrlocal/bin/lca-cli")
	if err := os.Remove(lcaHostCopy); err != nil && !os.IsNotExist(err) {
		log.FromContext(ctx).Error(err, "Failed to remove temporary lca-cli host copy", "path", lcaHostCopy)
	}

	// Reset IPConfig status to Idle-like state
	controllerutils.ResetStatusConditions(&ipc.Status.Conditions, ipc.Generation)
	ipc.Status.ValidNextStages = validNextStages(ipc, r.RPMOstreeClient)
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	return requeueWithShortInterval(), nil
}
