package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-logr/logr"
	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	rpmostreeclient "github.com/openshift-kni/lifecycle-agent/lca-cli/ostreeclient"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *IPConfigReconciler) handleIdle(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("IPConfigIdle")

	finalizeRequested, abortRequested := r.computeIdleIntent(ipc)

	if res, requeue, err := r.ensureFinalizationHealthOrWait(ctx, logger, ipc, finalizeRequested); requeue {
		return res, err
	}

	if res, requeue, err := r.remountSysrootOrError(ipc, abortRequested, finalizeRequested); requeue {
		return res, err
	}

	if res, requeue, err := r.cleanupStaterootsOrRequeue(ctx, logger, ipc, abortRequested, finalizeRequested); requeue {
		return res, err
	}

	if res, requeue, err := r.removeHostLcaCliOrRequeue(ctx, ipc); requeue {
		return res, err
	}

	if res, requeue, err := r.cleanupWorkspaceOrRequeue(ctx, ipc, abortRequested, finalizeRequested); requeue {
		return res, err
	}

	return r.resetStatusAndSetValidNextStages(ctx, ipc)
}

// computeIdleIntent figures out whether we should finalize (post-pivot or configure completed)
// and whether an abort was requested (prepare/configure in-progress without finalize).
func (r *IPConfigReconciler) computeIdleIntent(ipc *ipcv1.IPConfig) (bool, bool) {
	isAfterPivot := isTargetStaterootBooted(ipc, r.RPMOstreeClient)
	prepCond := controllerutils.GetIPInProgressCondition(ipc, ipcv1.IPStages.Prep)
	confCond := controllerutils.GetIPInProgressCondition(ipc, ipcv1.IPStages.Config)

	finalizeRequested := isAfterPivot ||
		(confCond != nil &&
			confCond.Status == metav1.ConditionTrue &&
			confCond.Reason == string(controllerutils.ConditionReasons.Completed))
	abortRequested := !finalizeRequested && (prepCond != nil || confCond != nil)
	return finalizeRequested, abortRequested
}

// ensureFinalizationHealthOrWait runs health checks if finalize is requested; if the system
// is not yet stable, updates status and requests a requeue with a health-check interval.
func (r *IPConfigReconciler) ensureFinalizationHealthOrWait(
	ctx context.Context,
	logger logr.Logger,
	ipc *ipcv1.IPConfig,
	finalizeRequested bool,
) (ctrl.Result, bool, error) {
	if !finalizeRequested {
		return ctrl.Result{}, false, nil
	}

	logger.Info("Idle requested for finalize; running health checks")
	if err := CheckHealth(ctx, r.NoncachedClient, logger); err != nil {
		msg := fmt.Sprintf("Waiting for system to stabilize: %s", err.Error())
		controllerutils.SetIPIdleStatusInProgress(ipc, controllerutils.ConditionReasons.Finalizing, msg)
		if uerr := r.Client.Status().Update(ctx, ipc); uerr != nil {
			res, ierr := requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
			return res, true, ierr
		}
		return requeueWithHealthCheckInterval(), true, nil
	}
	return ctrl.Result{}, false, nil
}

// remountSysrootOrError remounts the sysroot and, on failure, sets status and returns a requeue-with-error.
func (r *IPConfigReconciler) remountSysrootOrError(
	ipc *ipcv1.IPConfig,
	abortRequested bool,
	finalizeRequested bool,
) (ctrl.Result, bool, error) {
	if err := r.Ops.RemountSysroot(); err != nil {
		reason := controllerutils.ConditionReasons.FinalizeFailed
		if abortRequested && !finalizeRequested {
			reason = controllerutils.ConditionReasons.AbortFailed
		}
		controllerutils.SetIPIdleStatusInProgress(ipc, reason, fmt.Sprintf("failed to remount sysroot: %v", err))
		res, ierr := requeueWithError(fmt.Errorf("failed to remount sysroot: %w", err))
		return res, true, ierr
	}
	return ctrl.Result{}, false, nil
}

// cleanupStaterootsOrRequeue removes unbooted stateroots and orphaned boot directories; on failure
// it updates status and requests a long requeue interval.
func (r *IPConfigReconciler) cleanupStaterootsOrRequeue(
	ctx context.Context,
	logger logr.Logger,
	ipc *ipcv1.IPConfig,
	abortRequested bool,
	finalizeRequested bool,
) (ctrl.Result, bool, error) {
	if err := r.cleanuoUnbootedStateroots(logger); err != nil {
		reason := controllerutils.ConditionReasons.FinalizeFailed
		if abortRequested && !finalizeRequested {
			reason = controllerutils.ConditionReasons.AbortFailed
		}
		controllerutils.SetIPIdleStatusInProgress(ipc, reason, fmt.Sprintf("failed to clean up unbooted stateroots: %v", err))

		if uerr := r.Client.Status().Update(ctx, ipc); uerr != nil {
			res, ierr := requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
			return res, true, ierr
		}

		return requeueWithLongInterval(), true, nil
	}

	return ctrl.Result{}, false, nil
}

// removeHostLcaCliOrRequeue removes the temporary lca-cli copy on the host; on failure updates
// status and requests a short requeue interval.
func (r *IPConfigReconciler) removeHostLcaCliOrRequeue(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
) (ctrl.Result, bool, error) {
	lcaHostCopy := common.PathOutsideChroot("/var/usrlocal/bin/lca-cli")
	if err := os.Remove(lcaHostCopy); err != nil && !os.IsNotExist(err) {
		controllerutils.SetIPIdleStatusInProgress(ipc, controllerutils.ConditionReasons.FinalizeFailed, fmt.Sprintf("failed to remove temporary lca-cli host copy: %v", err))

		if uerr := r.Client.Status().Update(ctx, ipc); uerr != nil {
			res, ierr := requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
			return res, true, ierr
		}

		return requeueWithShortInterval(), true, nil
	}
	return ctrl.Result{}, false, nil
}

// cleanupWorkspaceOrRequeue cleans up IPConfig workspace/files; on failure updates status and
// requests a short requeue interval.
func (r *IPConfigReconciler) cleanupWorkspaceOrRequeue(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	abortRequested bool,
	finalizeRequested bool,
) (ctrl.Result, bool, error) {
	if err := cleanupIPConfigFiles(); err != nil {
		reason := controllerutils.ConditionReasons.FinalizeFailed
		if abortRequested && !finalizeRequested {
			reason = controllerutils.ConditionReasons.AbortFailed
		}
		controllerutils.SetIPIdleStatusInProgress(ipc, reason, fmt.Sprintf("failed to cleanup workspace: %v", err))
		if uerr := r.Client.Status().Update(ctx, ipc); uerr != nil {
			res, ierr := requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
			return res, true, ierr
		}
		return requeueWithShortInterval(), true, nil
	}
	return ctrl.Result{}, false, nil
}

// resetStatusAndSetValidNextStages resets conditions and persists the valid next stages.
func (r *IPConfigReconciler) resetStatusAndSetValidNextStages(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	controllerutils.ResetStatusConditions(&ipc.Status.Conditions, ipc.Generation)
	validNextStages, err := validNextStages(ipc, r.RPMOstreeClient)
	if err != nil {
		return requeueWithError(fmt.Errorf("failed to get valid next stages: %w", err))
	}
	ipc.Status.ValidNextStages = validNextStages

	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	return doNotRequeue(), nil
}

func cleanupIPConfigFiles() error {
	if _, err := os.Stat(common.PathOutsideChroot(controllerutils.IPConfigWorkspacePath)); err != nil {
		return nil
	}
	if err := os.RemoveAll(common.PathOutsideChroot(controllerutils.IPConfigWorkspacePath)); err != nil {
		return fmt.Errorf("removing %s failed: %w", controllerutils.IPConfigWorkspacePath, err)
	}
	return nil
}

func (r *IPConfigReconciler) cleanuoUnbootedStateroots(logger logr.Logger) error {
	bootDirsToRemove, err := getBootDirectoriesToRemove(r.RPMOstreeClient)
	if err != nil {
		return fmt.Errorf("failed to determine boot directories to remove: %w", err)
	}

	for _, dirPath := range bootDirsToRemove {
		logger.Info("Removing orphaned boot directory", "path", dirPath)
		if err := os.RemoveAll(dirPath); err != nil {
			return fmt.Errorf("failed to remove boot directory %s: %w", dirPath, err)
		}
	}

	if err := CleanupUnbootedStateroots(logger, r.Ops, r.OstreeClient, r.RPMOstreeClient); err != nil {
		return fmt.Errorf("failed to clean up unbooted stateroots: %w", err)
	}

	if err := r.Ops.RemountBoot(); err != nil {
		return fmt.Errorf("failed to remount boot: %w", err)
	}

	return nil
}

func getBootDirectoriesToRemove(rpmOstreeClient rpmostreeclient.IClient) ([]string, error) {
	status, err := rpmOstreeClient.QueryStatus()
	if err != nil {
		return nil, fmt.Errorf("failed to query status with rpmostree: %w", err)
	}

	// Build the list of boot directories for unbooted deployments: /boot/ostree/<osname>-<checksum>
	seen := map[string]struct{}{}
	toRemove := make([]string, 0)

	for i := len(status.Deployments) - 1; i >= 0; i-- {
		deployment := &status.Deployments[i]
		if deployment.Booted {
			continue
		}
		if deployment.Checksum == "" || deployment.OSName == "" {
			continue
		}
		dirName := fmt.Sprintf("%s-%s", deployment.OSName, deployment.Checksum)
		if _, exists := seen[dirName]; exists {
			continue
		}
		seen[dirName] = struct{}{}

		toRemove = append(toRemove, common.PathOutsideChroot(filepath.Join("/boot/ostree", dirName)))
	}

	return toRemove, nil
}
