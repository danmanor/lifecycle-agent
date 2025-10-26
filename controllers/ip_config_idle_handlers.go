package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-logr/logr"
	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	rpmostreeclient "github.com/openshift-kni/lifecycle-agent/lca-cli/ostreeclient"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *IPConfigReconciler) handleIdle(ctx context.Context, ipc *ipcv1.IPConfig) (res ctrl.Result, err error) {
	logger := log.FromContext(ctx).WithName("IPConfigIdle")
	logger.Info("Starting handleIdle")

	idleCond := meta.FindStatusCondition(ipc.Status.Conditions, string(controllerutils.ConditionTypes.Idle))
	if idleCond != nil && idleCond.Status == metav1.ConditionFalse &&
		(idleCond.Reason == string(controllerutils.ConditionReasons.FinalizeFailed) ||
			idleCond.Reason == string(controllerutils.ConditionReasons.AbortFailed)) {
		if done, cerr := r.checkIPManualCleanup(ctx, ipc); cerr != nil {
			return requeueWithShortInterval(), cerr
		} else if done {
			logger.Info("Manual cleanup annotation is found, removed annotation and retrying idle tasks")
		}
	}

	if isIPTransitionRequested(ipc) && ipc.Status.ValidNextStages != nil {
		if err := r.validateIPConfigStage(ipc); err != nil {
			controllerutils.SetIPIdleStatusFalse(
				ipc,
				controllerutils.ConditionReasons.InvalidTransition,
				fmt.Sprintf("invalid IPConfig stage: %s", ipc.Spec.Stage),
			)
			if err := r.Client.Status().Update(ctx, ipc); err != nil {
				return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
			}
			return doNotRequeue(), nil
		}
	}

	finalizeRequested, abortRequested := r.computeIdleIntent(ipc)
	logger.Info("Idle requested for finalize; running health checks")
	if err := CheckHealth(ctx, r.NoncachedClient, logger); err != nil {
		msg := fmt.Sprintf("Waiting for system to stabilize: %s", err.Error())
		controllerutils.SetIPIdleStatusFalse(ipc, controllerutils.ConditionReasons.Finalizing, msg)
		if uerr := r.Client.Status().Update(ctx, ipc); uerr != nil {
			res, ierr := requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
			return res, ierr
		}
		return requeueWithHealthCheckInterval(), fmt.Errorf("waiting for system to stabilize: %s", err.Error())
	}

	if err := r.Ops.RemountSysroot(); err != nil {
		reason := controllerutils.ConditionReasons.FinalizeFailed
		if abortRequested && !finalizeRequested {
			reason = controllerutils.ConditionReasons.AbortFailed
		}
		controllerutils.SetIPIdleStatusFalse(
			ipc,
			reason,
			fmt.Sprintf(
				"failed to remount sysroot: %v. Perform cleanup manually then add '%s' annotation to IPConfig CR to transition back to Idle",
				err,
				controllerutils.ManualCleanupAnnotation,
			),
		)
		if uerr := r.Client.Status().Update(ctx, ipc); uerr != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
		}

		return requeueWithError(fmt.Errorf("failed to remount sysroot: %w", err))
	}

	if err := r.cleanuoUnbootedStateroots(logger); err != nil {
		reason := controllerutils.ConditionReasons.FinalizeFailed
		if abortRequested && !finalizeRequested {
			reason = controllerutils.ConditionReasons.AbortFailed
		}
		controllerutils.SetIPIdleStatusFalse(
			ipc,
			reason,
			fmt.Sprintf(
				"failed to clean up unbooted stateroots: %v. Perform cleanup manually then add '%s' annotation to IPConfig CR to transition back to Idle",
				err,
				controllerutils.ManualCleanupAnnotation,
			),
		)
		if uerr := r.Client.Status().Update(ctx, ipc); uerr != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
		}

		return requeueWithError(fmt.Errorf("failed to clean up unbooted stateroots: %w", err))
	}

	lcaHostCopy := common.PathOutsideChroot("/var/usrlocal/bin/lca-cli")
	if err := os.Remove(lcaHostCopy); err != nil && !os.IsNotExist(err) {
		controllerutils.SetIPIdleStatusFalse(
			ipc,
			controllerutils.ConditionReasons.FinalizeFailed,
			fmt.Sprintf(
				"failed to remove temporary lca-cli host copy: %v. Perform cleanup manually then add '%s' annotation to IPConfig CR to transition back to Idle",
				err,
				controllerutils.ManualCleanupAnnotation,
			),
		)

		if uerr := r.Client.Status().Update(ctx, ipc); uerr != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
		}

		return requeueWithError(fmt.Errorf("failed to remove temporary lca-cli host copy: %w", err))
	}

	if err := cleanupIPConfigFiles(); err != nil {
		reason := controllerutils.ConditionReasons.FinalizeFailed
		if abortRequested && !finalizeRequested {
			reason = controllerutils.ConditionReasons.AbortFailed
		}
		controllerutils.SetIPIdleStatusFalse(
			ipc,
			reason,
			fmt.Sprintf(
				"failed to cleanup workspace: %v. Perform cleanup manually then add '%s' annotation to IPConfig CR to transition back to Idle",
				err,
				controllerutils.ManualCleanupAnnotation,
			),
		)
		if uerr := r.Client.Status().Update(ctx, ipc); uerr != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
		}
		return requeueWithError(fmt.Errorf("failed to cleanup workspace: %w", err))
	}

	controllerutils.ResetStatusConditions(&ipc.Status.Conditions, ipc.Generation)
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Info("handleIdle completed successfully")

	return doNotRequeue(), nil
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

func cleanupIPConfigFiles() error {
	if _, err := os.Stat(common.PathOutsideChroot(controllerutils.IPConfigWorkspacePath)); err != nil {
		return nil
	}
	if err := os.RemoveAll(common.PathOutsideChroot(controllerutils.IPConfigWorkspacePath)); err != nil {
		return fmt.Errorf("removing %s failed: %w", controllerutils.IPConfigWorkspacePath, err)
	}
	return nil
}

// checkIPManualCleanup looks for ManualCleanupAnnotation on the IPConfig CR. If present, it removes
// the annotation and returns true so the reconcile loop can retry idle tasks.
func (r *IPConfigReconciler) checkIPManualCleanup(ctx context.Context, ipc *ipcv1.IPConfig) (bool, error) {
	if _, ok := ipc.Annotations[controllerutils.ManualCleanupAnnotation]; ok {
		delete(ipc.Annotations, controllerutils.ManualCleanupAnnotation)
		if err := r.Client.Update(ctx, ipc); err != nil {
			return false, fmt.Errorf("failed to remove manual cleanup annotation from IPConfig: %w", err)
		}
		return true, nil
	}
	return false, nil
}

func (r *IPConfigReconciler) cleanuoUnbootedStateroots(logger logr.Logger) error {
	staterootsToRemove, err := getStaterootsToRemove(r.RPMOstreeClient)
	if err != nil {
		return fmt.Errorf("failed to determine stateroots to remove: %w", err)
	}
	logger.Info("Stateroots to remove", "stateroots", staterootsToRemove)

	if err := r.Ops.RemountBoot(); err != nil {
		return fmt.Errorf("failed to remount boot: %w", err)
	}

	if err := removeBootDirsByStaterootPrefixes(logger, staterootsToRemove); err != nil {
		return err
	}

	if err := CleanupUnbootedStateroots(logger, r.Ops, r.OstreeClient, r.RPMOstreeClient); err != nil {
		return fmt.Errorf("failed to clean up unbooted stateroots: %w", err)
	}

	return nil
}

// removeBootDirsByStaterootPrefixes removes directories under /boot/ostree that
// start with any of the given stateroot names followed by a hyphen.
func removeBootDirsByStaterootPrefixes(logger logr.Logger, staterootsToRemove []string) error {
	bootOstreePath := common.PathOutsideChroot("/boot/ostree")
	entries, err := os.ReadDir(bootOstreePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to list boot ostree directory %s: %w", bootOstreePath, err)
	}

	for _, stateroot := range staterootsToRemove {
		prefix := stateroot + "-"
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			if !strings.HasPrefix(name, prefix) {
				continue
			}
			dirPath := filepath.Join(bootOstreePath, name)
			logger.Info("Removing orphaned boot directory", "path", dirPath)
			if err := os.RemoveAll(dirPath); err != nil {
				return fmt.Errorf("failed to remove boot directory %s: %w", dirPath, err)
			}
		}
	}
	return nil
}

func getStaterootsToRemove(rpmOstreeClient rpmostreeclient.IClient) ([]string, error) {
	status, err := rpmOstreeClient.QueryStatus()
	if err != nil {
		return nil, fmt.Errorf("failed to query status with rpmostree: %w", err)
	}

	toRemove := make([]string, 0)

	for i := len(status.Deployments) - 1; i >= 0; i-- {
		deployment := &status.Deployments[i]
		if deployment.Booted {
			continue
		}
		toRemove = append(toRemove, deployment.OSName)
	}

	return toRemove, nil
}
