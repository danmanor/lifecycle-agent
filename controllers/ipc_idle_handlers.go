package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-logr/logr"
	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/internal/ostreeclient"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	rpmostreeclient "github.com/openshift-kni/lifecycle-agent/lca-cli/ostreeclient"
	lcautils "github.com/openshift-kni/lifecycle-agent/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type IPConfigIdleStageHandler struct {
	Client          client.Client
	NoncachedClient client.Reader
	ChrootOps       ops.Ops
	OstreeClient    ostreeclient.IClient
	RPMOstreeClient rpmostreeclient.IClient
}

func NewIPConfigIdleStageHandler(
	client client.Client,
	noncachedClient client.Reader,
	chrootOps ops.Ops,
	ostreeClient ostreeclient.IClient,
	rpmOstreeClient rpmostreeclient.IClient,
) IPConfigStageHandler {
	return &IPConfigIdleStageHandler{
		Client:          client,
		NoncachedClient: noncachedClient,
		ChrootOps:       chrootOps,
		OstreeClient:    ostreeClient,
		RPMOstreeClient: rpmOstreeClient,
	}
}

func (h *IPConfigIdleStageHandler) Handle(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("IPConfigIdle")
	logger.Info("Starting handleIdle")

	recertCacheInterval := getRecertCacheInterval(ipc)
	if shouldRefresh := h.shouldRefreshRecertImage(ipc, recertCacheInterval); shouldRefresh {
		if err := h.refreshRecertImage(ctx, ipc, logger); err != nil {
			return requeueWithError(fmt.Errorf("failed to refresh recert image: %w", err))
		}

		if ipc.Annotations == nil {
			ipc.Annotations = map[string]string{}
		}

		ipc.Annotations[controllerutils.IPConfigRecertCacheLastRefreshAnnotation] = time.Now().UTC().Format(time.RFC3339)
		if err := h.Client.Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update last recert cache check annotation: %w", err))
		}
	}

	if err := h.handleManualCleanupIfFailed(ctx, ipc, logger); err != nil {
		return requeueWithError(fmt.Errorf("failed to handle manual cleanup if failed: %w", err))
	}

	if isIPTransitionRequested(ipc) && ipc.Status.ValidNextStages != nil {
		if err := validateIPConfigStage(ipc); err != nil {
			controllerutils.SetIPIdleStatusFalse(
				ipc,
				controllerutils.ConditionReasons.InvalidTransition,
				fmt.Sprintf("invalid IPConfig stage: %s", ipc.Spec.Stage),
			)
			if uerr := h.Client.Status().Update(ctx, ipc); uerr != nil {
				return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
			}
			return requeueWithError(fmt.Errorf("invalid IPConfig stage: %w", err))
		}
	}

	logger.Info("Running health checks")
	if err := CheckHealth(ctx, h.NoncachedClient, logger); err != nil {
		msg := fmt.Sprintf("Waiting for system to stabilize: %s", err.Error())
		controllerutils.SetIPIdleStatusFalse(ipc, controllerutils.ConditionReasons.Failed, msg)
		if uerr := h.Client.Status().Update(ctx, ipc); uerr != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
		}
		return requeueWithHealthCheckInterval(), fmt.Errorf("waiting for system to stabilize: %s", err.Error())
	}

	if err := h.cleanup(logger); err != nil {
		controllerutils.SetIPIdleStatusFalse(
			ipc,
			controllerutils.ConditionReasons.Failed,
			fmt.Sprintf("failed to cleanup: %v. Perform cleanup manually then add '%s' annotation to IPConfig CR to transition back to Idle",
				err,
				controllerutils.ManualCleanupAnnotation,
			),
		)
		if uerr := h.Client.Status().Update(ctx, ipc); uerr != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
		}
		return requeueWithError(fmt.Errorf("failed to cleanup: %w", err))
	}

	controllerutils.ResetStatusConditions(&ipc.Status.Conditions, ipc.Generation)
	if err := h.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Info("handleIdle completed successfully")

	return requeueWithCustomInterval(getRecertCacheInterval(ipc)), nil
}

func (h *IPConfigIdleStageHandler) cleanup(logger logr.Logger) error {
	if err := h.ChrootOps.RemountSysroot(); err != nil {
		return fmt.Errorf("failed to remount sysroot: %w", err)
	}

	if err := h.cleanuoUnbootedStateroots(logger); err != nil {
		return fmt.Errorf("failed to clean up unbooted stateroots: %w", err)
	}

	if err := cleanupIPConfigFiles(h.ChrootOps); err != nil {
		return fmt.Errorf("failed to cleanup workspace: %w", err)
	}

	return nil
}

func cleanupIPConfigFiles(chrootOps ops.Ops) error {
	if _, err := chrootOps.StatFile(
		common.PathOutsideChroot(controllerutils.IPConfigWorkspacePath),
	); err != nil {
		return nil
	}
	if err := chrootOps.RemoveAllFiles(
		common.PathOutsideChroot(controllerutils.IPConfigWorkspacePath),
	); err != nil {
		return fmt.Errorf("removing %s failed: %w", controllerutils.IPConfigWorkspacePath, err)
	}
	return nil
}

// checkIPManualCleanup looks for ManualCleanupAnnotation on the IPConfig CR. If present, it removes
// the annotation and returns true so the reconcile loop can retry idle tasks.
func (h *IPConfigIdleStageHandler) checkIPManualCleanup(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
) (bool, error) {
	if _, ok := ipc.Annotations[controllerutils.ManualCleanupAnnotation]; ok {
		delete(ipc.Annotations, controllerutils.ManualCleanupAnnotation)
		if err := h.Client.Update(ctx, ipc); err != nil {
			return false, fmt.Errorf("failed to remove manual cleanup annotation from IPConfig: %w", err)
		}
		return true, nil
	}
	return false, nil
}

func (h *IPConfigIdleStageHandler) cleanuoUnbootedStateroots(logger logr.Logger) error {
	staterootsToRemove, err := getStaterootsToRemove(h.RPMOstreeClient)
	if err != nil {
		return fmt.Errorf("failed to determine stateroots to remove: %w", err)
	}
	logger.Info("Stateroots to remove", "stateroots", staterootsToRemove)

	if err := h.ChrootOps.RemountBoot(); err != nil {
		return fmt.Errorf("failed to remount boot: %w", err)
	}

	if err := removeBootDirsByStaterootPrefixes(logger, h.ChrootOps, staterootsToRemove); err != nil {
		return err
	}

	if err := CleanupUnbootedStateroots(logger, h.ChrootOps, h.OstreeClient, h.RPMOstreeClient); err != nil {
		return fmt.Errorf("failed to clean up unbooted stateroots: %w", err)
	}

	return nil
}

// removeBootDirsByStaterootPrefixes removes directories under /boot/ostree that
// start with any of the given stateroot names followed by a hyphen.
func removeBootDirsByStaterootPrefixes(
	logger logr.Logger,
	chrootOps ops.Ops,
	staterootsToRemove []string,
) error {
	bootOstreePath := common.PathOutsideChroot("/boot/ostree")
	entries, err := chrootOps.ReadDir(bootOstreePath)
	if err != nil {
		if chrootOps.IsNotExist(err) {
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
			if err := chrootOps.RemoveAllFiles(dirPath); err != nil {
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

// shouldRefreshRecertImage determines if enough time has passed to run the recert image cache refresh again
func (h *IPConfigIdleStageHandler) shouldRefreshRecertImage(ipc *ipcv1.IPConfig, interval time.Duration) bool {
	if ipc.Annotations == nil {
		return true
	}
	last := ipc.Annotations[controllerutils.IPConfigRecertCacheLastRefreshAnnotation]
	if last == "" {
		return true
	}
	ts, err := time.Parse(time.RFC3339, last)
	if err != nil {
		return true
	}
	return time.Since(ts) >= interval
}

// getRecertImageFromIPC resolves the recert image to use in priority: spec, env, default
func getRecertImage(ipc *ipcv1.IPConfig) string {
	if ipc.Spec.Recert != nil && ipc.Spec.Recert.Image != "" {
		return ipc.Spec.Recert.Image
	}
	if v := os.Getenv(common.RecertImageEnvKey); v != "" {
		return v
	}
	return common.DefaultRecertImage
}

func getRecertCacheInterval(ipc *ipcv1.IPConfig) time.Duration {
	if ipc.Spec.Recert != nil && ipc.Spec.Recert.CacheInterval.Duration > 0 {
		return ipc.Spec.Recert.CacheInterval.Duration
	}
	return 1 * time.Hour
}

// refreshRecertImage pulls the recert image
func (h *IPConfigIdleStageHandler) refreshRecertImage(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) error {
	image := getRecertImage(ipc)
	if image == "" {
		return nil
	}

	authFile := common.ImageRegistryAuthFile
	if ipc.Spec.Recert != nil && ipc.Spec.Recert.PullSecretRef != nil {
		pullSecret, err := lcautils.GetSecretData(
			ctx, ipc.Spec.Recert.PullSecretRef.Name,
			common.LcaNamespace,
			corev1.DockerConfigJsonKey,
			h.Client,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to get pull-secret with the name %s in namespace %s holding the key %s: %w",
				ipc.Spec.Recert.PullSecretRef.Name,
				common.LcaNamespace,
				corev1.DockerConfigJsonKey,
				err,
			)
		}

		tempAuthFile, err := os.CreateTemp(os.TempDir(), "recert-pull-secret.json")
		if err != nil {
			return fmt.Errorf("failed to create temporary pull secret file: %w", err)
		}
		defer tempAuthFile.Close()

		if _, err := tempAuthFile.Write([]byte(pullSecret)); err != nil {
			return fmt.Errorf("failed to write pull secret to temporary file: %w", err)
		}

		authFile = tempAuthFile.Name()
	}

	command := "podman"
	if ipc.Spec.Proxy != nil {
		noProxy := strings.Join(ipc.Spec.Proxy.NoProxy, ",")
		httpProxy := ipc.Spec.Proxy.HTTPProxy
		httpsProxy := ipc.Spec.Proxy.HTTPSProxy
		if httpProxy != "" || httpsProxy != "" || noProxy != "" {
			command = fmt.Sprintf("HTTP_PROXY=%s HTTPS_PROXY=%s NO_PROXY=%s %s", httpProxy, httpsProxy, noProxy, command)
		}
	}

	if _, err := h.ChrootOps.RunBashInHostNamespace(command, "pull", "--authfile", authFile, image); err != nil {
		return fmt.Errorf("failed to pull recert image %s: %w", image, err)
	}

	logger.Info("recert image cached on host", "image", image)

	return nil
}

func (h *IPConfigIdleStageHandler) handleManualCleanupIfFailed(
	ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger,
) error {
	idleCond := meta.FindStatusCondition(ipc.Status.Conditions, string(controllerutils.ConditionTypes.Idle))
	if idleCond != nil && idleCond.Status == metav1.ConditionFalse &&
		idleCond.Reason == string(controllerutils.ConditionReasons.Failed) {
		done, err := h.checkIPManualCleanup(ctx, ipc)
		if err != nil {
			return fmt.Errorf("failed to check manual cleanup: %w", err)
		}

		if done {
			logger.Info("Manual cleanup annotation is found, removed annotation and retrying idle tasks")
		}
	}

	return nil
}
