package controllers

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	ibuv1 "github.com/openshift-kni/lifecycle-agent/api/imagebasedupgrade/v1"
	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/internal/ostreeclient"
	"github.com/openshift-kni/lifecycle-agent/internal/reboot"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	rpmostreeclient "github.com/openshift-kni/lifecycle-agent/lca-cli/ostreeclient"
	"github.com/openshift-kni/lifecycle-agent/utils"
	"github.com/samber/lo"
)

//+kubebuilder:rbac:groups=lca.openshift.io,resources=ipconfigs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=lca.openshift.io,resources=ipconfigs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=lca.openshift.io,resources=ipconfigs/finalizers,verbs=update

// IPConfigReconciler reconciles an IPConfig object
type IPConfigReconciler struct {
	client.Client
	NoncachedClient client.Reader
	Scheme          *runtime.Scheme
	Executor        ops.Execute
	Ops             ops.Ops
	RebootClient    reboot.RebootIntf
	RPMOstreeClient rpmostreeclient.IClient
	OstreeClient    ostreeclient.IClient
	Clientset       *kubernetes.Clientset
	PrepHandler     IPConfigPrepHandlerInterface
	ConfigHandler   IPConfigConfigurationHandlerInterface
	RollbackHandler IPConfigRollbackHandlerInterface
	Mux             *sync.Mutex
}

func (r *IPConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.Mux != nil {
		r.Mux.Lock()
		defer r.Mux.Unlock()
	}

	logger := log.FromContext(ctx).WithName("IPConfig")
	logger.Info(
		"Start reconciling IPConfig",
		"name", req.NamespacedName.Name,
		"namespace", req.NamespacedName.Namespace,
	)

	if err := validateIPConfigName(req); err != nil {
		return requeueWithError(fmt.Errorf("invalid IPConfig name: %w", err))
	}

	ipc, err := r.getOrCreateIPConfig(ctx)
	if err != nil {
		return requeueWithError(fmt.Errorf("failed to get or create IPConfig: %w", err))
	}

	validNextStages, err := validNextStages(ipc, r.RPMOstreeClient)
	if err != nil {
		return requeueWithError(fmt.Errorf("failed to get valid next stages: %w", err))
	}
	ipc.Status.ValidNextStages = validNextStages

	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update IPConfig status: %w", err))
	}

	if isIPTransitionRequested(ipc) {
		if err := r.validateIPConfigStage(ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to validate IPConfig stage: %w", err))
		}
	}

	if err := r.validateIBUIdle(ctx); err != nil {
		return requeueWithError(fmt.Errorf("failed to validate IBU idle: %w", err))
	}

	ipc.Status.ObservedGeneration = ipc.Generation

	if err := refreshCurrentIPs(ctx, ipc, r.NoncachedClient); err != nil {
		return requeueWithError(fmt.Errorf("failed to refresh current IPs: %w", err))
	}

	switch ipc.Spec.Stage {
	case ipcv1.IPStages.Idle:
		return r.handleIdle(ctx, ipc)
	case ipcv1.IPStages.Prep:
		return r.handlePrep(ctx, ipc)
	case ipcv1.IPStages.Config:
		return r.handleConfig(ctx, ipc)
	case ipcv1.IPStages.Rollback:
		return r.handleRollback(ctx, ipc)
	default:
		return requeueWithError(fmt.Errorf("invalid IPConfig stage: %s", ipc.Spec.Stage))
	}
}

func refreshCurrentIPs(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	nonCachedK8sClient client.Reader,
) error {
	ips, err := utils.GetNodeInternalIPs(ctx, nonCachedK8sClient)
	if err != nil {
		return fmt.Errorf("failed to get node internal ips: %w", err)
	}

	clusterIPStatus := &ipcv1.ClusterIPsStatus{}
	for _, addr := range ips {
		fam := "IPv4"
		if strings.Contains(addr, ":") {
			fam = "IPv6"
		}
		clusterIPStatus.NodeInternalIPs = append(
			clusterIPStatus.NodeInternalIPs,
			ipcv1.FamilyIP{Family: fam, Address: addr},
		)
	}
	ipc.Status.ClusterIPs = clusterIPStatus

	return nil
}

func validNextStages(ipc *ipcv1.IPConfig, rpmOstreeClient rpmostreeclient.IClient) ([]ipcv1.IPConfigStage, error) {
	inProgressStage := controllerutils.GetIPInProgressStage(ipc)

	if inProgressStage == ipcv1.IPStages.Idle || inProgressStage == ipcv1.IPStages.Rollback || controllerutils.IsIPStageFailed(ipc, ipcv1.IPStages.Rollback) {
		// no valid transition if aborting/abort failed/finalizing/finalize failed/rollback in progress/rollback failed
		return []ipcv1.IPConfigStage{}, nil
	}

	if inProgressStage == ipcv1.IPStages.Prep || controllerutils.IsIPStageFailed(ipc, ipcv1.IPStages.Prep) {
		isInNewStateroot := isTargetStaterootBooted(ipc, rpmOstreeClient)
		if isInNewStateroot {
			return []ipcv1.IPConfigStage{ipcv1.IPStages.Rollback}, nil
		} else {
			return []ipcv1.IPConfigStage{ipcv1.IPStages.Idle}, nil
		}
	}

	if inProgressStage == ipcv1.IPStages.Config || controllerutils.IsIPStageFailed(ipc, ipcv1.IPStages.Config) {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Rollback}, nil
	}

	// no in progress stage, check completed stages in reverse order
	if controllerutils.IsIPStageCompleted(ipc, ipcv1.IPStages.Rollback) {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Idle}, nil
	}
	if controllerutils.IsIPStageCompleted(ipc, ipcv1.IPStages.Config) {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Idle, ipcv1.IPStages.Rollback}, nil
	}
	if controllerutils.IsIPStageCompleted(ipc, ipcv1.IPStages.Prep) {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Config, ipcv1.IPStages.Rollback}, nil
	}
	if controllerutils.IsIPStageCompleted(ipc, ipcv1.IPStages.Idle) {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Prep}, nil
	}

	// initial IPConfig creation - no idle condition
	idleCondition := meta.FindStatusCondition(ipc.Status.Conditions, string(controllerutils.ConditionTypes.Idle))
	if idleCondition == nil {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Idle}, nil
	}

	return []ipcv1.IPConfigStage{}, nil
}

// isTargetStaterootBooted determines whether the stateroot prepared for this IP change is currently booted.
// It reconstructs the expected stateroot name from the spec (matching the lca-cli prepare logic) and queries rpm-ostree.
func isTargetStaterootBooted(ipc *ipcv1.IPConfig, rpmOstreeClient rpmostreeclient.IClient) bool {
	if rpmOstreeClient == nil {
		return false
	}
	target := buildIPConfigStaterootName(ipc)
	if target == "" {
		return false
	}
	booted, err := rpmOstreeClient.IsStaterootBooted(target)
	if err != nil {
		return false
	}
	return booted
}

// buildIPConfigStaterootName mirrors the lca-cli ip-config prepare naming scheme: rhcos_<ipv4>_<ipv6>
// where IPs are sanitized to alphanumeric and dashes, and IPv6 brackets are stripped.
func buildIPConfigStaterootName(ipc *ipcv1.IPConfig) string {
	parts := []string{"rhcos"}
	if v := ipc.Spec.IPv4; v != nil && v.Address != "" {
		parts = append(parts, common.SanitizeForOsname(strings.Split(v.Address, "/")[0]))
	}
	if v := ipc.Spec.IPv6; v != nil && v.Address != "" {
		addr := strings.Split(v.Address, "/")[0]
		addr = strings.Trim(addr, "[]")
		parts = append(parts, common.SanitizeForOsname(addr))
	}
	if len(parts) == 1 {
		return ""
	}
	return strings.Join(parts, "_")
}

// SetupWithManager sets up the controller with the Manager.
func (r *IPConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	//nolint:wrapcheck
	return ctrl.NewControllerManagedBy(mgr).
		For(&ipcv1.IPConfig{}, builder.WithPredicates(predicate.Funcs{
			UpdateFunc: func(e event.UpdateEvent) bool {
				return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration()
			},
			CreateFunc:  func(ce event.CreateEvent) bool { return true },
			GenericFunc: func(ge event.GenericEvent) bool { return false },
			DeleteFunc:  func(de event.DeleteEvent) bool { return false },
		})).
		Complete(r)
}

func validateIPConfigName(req ctrl.Request) error {
	if req.Name != common.IPConfigName {
		return fmt.Errorf("ipconfig CR must be named %s", common.IPConfigName)
	}
	return nil
}

func (r *IPConfigReconciler) getOrCreateIPConfig(ctx context.Context) (*ipcv1.IPConfig, error) {
	ipc := &ipcv1.IPConfig{}
	if err := r.NoncachedClient.Get(ctx, client.ObjectKey{Name: common.IPConfigName}, ipc); err != nil {
		if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get IPConfig: %w", err)
		}

		ipc = &ipcv1.IPConfig{
			ObjectMeta: metav1.ObjectMeta{Name: common.IPConfigName},
			Spec:       ipcv1.IPConfigSpec{Stage: ipcv1.IPStages.Idle},
		}

		if createErr := r.Client.Create(ctx, ipc); createErr != nil {
			return nil, fmt.Errorf("failed to create IPConfig: %w", createErr)
		}

		if getErr := r.NoncachedClient.Get(ctx, client.ObjectKey{Name: common.IPConfigName}, ipc); getErr != nil {
			return nil, fmt.Errorf("failed to get IPConfig after creation: %w", getErr)
		}
	}

	return ipc, nil
}

func (r *IPConfigReconciler) getIBUStage(ctx context.Context) (*ibuv1.ImageBasedUpgradeStage, error) {
	ibu := &ibuv1.ImageBasedUpgrade{}
	if err := r.NoncachedClient.Get(ctx, client.ObjectKey{Name: "upgrade"}, ibu); err != nil {
		return nil, fmt.Errorf("failed to get IBU: %w", err)
	}

	return &ibu.Spec.Stage, nil
}

func (r *IPConfigReconciler) validateIBUIdle(ctx context.Context) error {
	ibuStage, err := r.getIBUStage(ctx)
	if err != nil {
		return fmt.Errorf("failed to get IBU stage: %w", err)
	}

	if lo.FromPtr(ibuStage) != ibuv1.Stages.Idle {
		return fmt.Errorf("IBU is not in Idle stage, current stage is %s", lo.FromPtr(ibuStage))
	}

	return nil
}

func (r *IPConfigReconciler) validateIPConfigStage(ipc *ipcv1.IPConfig) error {
	if !lo.Contains(ipc.Status.ValidNextStages, ipc.Spec.Stage) {
		return fmt.Errorf("invalid IPConfig stage: %s", ipc.Spec.Stage)
	}

	return nil
}

func isIPTransitionRequested(ipc *ipcv1.IPConfig) bool {
	desiredStage := ipc.Spec.Stage
	if desiredStage == ipcv1.IPStages.Idle {
		return !(controllerutils.IsIPStageCompleted(ipc, desiredStage) ||
			controllerutils.IsIPStageInProgress(ipc, desiredStage))
	}
	return !(controllerutils.IsIPStageCompletedOrFailed(ipc, desiredStage) ||
		controllerutils.IsIPStageInProgress(ipc, desiredStage))
}
