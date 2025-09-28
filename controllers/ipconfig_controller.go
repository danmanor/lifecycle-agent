package controllers

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/go-logr/logr"
	ibuv1 "github.com/openshift-kni/lifecycle-agent/api/imagebasedupgrade/v1"
	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	"github.com/openshift-kni/lifecycle-agent/internal/healthcheck"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	"github.com/openshift-kni/lifecycle-agent/utils"
	"github.com/samber/lo"
)

// IPConfigReconciler reconciles an IPConfig object
type IPConfigReconciler struct {
	client.Client
	NoncachedClient client.Reader
	Scheme          *runtime.Scheme
	Executor        ops.Execute
	Ops             ops.Ops
	Mux             *sync.Mutex
}

const ipconfigName = "ipconfig"

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

	nextReconcile := doNotRequeue()
	defer logFinishReconciling(nextReconcile, logger, req)

	if err := validateIPConfigName(req); err != nil {
		return nextReconcile, fmt.Errorf("invalid IPConfig name: %w", err)
	}

	ipc, err := r.getOrCreateIPConfig(ctx)
	if err != nil {
		return nextReconcile, fmt.Errorf("failed to get or create IPConfig: %w", err)
	}

	ibuStage, err := r.getIBUStage(ctx)
	if err != nil {
		return nextReconcile, fmt.Errorf("failed to get IBU stage: %w", err)
	}
	if lo.FromPtr(ibuStage) != ibuv1.Stages.Idle {
		nextReconcile = requeueWithLongInterval()
		return nextReconcile, fmt.Errorf(
			"cannot run IP Config when IBU is not in Idle stage, current stage is %s",
			lo.FromPtr(ibuStage),
		)
	}

	ipc.Status.ObservedGeneration = ipc.Generation

	if err := r.refreshCurrentIPs(ctx, ipc); err != nil {
		return nextReconcile, fmt.Errorf("failed to refresh current IPs: %w", err)
	}

	ipc.Status.ValidNextStages = r.validNextStages(ipc)

	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		nextReconcile = requeueWithShortInterval()
		return nextReconcile, fmt.Errorf("failed to update IPConfig status: %w", err)
	}

	switch ipc.Spec.Stage {
	case ipcv1.IPStages.Idle:
		return r.handleIdle(ctx, ipc, nextReconcile)
	case ipcv1.IPStages.Prepare:
		return r.handlePrepare(ctx, ipc, nextReconcile)
	case ipcv1.IPStages.Configure:
		return r.handleConfigure(ctx, ipc, nextReconcile)
	case ipcv1.IPStages.Rollback:
		return r.handleRollback(ctx, ipc, nextReconcile)
	default:
		return nextReconcile, nil
	}
}

func (r *IPConfigReconciler) refreshCurrentIPs(ctx context.Context, ipc *ipcv1.IPConfig) error {
	nodeName, err := utils.GetLocalNodeName(ctx, r.Client)
	if err != nil {
		return fmt.Errorf("failed to get local node name: %w", err)
	}

	node := &corev1.Node{}
	if err := r.NoncachedClient.Get(ctx, client.ObjectKey{Name: nodeName}, node); err == nil {
		status := &ipcv1.ClusterIPsStatus{}
		for _, addr := range node.Status.Addresses {
			if addr.Type != corev1.NodeInternalIP {
				continue
			}
			fam := "IPv4"
			if strings.Contains(addr.Address, ":") {
				fam = "IPv6"
			}
			status.NodeInternalIPs = append(status.NodeInternalIPs, ipcv1.FamilyIP{Family: fam, Address: addr.Address})
		}
		ipc.Status.CurrentClusterIPs = status
	}

	return nil
}

func (r *IPConfigReconciler) validNextStages(ipc *ipcv1.IPConfig) []ipcv1.IPConfigStage {
	switch ipc.Spec.Stage {
	case ipcv1.IPStages.Idle:
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Prepare}
	case ipcv1.IPStages.Prepare:
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Configure}
	case ipcv1.IPStages.Configure:
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Idle, ipcv1.IPStages.Rollback}
	case ipcv1.IPStages.Rollback:
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Idle}
	default:
		return nil
	}
}

func (r *IPConfigReconciler) handlePrepare(ctx context.Context, ipc *ipcv1.IPConfig, nextReconcile ctrl.Result) (ctrl.Result, error) {
	// Build systemd-run to call lca-cli ip-config prepare with ipv4/ipv6 per spec
	args := []string{
		"--collect",
		"--wait",
		"--property", "ExitType=cgroup",
		"--unit", "lca-ipconfig-prepare",
		"--setenv", "HTTP_PROXY",
		"--setenv", "HTTPS_PROXY",
		"--setenv", "NO_PROXY",
		"lca-cli", "ip-config", "prepare",
	}
	if ipc.Spec.IPv4 != nil && ipc.Spec.IPv4.Address != "" {
		args = append(args, "--ipv4-address", strings.Split(ipc.Spec.IPv4.Address, "/")[0])
	}
	if ipc.Spec.IPv6 != nil && ipc.Spec.IPv6.Address != "" {
		args = append(args, "--ipv6-address", strings.Split(ipc.Spec.IPv6.Address, "/")[0])
	}

	if _, err := r.Executor.Execute("systemd-run", args...); err != nil {
		setCondition(
			&ipc.Status.Conditions,
			ipcv1.ConditionPrepared,
			metav1.ConditionFalse,
			"PrepareFailed",
			err.Error(),
			ipc.Generation,
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return nextReconcile, fmt.Errorf("failed to update IPConfig status: %w", err)
		}
		return nextReconcile, nil
	}

	setCondition(
		&ipc.Status.Conditions,
		ipcv1.ConditionPrepared,
		metav1.ConditionTrue,
		"PrepareCompleted",
		"Alternate stateroot prepared",
		ipc.Generation,
	)

	ipc.Status.ValidNextStages = r.validNextStages(ipc)
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return nextReconcile, fmt.Errorf("failed to update IPConfig status: %w", err)
	}

	return nextReconcile, nil
}

func (r *IPConfigReconciler) handleConfigure(ctx context.Context, ipc *ipcv1.IPConfig, nextReconcile ctrl.Result) (ctrl.Result, error) {
	args := []string{"lca-cli", "ip-config", "run"}
	if v := ipc.Spec.IPv4; v != nil {
		if v.Address != "" && v.MachineNetwork != "" {
			addr := strings.Split(v.Address, "/")[0]
			args = append(args, "--ipv4-address", addr, "--ipv4-machine-network", v.MachineNetwork)
		}
	}
	if v := ipc.Spec.IPv6; v != nil {
		if v.Address != "" && v.MachineNetwork != "" {
			addr := strings.Split(v.Address, "/")[0]
			args = append(args, "--ipv6-address", addr, "--ipv6-machine-network", v.MachineNetwork)
		}
	}
	if p := ipc.Spec.Proxy; p != nil {
		if p.HTTPProxy != "" {
			args = append(args, "--http-proxy", p.HTTPProxy)
		}
		if p.HTTPSProxy != "" {
			args = append(args, "--https-proxy", p.HTTPSProxy)
		}
		if len(p.NoProxy) > 0 {
			args = append(args, "--no-proxy", strings.Join(p.NoProxy, ","))
		}
	}
	if ipc.Spec.PullSecretRef != "" {
		// The lca-cli expects a file path; here we rely on the same default used by CLI when a path is provided by spec.
		// Controller may mount/prepare this path elsewhere; for now we omit if not managed.
	}

	if _, err := r.Executor.Execute(args[0], args[1:]...); err != nil {
		setCondition(&ipc.Status.Conditions, ipcv1.ConditionConfigured, metav1.ConditionFalse, "ConfigureFailed", err.Error(), ipc.Generation)
		_ = r.Client.Status().Update(ctx, ipc)
		return nextReconcile, nil
	}

	setCondition(&ipc.Status.Conditions, ipcv1.ConditionConfigured, metav1.ConditionTrue, "Completed", "Post-reboot checks pending", ipc.Generation)
	ipc.Status.ValidNextStages = r.validNextStages(ipc)
	_ = r.Client.Status().Update(ctx, ipc)
	return nextReconcile, nil
}

func (r *IPConfigReconciler) handleRollback(ctx context.Context, ipc *ipcv1.IPConfig, nextReconcile ctrl.Result) (ctrl.Result, error) {
	// For rollback we need a target stateroot. If unspecified, try unbooted stateroot through rpm-ostree via ops is not available here; defer to lca-cli which can compute it.
	args := []string{"lca-cli", "ip-config", "rollback"}
	// If user provided stateroot via annotation (optional future), we would pass it here.
	if _, err := r.Executor.Execute(args[0], args[1:]...); err != nil {
		setCondition(&ipc.Status.Conditions, ipcv1.ConditionDegraded, metav1.ConditionTrue, "RollbackFailed", err.Error(), ipc.Generation)
		_ = r.Client.Status().Update(ctx, ipc)
		return nextReconcile, nil
	}

	setCondition(&ipc.Status.Conditions, ipcv1.ConditionDegraded, metav1.ConditionFalse, "", "", ipc.Generation)
	ipc.Status.ValidNextStages = r.validNextStages(ipc)
	_ = r.Client.Status().Update(ctx, ipc)
	return nextReconcile, nil
}

func (r *IPConfigReconciler) handleIdle(ctx context.Context, ipc *ipcv1.IPConfig, nextReconcile ctrl.Result) (ctrl.Result, error) {
	configured := meta.FindStatusCondition(ipc.Status.Conditions, ipcv1.ConditionConfigured)
	prepared := meta.FindStatusCondition(ipc.Status.Conditions, ipcv1.ConditionPrepared)
	degraded := meta.FindStatusCondition(ipc.Status.Conditions, ipcv1.ConditionDegraded)

	if configured != nil && configured.Status == metav1.ConditionTrue {
		if err := healthcheck.HealthChecks(ctx, r.NoncachedClient, log.FromContext(ctx)); err != nil {
			setCondition(&ipc.Status.Conditions, ipcv1.ConditionConfigured, metav1.ConditionFalse, "Finalizing", "Waiting for system to stabilize: "+err.Error(), ipc.Generation)
			_ = r.Client.Status().Update(ctx, ipc)
			return nextReconcile, nil
		}

		setCondition(&ipc.Status.Conditions, ipcv1.ConditionConfigured, metav1.ConditionTrue, "Completed", "System stable", ipc.Generation)
		ipc.Status.ValidNextStages = r.validNextStages(ipc)
		_ = r.Client.Status().Update(ctx, ipc)
		return nextReconcile, nil
	}

	if (prepared != nil && prepared.Status == metav1.ConditionTrue) || (degraded != nil && degraded.Status == metav1.ConditionTrue) {
		args := []string{"lca-cli", "ip-config", "rollback"}
		if _, err := r.Executor.Execute(args[0], args[1:]...); err != nil {
			setCondition(&ipc.Status.Conditions, ipcv1.ConditionDegraded, metav1.ConditionTrue, "RollbackFailed", err.Error(), ipc.Generation)
			_ = r.Client.Status().Update(ctx, ipc)
			return nextReconcile, nil
		}

		setCondition(&ipc.Status.Conditions, ipcv1.ConditionDegraded, metav1.ConditionFalse, "", "", ipc.Generation)
		setCondition(&ipc.Status.Conditions, ipcv1.ConditionPrepared, metav1.ConditionFalse, "Aborted", "Aborted and cleaned up prepared state", ipc.Generation)
		ipc.Status.ValidNextStages = r.validNextStages(ipc)
		_ = r.Client.Status().Update(ctx, ipc)
		return nextReconcile, nil
	}

	return nextReconcile, nil
}

func setCondition(conds *[]metav1.Condition, condType string, status metav1.ConditionStatus, reason, message string, gen int64) {
	// remove existing of same type
	var filtered []metav1.Condition
	for _, c := range *conds {
		if c.Type == condType {
			continue
		}
		filtered = append(filtered, c)
	}
	*conds = filtered
	*conds = append(*conds, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: gen,
		LastTransitionTime: metav1.NewTime(time.Now()),
	})
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

func logFinishReconciling(nextReconcile ctrl.Result, logger logr.Logger, req ctrl.Request) {
	if nextReconcile.RequeueAfter > 0 {
		logger.Info(
			"Finish reconciling IPConfig",
			"name", req.NamespacedName.Name,
			"namespace", req.NamespacedName.Namespace,
			"requeueAfter", nextReconcile.RequeueAfter.Seconds(),
		)
	} else {
		logger.Info(
			"Finish reconciling IPConfig",
			"name", req.NamespacedName.Name,
			"namespace", req.NamespacedName.Namespace,
			"requeueRightAway", nextReconcile.Requeue,
		)
	}
}

func validateIPConfigName(req ctrl.Request) error {
	if req.Name != ipconfigName {
		return fmt.Errorf("ipconfig CR must be named %s", ipconfigName)
	}
	return nil
}

func (r *IPConfigReconciler) getOrCreateIPConfig(ctx context.Context) (*ipcv1.IPConfig, error) {
	ipc := &ipcv1.IPConfig{}
	if err := r.NoncachedClient.Get(ctx, client.ObjectKey{Name: ipconfigName}, ipc); err != nil {
		if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get IPConfig: %w", err)
		}

		ipc = &ipcv1.IPConfig{
			ObjectMeta: metav1.ObjectMeta{Name: ipconfigName},
			Spec:       ipcv1.IPConfigSpec{Stage: ipcv1.IPStages.Idle},
		}

		if createErr := r.Client.Create(ctx, ipc); createErr != nil {
			return nil, fmt.Errorf("failed to create IPConfig: %w", createErr)
		}

		if getErr := r.NoncachedClient.Get(ctx, client.ObjectKey{Name: ipconfigName}, ipc); getErr != nil {
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
