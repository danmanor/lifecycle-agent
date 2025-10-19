package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/go-logr/logr"
	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/internal/ostreeclient"
	"github.com/openshift-kni/lifecycle-agent/internal/reboot"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type IPConfigConfigureHandlerInterface interface {
	PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error)
	PostPivot(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error)
}

type IPConfigConfigureHandler struct {
	Client          client.Client
	NoncachedClient client.Reader
	Executor        ops.Execute
	Ops             ops.Ops
	RebootClient    reboot.RebootIntf
	OstreeClient    ostreeclient.IClient
}

func NewIPConfigConfigureHandler(
	client client.Client,
	noncachedClient client.Reader,
	executor ops.Execute,
	ops ops.Ops,
	rebootClient reboot.RebootIntf,
	ostreeClient ostreeclient.IClient,
) IPConfigConfigureHandlerInterface {
	return &IPConfigConfigureHandler{
		Client:          client,
		NoncachedClient: noncachedClient,
		Executor:        executor,
		Ops:             ops,
		RebootClient:    rebootClient,
		OstreeClient:    ostreeClient,
	}
}

func (c *IPConfigConfigureHandler) PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	if err := c.writeIPConfigRunConfig(ipc); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to write ip-config run config: %s", err.Error()),
			ipc.Generation,
		)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to write ip-config run config: %s", err.Error()),
			ipc.Generation,
		)

		if err := c.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return doNotRequeue(), fmt.Errorf("failed to write ip-config run config: %w", err)
	}

	if err := controllerutils.CopyLcaCliToHost(logger); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to copy lca-cli binary: %s", err.Error()),
			ipc.Generation,
		)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to copy lca-cli binary: %s", err.Error()),
			ipc.Generation,
		)

		if err := c.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return doNotRequeue(), fmt.Errorf("failed to copy lca-cli binary: %w", err)
	}

	log.FromContext(ctx).Info("Scheduling lca-cli ip-config run via systemd-run")
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Configure),
		controllerutils.ConditionReasons.InProgress,
		metav1.ConditionTrue,
		"IP configuration is in progress",
		ipc.Generation,
	)
	if err := c.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	args := []string{
		"--property", "ExitType=cgroup",
		"--unit", "lca-ipconfig-run",
		"--description", "lifecycle-agent: ip-config run",
		"lca-cli", "ip-config", "run",
	}
	if _, err := c.Executor.Execute("systemd-run", args...); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to run ip-config: %s", err.Error()),
			ipc.Generation,
		)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to run ip-config: %s", err.Error()),
			ipc.Generation,
		)

		if err := c.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return doNotRequeue(), fmt.Errorf("failed to schedule ip-config run: %w", err)
	}

	// should not reach here on successful ip-config run
	return doNotRequeue(), nil
}

// PreConfigure and PostConfigure were merged into PrePivot

func (c *IPConfigConfigureHandler) PostPivot(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	log.FromContext(ctx).Info("Starting health check for different components")
	if err := CheckHealth(ctx, c.NoncachedClient, log.FromContext(ctx)); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.InProgress,
			metav1.ConditionTrue,
			fmt.Sprintf("Waiting for system to stabilize: %s", err.Error()),
			ipc.Generation,
		)

		if err := c.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithHealthCheckInterval(), nil
	}

	controllerutils.SetStatusCondition(
		&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Configure),
		controllerutils.ConditionReasons.InProgress,
		metav1.ConditionTrue,
		"Cluster has stabilized",
		ipc.Generation,
	)

	if err := c.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	if err := refreshCurrentIPs(ctx, ipc, c.NoncachedClient); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to refresh current IPs: %s", err.Error()),
			ipc.Generation,
		)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to refresh current IPs: %s", err.Error()),
			ipc.Generation,
		)

		if err := c.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(fmt.Errorf("failed to refresh current IPs: %w", err))
	}

	if err := statusIPsMatchSpec(ipc); err != nil {
		controllerutils.SetStatusCondition(
			&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.InProgress,
			metav1.ConditionTrue,
			fmt.Sprintf("Waiting for current IPs to match spec: %s", err.Error()),
			ipc.Generation,
		)

		if err := c.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithHealthCheckInterval(), nil
	}

	controllerutils.SetStatusCondition(
		&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Configure),
		controllerutils.ConditionReasons.Completed,
		metav1.ConditionTrue,
		"Configuration completed",
		ipc.Generation,
	)

	if err := c.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	return doNotRequeue(), nil
}

// statusIPsMatchSpec checks whether the IPs requested in spec are present in status.ClusterIPs.
// It returns true when all requested families (IPv4/IPv6) are observed with exact addresses.
func statusIPsMatchSpec(ipc *ipcv1.IPConfig) error {
	desiredV4 := ""
	desiredV6 := ""

	if v := ipc.Spec.IPv4; v != nil && v.Address != "" {
		desiredV4 = strings.Split(v.Address, "/")[0]
	}

	if v := ipc.Spec.IPv6; v != nil && v.Address != "" {
		addr := strings.Split(v.Address, "/")[0]
		desiredV6 = strings.Trim(addr, "[]")
	}

	// Nothing requested, trivially matches
	if desiredV4 == "" && desiredV6 == "" {
		return fmt.Errorf("nothing requested, shouldn't happen")
	}

	if ipc.Status.ClusterIPs == nil {
		return fmt.Errorf("clusterIPs not yet populated")
	}

	foundV4 := desiredV4 == ""
	foundV6 := desiredV6 == ""

	for _, famIP := range ipc.Status.ClusterIPs.NodeInternalIPs {
		if desiredV4 != "" && famIP.Family == "IPv4" && famIP.Address == desiredV4 {
			foundV4 = true
		}
		if desiredV6 != "" && famIP.Family == "IPv6" && famIP.Address == desiredV6 {
			foundV6 = true
		}
	}

	if foundV4 && foundV6 {
		return nil
	}

	missing := []string{}
	if !foundV4 {
		missing = append(missing, fmt.Sprintf("IPv4 %s", desiredV4))
	}
	if !foundV6 {
		missing = append(missing, fmt.Sprintf("IPv6 %s", desiredV6))
	}

	return fmt.Errorf("desired IPs not observed in status: %s", strings.Join(missing, ", "))
}

// per-phase handlers for IPConfig run status
func (r *IPConfigReconciler) handleConfigureUnknown(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	logger.Info("Running IP config PrePivot handler")
	return r.ConfigureHandler.PrePivot(ctx, ipc, logger)
}

func (r *IPConfigReconciler) handleConfigureRunning(logger logr.Logger) (ctrl.Result, error) {
	logger.Info("ip-config run in progress; requeueing")
	return requeueWithShortInterval(), nil
}

func (r *IPConfigReconciler) handleConfigureFailed(ctx context.Context, ipc *ipcv1.IPConfig, message string) (ctrl.Result, error) {
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Configure),
		controllerutils.ConditionReasons.Failed,
		metav1.ConditionFalse,
		fmt.Sprintf("ip-config run failed: %s", message),
		ipc.Generation,
	)
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Configure),
		controllerutils.ConditionReasons.Failed,
		metav1.ConditionFalse,
		fmt.Sprintf("ip-config run failed: %s", message),
		ipc.Generation,
	)
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}
	return doNotRequeue(), fmt.Errorf("ip-config run failed: %s", message)
}

func (r *IPConfigReconciler) handleConfigureSucceeded(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	logger.Info("Running IP config PostPivot handler")
	result, err := r.ConfigureHandler.PostPivot(ctx, ipc)
	if err != nil {
		return result, fmt.Errorf("failed to run PostPivot: %w", err)
	}

	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Configure),
		controllerutils.ConditionReasons.Completed,
		metav1.ConditionFalse,
		"Configuration completed",
		ipc.Generation,
	)
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Configure),
		controllerutils.ConditionReasons.Completed,
		metav1.ConditionTrue,
		"Configuration completed",
		ipc.Generation,
	)

	validNextStages, err := validNextStages(ipc, r.RPMOstreeClient)
	if err != nil {
		return requeueWithError(fmt.Errorf("failed to get valid next stages: %w", err))
	}
	ipc.Status.ValidNextStages = validNextStages

	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Info("PostPivot completed successfully")
	return result, nil
}

func (r *IPConfigReconciler) handleConfigure(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("IPConfigConfigure")
	logger.Info("Starting handleConfigure")

	phase, message, err := common.ReadIPConfigStatus(common.PathOutsideChroot(common.IPConfigRunStatusFile))
	if err != nil {
		return requeueWithError(fmt.Errorf("failed to read ip-config run status: %w", err))
	}

	switch phase {
	case common.IPConfigRunPhaseUnknown:
		return r.handleConfigureUnknown(ctx, ipc, logger)
	case common.IPConfigRunPhaseRunning:
		return r.handleConfigureRunning(logger)
	case common.IPConfigRunPhaseFailed:
		return r.handleConfigureFailed(ctx, ipc, message)
	case common.IPConfigRunPhaseSucceeded:
		return r.handleConfigureSucceeded(ctx, ipc, logger)
	default:
		return requeueWithShortInterval(), nil
	}
}

// writeIPConfigRunConfigToNewStateroot writes the ip-config run configuration file into the new stateroot etc
func (c *IPConfigConfigureHandler) writeIPConfigRunConfig(ipc *ipcv1.IPConfig) error {
	cfg := common.IPConfigRunConfig{}

	if v := ipc.Spec.IPv4; v != nil {
		if v.Address != "" {
			cfg.IPv4Address = strings.Split(v.Address, "/")[0]
		}
		if v.MachineNetwork != "" {
			cfg.IPv4MachineNetwork = v.MachineNetwork
		}
	}
	if v := ipc.Spec.IPv6; v != nil {
		if v.Address != "" {
			cfg.IPv6Address = strings.Trim(strings.Split(v.Address, "/")[0], "[]")
		}
		if v.MachineNetwork != "" {
			cfg.IPv6MachineNetwork = v.MachineNetwork
		}
	}
	if p := ipc.Spec.Proxy; p != nil {
		if p.HTTPProxy != "" {
			cfg.HTTPProxy = p.HTTPProxy
		}
		if p.HTTPSProxy != "" {
			cfg.HTTPSProxy = p.HTTPSProxy
		}
		if len(p.NoProxy) > 0 {
			cfg.NoProxy = strings.Join(p.NoProxy, ",")
		}
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal ip-config run config: %w", err)
	}
	if err := os.WriteFile(common.PathOutsideChroot(common.IPConfigRunFlagsFile), data, 0o600); err != nil {
		return fmt.Errorf("failed to write ip-config run config: %w", err)
	}

	return nil
}
