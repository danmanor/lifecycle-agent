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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	IPConfigConfigPhasePrepivot  = "ConfigPrePivot"
	IPConfigConfigPhasePostpivot = "ConfigPostPivot"
)

type IPConfigConfigurationHandlerInterface interface {
	PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error)
	PostPivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error)
}

type IPConfigConfigurationHandler struct {
	Client          client.Client
	NoncachedClient client.Reader
	Executor        ops.Execute
	Ops             ops.Ops
	RebootClient    reboot.RebootIntf
	OstreeClient    ostreeclient.IClient
}

func NewIPConfigConfigurationHandler(
	client client.Client,
	noncachedClient client.Reader,
	executor ops.Execute,
	ops ops.Ops,
	rebootClient reboot.RebootIntf,
	ostreeClient ostreeclient.IClient,
) IPConfigConfigurationHandlerInterface {
	return &IPConfigConfigurationHandler{
		Client:          client,
		NoncachedClient: noncachedClient,
		Executor:        executor,
		Ops:             ops,
		RebootClient:    rebootClient,
		OstreeClient:    ostreeClient,
	}
}

func (c *IPConfigConfigurationHandler) PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	controllerutils.StartIPPhase(c.Client, logger, ipc, IPConfigConfigPhasePrepivot)

	if err := c.writeIPConfigRunConfig(ipc); err != nil {
		controllerutils.SetIPConfigStatusFailed(ipc, fmt.Sprintf("failed to write ip-config run config: %s", err.Error()))
		if err := c.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(fmt.Errorf("failed to write ip-config run config: %w", err))
	}

	if err := c.RunLcaCliIPConfigRun(logger); err != nil {
		controllerutils.SetIPConfigStatusFailed(ipc, fmt.Sprintf("failed to run ip-config: %s", err.Error()))
		if err := c.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		logger.Error(fmt.Errorf("failed to schedule ip-config run: %w", err), "failed to schedule ip-config run")
		return doNotRequeue(), nil
	}

	// should not reach here on successful ip-config run

	return doNotRequeue(), nil
}

// PreConfigure and PostConfigure were merged into PrePivot

func (c *IPConfigConfigurationHandler) PostPivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	controllerutils.StartIPPhase(c.Client, logger, ipc, IPConfigConfigPhasePostpivot)
	logger.Info("Starting health check for different components")
	if err := CheckHealth(ctx, c.NoncachedClient, logger); err != nil {
		controllerutils.SetIPConfigStatusInProgress(
			ipc,
			fmt.Sprintf("Waiting for system to stabilize: %s", err.Error()),
		)
		if err := c.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithHealthCheckInterval(), fmt.Errorf("waiting for system to stabilize: %s", err.Error())
	}

	controllerutils.SetIPConfigStatusInProgress(ipc, "Cluster has stabilized")
	if err := c.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	if err := refreshCurrentIPs(ctx, ipc, c.NoncachedClient); err != nil {
		controllerutils.SetIPConfigStatusFailed(ipc, fmt.Sprintf("failed to refresh current IPs: %s", err.Error()))
		if err := c.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(fmt.Errorf("failed to refresh current IPs: %w", err))
	}

	if err := statusIPsMatchSpec(ipc); err != nil {
		controllerutils.SetIPConfigStatusInProgress(ipc, fmt.Sprintf("Waiting for current IPs to match spec: %s", err.Error()))
		if err := c.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithHealthCheckInterval(), nil
	}

	if err := c.RebootClient.DisableInitMonitor(); err != nil {
		return requeueWithError(fmt.Errorf("failed to disable init monitor: %w", err))
	}

	controllerutils.StopIPPhase(c.Client, logger, ipc, IPConfigConfigPhasePostpivot)

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
func (r *IPConfigReconciler) handleConfigUnknown(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	controllerutils.SetIPConfigStatusInProgress(ipc, "IP configuration is in progress")
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Info("Running IP config PrePivot handler")
	result, err := r.ConfigHandler.PrePivot(ctx, ipc, logger)
	if err != nil {
		return result, fmt.Errorf("failed to run PrePivot: %w", err)
	}

	return result, nil
}

func (r *IPConfigReconciler) handleConfigRunning(logger logr.Logger) (ctrl.Result, error) {
	logger.Info("ip-config run in progress; requeueing")
	return requeueWithShortInterval(), nil
}

func (r *IPConfigReconciler) handleConfigFailed(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
	message string,
) (ctrl.Result, error) {
	controllerutils.SetIPConfigStatusFailed(ipc, fmt.Sprintf("ip-config run failed: %s", message))
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Error(fmt.Errorf("ip-config run failed: %s", message), "ip-config run failed")
	return doNotRequeue(), nil
}

func (r *IPConfigReconciler) handleConfigSucceeded(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	controllerutils.StopIPPhase(r.Client, logger, ipc, IPConfigConfigPhasePrepivot)

	logger.Info("Running IP config PostPivot handler")
	result, err := r.ConfigHandler.PostPivot(ctx, ipc, logger)
	if err != nil {
		return result, fmt.Errorf("post pivot failed: %w", err)
	}

	controllerutils.SetIPConfigStatusCompleted(ipc, "Configuration completed")
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}
	controllerutils.StopIPStageHistory(r.Client, logger, ipc)

	logger.Info("config completed successfully")
	return result, nil
}

func (r *IPConfigReconciler) handleConfig(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
) (res ctrl.Result, err error) {
	logger := log.FromContext(ctx).WithName("IPConfigConfig")
	logger.Info("Starting handleConfig")

	if isIPTransitionRequested(ipc) {
		if err := r.validateIPConfigStage(ipc); err != nil {
			controllerutils.SetIPConfigStatusFailed(
				ipc,
				"invalid transition: "+string(ipc.Spec.Stage),
			)
			if err := r.Client.Status().Update(ctx, ipc); err != nil {
				return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
			}
			return doNotRequeue(), nil
		}
	}

	phase, message, err := common.ReadIPConfigStatus(common.PathOutsideChroot(common.IPConfigRunStatusFile))
	if err != nil {
		return requeueWithError(fmt.Errorf("failed to read ip-config run status: %w", err))
	}

	switch phase {
	case common.IPConfigRunPhaseUnknown:
		return r.handleConfigUnknown(ctx, ipc, logger)
	case common.IPConfigRunPhaseRunning:
		return r.handleConfigRunning(logger)
	case common.IPConfigRunPhaseFailed:
		return r.handleConfigFailed(ctx, ipc, logger, message)
	case common.IPConfigRunPhaseSucceeded:
		return r.handleConfigSucceeded(ctx, ipc, logger)
	default:
		return requeueWithShortInterval(), nil
	}
}

// writeIPConfigRunConfigToNewStateroot writes the ip-config run configuration file into the new stateroot etc
func (c *IPConfigConfigurationHandler) writeIPConfigRunConfig(ipc *ipcv1.IPConfig) error {
	cfg := common.IPConfigRunConfig{}

	if v := ipc.Spec.IPv4; v != nil {
		if v.Address != "" {
			cfg.IPv4Address = strings.Split(v.Address, "/")[0]
		}
		if v.MachineNetwork != "" {
			cfg.IPv4MachineNetwork = v.MachineNetwork
		}
		if v.Gateway != "" {
			cfg.IPv4Gateway = v.Gateway
		}
		if v.DNSServer != "" {
			cfg.IPv4DNSServer = v.DNSServer
		}
	}
	if v := ipc.Spec.IPv6; v != nil {
		if v.Address != "" {
			cfg.IPv6Address = strings.Trim(strings.Split(v.Address, "/")[0], "[]")
		}
		if v.MachineNetwork != "" {
			cfg.IPv6MachineNetwork = v.MachineNetwork
		}
		if v.Gateway != "" {
			cfg.IPv6Gateway = v.Gateway
		}
		if v.DNSServer != "" {
			cfg.IPv6DNSServer = v.DNSServer
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

// RunLcaCliIPConfigRun schedules an lca-cli ip-config run via systemd-run.
func (c *IPConfigConfigurationHandler) RunLcaCliIPConfigRun(
	logger logr.Logger,
) error {
	logger.Info("Scheduling lca-cli ip-config run via systemd-run")

	args := []string{
		"--property", "ExitType=cgroup",
		"--unit", "lca-ipconfig-run",
		"--description", "lifecycle-agent: ip-config run",
		"lca-cli", "ip-config", "run",
	}

	if _, err := c.Executor.Execute("systemd-run", args...); err != nil {
		return fmt.Errorf("failed to schedule lca-cli ip-config run: %w", err)
	}

	return nil
}
