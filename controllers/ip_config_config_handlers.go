package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
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

type IPConfigConfigStageHandler struct {
	Client                client.Client
	ChrootOps             ops.Ops
	TwoPhaseConfigHandler IPConfigTwoPhaseStageHandler
}

func NewIPConfigConfigStageHandler(
	client client.Client,
	chrootOps ops.Ops,
	twoPhaseHandler IPConfigTwoPhaseStageHandler,
) IPConfigStageHandler {
	return &IPConfigConfigStageHandler{
		Client:                client,
		ChrootOps:             chrootOps,
		TwoPhaseConfigHandler: twoPhaseHandler,
	}
}

type IPConfigTwoPhaseConfigurationHandler struct {
	Client          client.Client
	NoncachedClient client.Reader
	Ops             ops.Ops
	RebootClient    reboot.RebootIntf
}

func NewIPConfigTwoPhaseConfigurationHandler(
	client client.Client,
	noncachedClient client.Reader,
	ops ops.Ops,
	rebootClient reboot.RebootIntf,
) IPConfigTwoPhaseStageHandler {
	return &IPConfigTwoPhaseConfigurationHandler{
		Client:          client,
		NoncachedClient: noncachedClient,
		Ops:             ops,
		RebootClient:    rebootClient,
	}
}

// Handle executes the Config stage state machine
func (h *IPConfigConfigStageHandler) Handle(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("IPConfigConfig")
	logger.Info("Starting handleConfig")

	if isIPTransitionRequested(ipc) {
		if err := validateIPConfigStage(ipc); err != nil {
			controllerutils.SetIPConfigStatusFailed(
				ipc,
				"invalid transition: "+string(ipc.Spec.Stage),
			)
			if err := h.Client.Status().Update(ctx, ipc); err != nil {
				return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
			}
			return doNotRequeue(), nil
		}
	}

	phase, message, err := common.ReadIPConfigStatus(
		common.PathOutsideChroot(common.IPConfigRunStatusFile),
		h.ChrootOps,
	)
	if err != nil {
		return requeueWithError(fmt.Errorf("failed to read ip-config run status: %w", err))
	}

	switch phase {
	case common.IPConfigRunPhaseUnknown:
		return h.handleConfigUnknown(ctx, ipc, logger)
	case common.IPConfigRunPhaseRunning:
		return h.handleConfigRunning(ctx, ipc)
	case common.IPConfigRunPhaseFailed:
		return h.handleConfigFailed(ctx, ipc, logger, message)
	case common.IPConfigRunPhaseSucceeded:
		return h.handleConfigSucceeded(ctx, ipc, logger)
	default:
		return requeueWithShortInterval(), nil
	}
}

func (h *IPConfigConfigStageHandler) handleConfigUnknown(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
	controllerutils.SetIPConfigStatusInProgress(ipc, "Configuration is in progress")
	if err := h.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Info("Running IP config PrePivot handler")
	result, err := h.TwoPhaseConfigHandler.PrePivot(ctx, ipc, logger)
	if err != nil {
		return result, fmt.Errorf("failed to run PrePivot: %w", err)
	}
	return result, nil
}

func (h *IPConfigConfigStageHandler) handleConfigRunning(
	ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	controllerutils.SetIPConfigStatusInProgress(ipc, "ip-config run in progress")
	if err := h.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}
	return requeueWithShortInterval(), nil
}

func (h *IPConfigConfigStageHandler) handleConfigFailed(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
	message string,
) (ctrl.Result, error) {
	controllerutils.SetIPConfigStatusFailed(ipc, fmt.Sprintf("ip-config run failed: %s", message))
	if err := h.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Error(fmt.Errorf("ip-config run failed: %s", message), "ip-config run failed")
	return doNotRequeue(), nil
}

func (h *IPConfigConfigStageHandler) handleConfigSucceeded(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
	controllerutils.StopIPPhase(h.Client, logger, ipc, IPConfigConfigPhasePrepivot)

	logger.Info("Running IP config PostPivot handler")
	result, err := h.TwoPhaseConfigHandler.PostPivot(ctx, ipc, logger)
	if err != nil {
		return result, fmt.Errorf("post pivot failed: %w", err)
	}

	controllerutils.SetIPConfigStatusCompleted(ipc, "Configuration completed")
	if err := h.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}
	controllerutils.StopIPStageHistory(h.Client, logger, ipc)

	logger.Info("config completed successfully")
	return result, nil
}

func (c *IPConfigTwoPhaseConfigurationHandler) PrePivot(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
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

func (c *IPConfigTwoPhaseConfigurationHandler) PostPivot(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
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

	// rely on controller reconcile to refresh host/cluster network statuses continuously

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

// statusIPsMatchSpec validates that all provided network config in spec matches
// the observed status in hostNetwork and clusterNetwork.
func statusIPsMatchSpec(ipc *ipcv1.IPConfig) error {
	mismatches := []string{}

	if ipc.Spec.IPv4 == nil && ipc.Spec.IPv6 == nil {
		return fmt.Errorf("nothing requested, shouldn't happen")
	}

	if ipc.Status.HostNetwork == nil || ipc.Status.ClusterNetwork == nil {
		return fmt.Errorf("host/cluster network not yet populated")
	}

	// Validate IPv4 if requested
	if v4 := ipc.Spec.IPv4; v4 != nil {
		if ipc.Status.HostNetwork.IPv4 == nil {
			mismatches = append(mismatches, "hostNetwork.ipv4 missing")
		} else {
			if err := compareAddressWithPrefix(
				controllerutils.IPv4FamilyName,
				v4.Address,
				ipc.Status.HostNetwork.IPv4.Address,
			); err != nil {
				mismatches = append(mismatches, err.Error())
			}
			if !cidrEqual(v4.MachineNetwork, ipc.Status.HostNetwork.IPv4.MachineNetwork) {
				mismatches = append(mismatches, fmt.Sprintf("ipv4 machineNetwork mismatch: spec=%s status=%s", v4.MachineNetwork, ipc.Status.HostNetwork.IPv4.MachineNetwork))
			}
			if v4.Gateway != "" && v4.Gateway != ipc.Status.HostNetwork.IPv4.Gateway {
				mismatches = append(mismatches, fmt.Sprintf("ipv4 gateway mismatch: spec=%s status=%s", v4.Gateway, ipc.Status.HostNetwork.IPv4.Gateway))
			}
			if v4.DNSServer != "" && v4.DNSServer != ipc.Status.HostNetwork.IPv4.DNSServer {
				mismatches = append(mismatches, fmt.Sprintf("ipv4 dns mismatch: spec=%s status=%s", v4.DNSServer, ipc.Status.HostNetwork.IPv4.DNSServer))
			}
		}

		wantIP, _, err := splitAddr(v4.Address)
		if err != nil {
			mismatches = append(mismatches, fmt.Sprintf("ipv4 spec address invalid: %v", err))
		} else {
			if ipc.Status.ClusterNetwork == nil || ipc.Status.ClusterNetwork.IPv4 == nil || ipc.Status.ClusterNetwork.IPv4.Address == "" {
				mismatches = append(mismatches, "cluster ipv4 not observed: ipv4 address missing")
			} else if !ipEqual(wantIP, ipc.Status.ClusterNetwork.IPv4.Address) {
				mismatches = append(mismatches, fmt.Sprintf("cluster ipv4 not observed: want %s got %s", wantIP, ipc.Status.ClusterNetwork.IPv4.Address))
			}
		}

		if v4.MachineNetwork != "" {
			if ipc.Status.ClusterNetwork == nil || ipc.Status.ClusterNetwork.IPv4 == nil || ipc.Status.ClusterNetwork.IPv4.MachineNetwork == "" {
				mismatches = append(mismatches, fmt.Sprintf("cluster ipv4 machineNetwork not observed: want %s", v4.MachineNetwork))
			} else if !cidrEqual(v4.MachineNetwork, ipc.Status.ClusterNetwork.IPv4.MachineNetwork) {
				mismatches = append(mismatches, fmt.Sprintf("cluster ipv4 machineNetwork not observed: want %s got %s", v4.MachineNetwork, ipc.Status.ClusterNetwork.IPv4.MachineNetwork))
			}
		}
	}

	// Validate IPv6 if requested
	if v6 := ipc.Spec.IPv6; v6 != nil {
		if ipc.Status.HostNetwork.IPv6 == nil {
			mismatches = append(mismatches, "hostNetwork.ipv6 missing")
		} else {
			if err := compareAddressWithPrefix(
				controllerutils.IPv6FamilyName,
				v6.Address,
				ipc.Status.HostNetwork.IPv6.Address,
			); err != nil {
				mismatches = append(mismatches, err.Error())
			}
			if !cidrEqual(v6.MachineNetwork, ipc.Status.HostNetwork.IPv6.MachineNetwork) {
				mismatches = append(mismatches, fmt.Sprintf("ipv6 machineNetwork mismatch: spec=%s status=%s", v6.MachineNetwork, ipc.Status.HostNetwork.IPv6.MachineNetwork))
			}
			if v6.Gateway != "" && v6.Gateway != ipc.Status.HostNetwork.IPv6.Gateway {
				mismatches = append(mismatches, fmt.Sprintf("ipv6 gateway mismatch: spec=%s status=%s", v6.Gateway, ipc.Status.HostNetwork.IPv6.Gateway))
			}
			if v6.DNSServer != "" && v6.DNSServer != ipc.Status.HostNetwork.IPv6.DNSServer {
				mismatches = append(mismatches, fmt.Sprintf("ipv6 dns mismatch: spec=%s status=%s", v6.DNSServer, ipc.Status.HostNetwork.IPv6.DNSServer))
			}
		}

		wantIP, _, err := splitAddr(v6.Address)
		if err != nil {
			mismatches = append(mismatches, fmt.Sprintf("ipv6 spec address invalid: %v", err))
		} else {
			if ipc.Status.ClusterNetwork == nil || ipc.Status.ClusterNetwork.IPv6 == nil || ipc.Status.ClusterNetwork.IPv6.Address == "" {
				mismatches = append(mismatches, "cluster ipv6 not observed: ipv6 address missing")
			} else if !ipEqual(wantIP, ipc.Status.ClusterNetwork.IPv6.Address) {
				mismatches = append(mismatches, fmt.Sprintf("cluster ipv6 not observed: want %s got %s", wantIP, ipc.Status.ClusterNetwork.IPv6.Address))
			}
		}
		// Machine network must be present and match exactly
		if v6.MachineNetwork != "" {
			if ipc.Status.ClusterNetwork == nil || ipc.Status.ClusterNetwork.IPv6 == nil || ipc.Status.ClusterNetwork.IPv6.MachineNetwork == "" {
				mismatches = append(mismatches, fmt.Sprintf("cluster ipv6 machineNetwork not observed: want %s", v6.MachineNetwork))
			} else if !cidrEqual(v6.MachineNetwork, ipc.Status.ClusterNetwork.IPv6.MachineNetwork) {
				mismatches = append(mismatches, fmt.Sprintf("cluster ipv6 machineNetwork not observed: want %s got %s", v6.MachineNetwork, ipc.Status.ClusterNetwork.IPv6.MachineNetwork))
			}
		}
	}

	if len(mismatches) > 0 {
		return fmt.Errorf("desired network not observed in status: %s", strings.Join(mismatches, ", "))
	}

	return nil
}

func compareAddressWithPrefix(family, specAddr, statusAddr string) error {
	sSpecIP, sSpecPref, err := splitAddr(specAddr)
	if err != nil {
		return fmt.Errorf("%s spec address invalid: %v", family, err)
	}
	sStatIP, sStatPref, err := splitAddr(statusAddr)
	if err != nil {
		return fmt.Errorf("%s status address invalid: %v", family, err)
	}
	if !ipEqual(sSpecIP, sStatIP) || sSpecPref != sStatPref {
		return fmt.Errorf("%s address mismatch: spec=%s/%d status=%s/%d", family, sSpecIP, sSpecPref, sStatIP, sStatPref)
	}
	return nil
}

func splitAddr(addr string) (string, int, error) {
	addr = strings.Trim(addr, "[]")
	ip, pref, ok := strings.Cut(addr, "/")
	if !ok {
		return "", 0, fmt.Errorf("missing prefix")
	}
	pi := net.ParseIP(ip)
	if pi == nil {
		return "", 0, fmt.Errorf("invalid ip")
	}
	n, err := strconv.Atoi(pref)
	if err != nil {
		return "", 0, fmt.Errorf("invalid prefix")
	}
	return pi.String(), n, nil
}

func ipEqual(a, b string) bool {
	return net.ParseIP(a).Equal(net.ParseIP(b))
}

func cidrEqual(a, b string) bool {
	na, ap, ea := parseCIDR(a)
	nb, bp, eb := parseCIDR(b)
	if ea != nil || eb != nil {
		return a == b
	}
	return ap == bp && net.ParseIP(na).Equal(net.ParseIP(nb))
}

func parseCIDR(c string) (string, int, error) {
	c = strings.Trim(c, "[]")
	_, ipNet, err := net.ParseCIDR(c)
	if err != nil {
		return "", 0, err
	}
	ones, _ := ipNet.Mask.Size()
	return ipNet.IP.String(), ones, nil
}

// per-phase handlers for IPConfig run status
// Reconciler-specific config handlers migrated to IPConfigConfigStageHandler

// writeIPConfigRunConfigToNewStateroot writes the ip-config run configuration file into the new stateroot etc
func (c *IPConfigTwoPhaseConfigurationHandler) writeIPConfigRunConfig(ipc *ipcv1.IPConfig) error {
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
	if err := c.Ops.WriteFile(common.PathOutsideChroot(common.IPConfigRunFlagsFile), data, 0o600); err != nil {
		return fmt.Errorf("failed to write ip-config run config: %w", err)
	}

	return nil
}

// RunLcaCliIPConfigRun schedules an lca-cli ip-config run via systemd-run.
func (c *IPConfigTwoPhaseConfigurationHandler) RunLcaCliIPConfigRun(
	logger logr.Logger,
) error {
	logger.Info("Scheduling lca-cli ip-config run via systemd-run")

	args := []string{
		"--property", controllerutils.SystemdExitTypeCgroup,
		"--unit", controllerutils.IPConfigRunUnit,
		"--description", controllerutils.IPConfigRunDescription,
		controllerutils.LcaCliBinaryName, "ip-config", "run",
	}

	if _, err := c.Ops.SystemctlAction("run", args...); err != nil {
		return fmt.Errorf("failed to schedule lca-cli ip-config run: %w", err)
	}

	return nil
}
