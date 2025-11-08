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
	rpmostreeclient "github.com/openshift-kni/lifecycle-agent/lca-cli/ostreeclient"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	IPConfigPhasePrePivot          = "pre-pivot"
	IPConfigPhasePreConfiguration  = "pre-configuration"
	IPConfigPhasePostConfiguration = "post-configuration"
)

//go:generate mockgen -source=ipc_config_handlers.go -package=controllers -destination=ipc_config_handlers_mock.go
type IPConfigConfigPhasesHandlerInterface interface {
	PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error)
	PreConfiguration(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error)
	PostConfiguration(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error)
}

type IPConfigConfigPhasesHandler struct {
	Client          client.Client
	NoncachedClient client.Reader
	RPMOstreeClient rpmostreeclient.IClient
	ChrootOps       ops.Ops
	RebootClient    reboot.RebootIntf
}

func NewIPConfigConfigPhasesHandler(
	client client.Client,
	noncachedClient client.Reader,
	rpmostreeClient rpmostreeclient.IClient,
	chrootOps ops.Ops,
	rebootClient reboot.RebootIntf,
) IPConfigConfigPhasesHandlerInterface {
	return &IPConfigConfigPhasesHandler{
		Client:          client,
		NoncachedClient: noncachedClient,
		RPMOstreeClient: rpmostreeClient,
		ChrootOps:       chrootOps,
		RebootClient:    rebootClient,
	}
}

type IPConfigConfigStageHandler struct {
	Client          client.Client
	NoncachedClient client.Reader
	RPMOstreeClient rpmostreeclient.IClient
	ChrootOps       ops.Ops
	PhasesHandler   IPConfigConfigPhasesHandlerInterface
}

func NewIPConfigConfigStageHandler(
	client client.Client,
	noncachedClient client.Reader,
	rpmOstreeClient rpmostreeclient.IClient,
	chrootOps ops.Ops,
	phasesHandler IPConfigConfigPhasesHandlerInterface,
) IPConfigStageHandler {
	return &IPConfigConfigStageHandler{
		Client:          client,
		NoncachedClient: noncachedClient,
		RPMOstreeClient: rpmOstreeClient,
		ChrootOps:       chrootOps,
		PhasesHandler:   phasesHandler,
	}
}

func (h *IPConfigConfigStageHandler) Handle(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("IPConfigConfig")
	logger.Info("Starting handleConfig")

	if isIPTransitionRequested(ipc) {
		if err := validateIPConfigStage(ipc); err != nil {
			controllerutils.SetIPConfigStatusFailed(
				ipc,
				"invalid transition: "+string(ipc.Spec.Stage),
			)
			if uerr := h.Client.Status().Update(ctx, ipc); uerr != nil {
				return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
			}
			return requeueWithError(fmt.Errorf("invalid IPConfig stage: %w", err))
		}
	}

	status, message, err := common.ReadIPConfigStatus(
		common.PathOutsideChroot(common.IPConfigPrepareStatusFile),
		h.ChrootOps,
	)
	if err != nil {
		controllerutils.SetIPConfigStatusFailed(ipc, fmt.Sprintf("failed to read ip-config prepare status: %s", err.Error()))
		if uerr := h.Client.Status().Update(ctx, ipc); uerr != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
		}
		return requeueWithError(fmt.Errorf("failed to read ip-config prepare status: %w", err))
	}

	switch status {
	case common.IPConfigPhaseUnknown:
		return h.handlePrepareUnknown(ctx, ipc, logger)
	case common.IPConfigPhaseRunning:
		return h.handlePrepareRunning()
	case common.IPConfigPhaseFailed:
		return h.handlePrepareFailed(ctx, ipc, logger, message)
	case common.IPConfigPhaseSucceeded:
		return h.handlePrepareSucceeded(ctx, ipc, logger)
	default:
		return requeueWithShortInterval(), nil
	}
}

func (h *IPConfigConfigStageHandler) handlePrepareUnknown(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
	if err := statusIPsMatchSpec(ipc); err == nil {
		controllerutils.SetIPConfigStatusCompleted(ipc, "IPConfig status matches spec; nothing to do")
		if err := h.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		logger.Info("IPConfig status matches spec")
		return doNotRequeue(), nil
	}

	controllerutils.SetIPConfigStatusInProgress(ipc, "Configuration preparation is in progress")
	if err := h.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	result, err := h.PhasesHandler.PrePivot(ctx, ipc, logger)
	if err != nil {
		return result, fmt.Errorf("pre-pivot phase failed: %w", err)
	}

	return result, nil
}

func (h *IPConfigConfigStageHandler) handlePrepareRunning() (ctrl.Result, error) {
	return requeueWithShortInterval(), nil
}

func (h *IPConfigConfigStageHandler) handlePrepareFailed(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
	message string,
) (ctrl.Result, error) {
	controllerutils.SetIPConfigStatusFailed(ipc, fmt.Sprintf("Configuration preparation failed: %s", message))
	if err := h.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Error(
		fmt.Errorf("failed to run configuration preparation: %s", message),
		"failed to run configuration preparation",
	)

	return doNotRequeue(), nil
}

func (h *IPConfigConfigStageHandler) handlePrepareSucceeded(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
	controllerutils.StopIPPhase(h.Client, logger, ipc, IPConfigPhasePrePivot)
	logger.Info("Finished pre-pivot phase successfully")

	if !isTargetStaterootBooted(ipc, h.RPMOstreeClient) {
		controllerutils.SetIPConfigStatusFailed(ipc, "host didn't reboot into new stateroot")
		if err := h.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return doNotRequeue(), nil
	}

	runPhase, runMessage, err := common.ReadIPConfigStatus(
		common.PathOutsideChroot(common.IPConfigRunStatusFile),
		h.ChrootOps,
	)
	if err != nil {
		controllerutils.SetIPConfigStatusFailed(ipc, fmt.Sprintf("failed to read ip-config run status: %s", err.Error()))
		if uerr := h.Client.Status().Update(ctx, ipc); uerr != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
		}
		return requeueWithError(fmt.Errorf("failed to read ip-config run status: %w", err))
	}

	switch runPhase {
	case common.IPConfigPhaseUnknown:
		return h.handleRunUnknown(ctx, ipc, logger)
	case common.IPConfigPhaseRunning:
		return h.handleRunRunning()
	case common.IPConfigPhaseFailed:
		return h.handleRunFailed(ctx, ipc, logger, runMessage)
	case common.IPConfigPhaseSucceeded:
		return h.handleRunSucceeded(ctx, ipc, logger)
	default:
		return requeueWithShortInterval(), nil
	}
}

func (h *IPConfigConfigStageHandler) handleRunUnknown(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
	controllerutils.SetIPConfigStatusInProgress(ipc, "Configuration is in progress")
	if err := h.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	result, err := h.PhasesHandler.PreConfiguration(ctx, ipc, logger)
	if err != nil {
		return result, fmt.Errorf("failed to run pre configuration: %w", err)
	}

	return result, nil
}

func (h *IPConfigConfigStageHandler) handleRunRunning() (ctrl.Result, error) {
	return requeueWithShortInterval(), nil
}

func (h *IPConfigConfigStageHandler) handleRunFailed(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
	message string,
) (ctrl.Result, error) {
	controllerutils.SetIPConfigStatusFailed(ipc, fmt.Sprintf("ip-config run failed: %s", message))
	if err := h.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Error(
		fmt.Errorf("failed to run ip-config: %s", message),
		"failed to run ip-config",
	)

	return doNotRequeue(), nil
}

func (h *IPConfigConfigStageHandler) handleRunSucceeded(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
	controllerutils.StopIPPhase(h.Client, logger, ipc, IPConfigPhasePreConfiguration)
	logger.Info("Finished pre-configuration phase successfully, running post configuration")

	result, err := h.PhasesHandler.PostConfiguration(ctx, ipc, logger)
	if err != nil {
		return result, fmt.Errorf("failed to run post configuration: %w", err)
	}

	logger.Info("Configuration completed successfully")

	return result, nil
}

func (c *IPConfigConfigPhasesHandler) PrePivot(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
	controllerutils.StartIPPhase(c.Client, logger, ipc, IPConfigPhasePrePivot)
	logger.Info("Starting pre-pivot phase")

	if err := CheckHealth(ctx, c.NoncachedClient, logger.WithName("HealthCheck")); err != nil {
		msg := fmt.Sprintf("Waiting for system to stabilize: %s", err.Error())
		controllerutils.SetIPConfigStatusInProgress(ipc, msg)
		if uerr := c.Client.Status().Update(ctx, ipc); uerr != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
		}
		return requeueWithHealthCheckInterval(), nil
	}

	ipv4Addr, ipv6Addr := getIPAddresses(ipc)
	if err := controllerutils.CopyLcaCliToHost(logger); err != nil {
		controllerutils.SetIPConfigStatusFailed(ipc, fmt.Sprintf("failed to copy lca-cli binary: %s", err.Error()))
		if uerr := c.Client.Status().Update(ctx, ipc); uerr != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
		}
		return requeueWithError(fmt.Errorf("failed to copy lca-cli binary: %w", err))
	}

	if err := runLcaCliIPConfigPrepare(c.ChrootOps, logger, ipv4Addr, ipv6Addr); err != nil {
		controllerutils.SetIPConfigStatusFailed(ipc, fmt.Sprintf("failed to run ip-config prepare: %s", err.Error()))
		if uerr := c.Client.Status().Update(ctx, ipc); uerr != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
		}
		logger.Error(fmt.Errorf("failed to schedule ip-config prepare: %w", err), "failed to schedule ip-config prepare")
		return doNotRequeue(), nil
	}

	// We shouldn't reach here on successful ip-config prepare

	return requeueWithShortInterval(), nil
}

func (h *IPConfigConfigPhasesHandler) PreConfiguration(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
	controllerutils.StartIPPhase(h.Client, logger, ipc, IPConfigPhasePreConfiguration)
	logger.Info("Starting pre-configuration phase")

	if err := CheckHealth(ctx, h.NoncachedClient, logger); err != nil {
		controllerutils.SetIPConfigStatusInProgress(
			ipc,
			fmt.Sprintf("Waiting for system to stabilize: %s", err.Error()),
		)
		if err := h.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithHealthCheckInterval(), nil
	}

	if err := h.startIPConfigInitMonitor(ipc, logger); err != nil {
		controllerutils.SetIPConfigStatusFailed(
			ipc,
			fmt.Sprintf("failed to start ip-config init monitor: %s", err.Error()),
		)
		if err := h.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to start ip-config init monitor: %w", err))
	}

	if err := h.writeIPConfigRunConfig(ipc); err != nil {
		controllerutils.SetIPConfigStatusFailed(
			ipc,
			fmt.Sprintf("failed to write ip-config run config: %s", err.Error()),
		)
		if err := h.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(fmt.Errorf("failed to write ip-config run config: %w", err))
	}

	if err := h.RunLcaCliIPConfigRun(logger); err != nil {
		controllerutils.SetIPConfigStatusFailed(
			ipc,
			fmt.Sprintf("failed to run ip-config: %s", err.Error()),
		)
		if err := h.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(fmt.Errorf("failed to run ip-config run command: %w", err))
	}

	// We shouldn't reach here on successful ip-config run

	return doNotRequeue(), nil
}

func (h *IPConfigConfigPhasesHandler) PostConfiguration(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
	controllerutils.StartIPPhase(h.Client, logger, ipc, IPConfigPhasePostConfiguration)
	logger.Info("Starting post-configuration phase")

	if err := CheckHealth(ctx, h.NoncachedClient, logger); err != nil {
		controllerutils.SetIPConfigStatusInProgress(
			ipc,
			fmt.Sprintf("Waiting for system to stabilize: %s", err.Error()),
		)
		if err := h.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithHealthCheckInterval(), nil
	}

	controllerutils.SetIPConfigStatusInProgress(ipc, "Cluster has stabilized")
	if err := h.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	if err := statusIPsMatchSpec(ipc); err != nil {
		controllerutils.SetIPConfigStatusInProgress(
			ipc,
			fmt.Sprintf("Waiting for current IPs to match spec: %s", err.Error()),
		)
		if err := h.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithHealthCheckInterval(), nil
	}

	if err := h.RebootClient.DisableInitMonitor(); err != nil {
		controllerutils.SetIPConfigStatusFailed(
			ipc,
			fmt.Sprintf("failed to disable init monitor: %s", err.Error()),
		)
		if err := h.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(fmt.Errorf("failed to disable init monitor: %w", err))
	}

	controllerutils.StopIPPhase(h.Client, logger, ipc, IPConfigPhasePostConfiguration)
	controllerutils.StopIPStageHistory(h.Client, logger, ipc)
	controllerutils.SetIPConfigStatusCompleted(ipc, "Configuration completed successfully")
	if err := h.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Info("Finished post-configuration phase successfully")

	return doNotRequeue(), nil
}

// statusIPsMatchSpec validates that all provided network config in spec matches
// the observed status in hostNetwork and clusterNetwork.
func statusIPsMatchSpec(ipc *ipcv1.IPConfig) error {
	mismatches := []string{}

	if ipc.Spec.IPv4 == nil && ipc.Spec.IPv6 == nil {
		return fmt.Errorf("nothing requested, shouldn't happen")
	}

	if ipc.Status.Network == nil || ipc.Status.Network.HostNetwork == nil || ipc.Status.Network.ClusterNetwork == nil {
		return fmt.Errorf("host/cluster network not yet populated")
	}

	// Validate IPv4 if requested
	if v4 := ipc.Spec.IPv4; v4 != nil {
		if ipc.Status.Network.HostNetwork.IPv4 == nil {
			mismatches = append(mismatches, "hostNetwork.ipv4 missing")
		} else {
			if err := compareAddressWithPrefix(
				controllerutils.IPv4FamilyName,
				v4.Address,
				ipc.Status.Network.HostNetwork.IPv4.Address,
			); err != nil {
				mismatches = append(
					mismatches, fmt.Sprintf(
						"ipv4 address mismatch: spec=%s status=%s",
						v4.Address,
						ipc.Status.Network.HostNetwork.IPv4.Address,
					))
			}
			if !cidrEqual(v4.MachineNetwork, ipc.Status.Network.HostNetwork.IPv4.MachineNetwork) {
				mismatches = append(
					mismatches, fmt.Sprintf(
						"ipv4 machineNetwork mismatch: spec=%s status=%s",
						v4.MachineNetwork,
						ipc.Status.Network.HostNetwork.IPv4.MachineNetwork,
					))
			}
			if v4.Gateway != "" && v4.Gateway != ipc.Status.Network.HostNetwork.IPv4.Gateway {
				mismatches = append(mismatches, fmt.Sprintf(
					"ipv4 gateway mismatch: spec=%s status=%s",
					v4.Gateway,
					ipc.Status.Network.HostNetwork.IPv4.Gateway,
				))
			}
			if v4.DNSServer != "" && v4.DNSServer != ipc.Status.Network.HostNetwork.IPv4.DNSServer {
				mismatches = append(
					mismatches,
					fmt.Sprintf("ipv4 dns mismatch: spec=%s status=%s",
						v4.DNSServer,
						ipc.Status.Network.HostNetwork.IPv4.DNSServer,
					))
			}
		}

		if ipc.Status.Network.ClusterNetwork == nil || ipc.Status.Network.ClusterNetwork.IPv4 == nil || ipc.Status.Network.ClusterNetwork.IPv4.Address == "" {
			mismatches = append(
				mismatches,
				"cluster ipv4 not observed: ipv4 address missing",
			)
		} else if !ipEqual(v4.Address, ipc.Status.Network.ClusterNetwork.IPv4.Address) {
			mismatches = append(
				mismatches,
				fmt.Sprintf("cluster ipv4 not observed: want %s got %s", v4.Address, ipc.Status.Network.ClusterNetwork.IPv4.Address),
			)
		}

		if v4.MachineNetwork != "" {
			if ipc.Status.Network.ClusterNetwork == nil || ipc.Status.Network.ClusterNetwork.IPv4 == nil || ipc.Status.Network.ClusterNetwork.IPv4.MachineNetwork == "" {
				mismatches = append(
					mismatches,
					fmt.Sprintf("cluster ipv4 machineNetwork not observed: want %s", v4.MachineNetwork),
				)
			} else if !cidrEqual(v4.MachineNetwork, ipc.Status.Network.ClusterNetwork.IPv4.MachineNetwork) {
				mismatches = append(
					mismatches,
					fmt.Sprintf("cluster ipv4 machineNetwork not observed: want %s got %s", v4.MachineNetwork, ipc.Status.Network.ClusterNetwork.IPv4.MachineNetwork),
				)
			}
		}
	}

	// Validate IPv6 if requested
	if v6 := ipc.Spec.IPv6; v6 != nil {
		if ipc.Status.Network.HostNetwork.IPv6 == nil {
			mismatches = append(mismatches, "hostNetwork.ipv6 missing")
		} else {
			if err := compareAddressWithPrefix(
				controllerutils.IPv6FamilyName,
				v6.Address,
				ipc.Status.Network.HostNetwork.IPv6.Address,
			); err != nil {
				mismatches = append(mismatches, fmt.Sprintf(
					"ipv6 address mismatch: spec=%s status=%s",
					v6.Address,
					ipc.Status.Network.HostNetwork.IPv6.Address,
				))
			}
			if !cidrEqual(v6.MachineNetwork, ipc.Status.Network.HostNetwork.IPv6.MachineNetwork) {
				mismatches = append(
					mismatches, fmt.Sprintf(
						"ipv6 machineNetwork mismatch: spec=%s status=%s",
						v6.MachineNetwork,
						ipc.Status.Network.HostNetwork.IPv6.MachineNetwork,
					))
			}
			if v6.Gateway != "" && v6.Gateway != ipc.Status.Network.HostNetwork.IPv6.Gateway {
				mismatches = append(
					mismatches, fmt.Sprintf(
						"ipv6 gateway mismatch: spec=%s status=%s",
						v6.Gateway,
						ipc.Status.Network.HostNetwork.IPv6.Gateway,
					))
			}
			if v6.DNSServer != "" && v6.DNSServer != ipc.Status.Network.HostNetwork.IPv6.DNSServer {
				mismatches = append(mismatches, fmt.Sprintf(
					"ipv6 dns mismatch: spec=%s status=%s",
					v6.DNSServer,
					ipc.Status.Network.HostNetwork.IPv6.DNSServer,
				))
			}
		}

		if ipc.Status.Network.ClusterNetwork == nil || ipc.Status.Network.ClusterNetwork.IPv6 == nil || ipc.Status.Network.ClusterNetwork.IPv6.Address == "" {
			mismatches = append(mismatches, "cluster ipv6 not observed: ipv6 address missing")
		} else if !ipEqual(v6.Address, ipc.Status.Network.ClusterNetwork.IPv6.Address) {
			mismatches = append(mismatches, fmt.Sprintf(
				"cluster ipv6 not observed: want %s got %s",
				v6.Address,
				ipc.Status.Network.ClusterNetwork.IPv6.Address,
			))
		}

		if v6.MachineNetwork != "" {
			if ipc.Status.Network.ClusterNetwork == nil || ipc.Status.Network.ClusterNetwork.IPv6 == nil || ipc.Status.Network.ClusterNetwork.IPv6.MachineNetwork == "" {
				mismatches = append(mismatches, fmt.Sprintf("cluster ipv6 machineNetwork not observed: want %s", v6.MachineNetwork))
			} else if !cidrEqual(v6.MachineNetwork, ipc.Status.Network.ClusterNetwork.IPv6.MachineNetwork) {
				mismatches = append(mismatches,
					fmt.Sprintf("cluster ipv6 machineNetwork not observed: want %s got %s",
						v6.MachineNetwork,
						ipc.Status.Network.ClusterNetwork.IPv6.MachineNetwork,
					))
			}
		}
	}

	if len(mismatches) > 0 {
		return fmt.Errorf("desired network not observed in status: %s", strings.Join(mismatches, ", "))
	}

	return nil
}

func compareAddressWithPrefix(family, specAddr, statusAddr string) error {
	specIP, specPref, specHasPref, err := parseAddrMaybePrefix(specAddr)
	if err != nil {
		return fmt.Errorf("%s spec address invalid: %v", family, err)
	}
	statIP, statPref, statHasPref, err := parseAddrMaybePrefix(statusAddr)
	if err != nil {
		return fmt.Errorf("%s status address invalid: %v", family, err)
	}
	if !ipEqual(specIP, statIP) {
		return fmt.Errorf("%s address mismatch: ip differs: spec=%s status=%s", family, specAddr, statusAddr)
	}
	if specHasPref && statHasPref && specPref != statPref {
		return fmt.Errorf("%s address mismatch: prefix differs: spec=%s/%d status=%s/%d", family, specIP, specPref, statIP, statPref)
	}
	return nil
}

func parseAddrMaybePrefix(addr string) (string, int, bool, error) {
	a := strings.Trim(addr, "[]")
	if strings.Contains(a, "/") {
		ip, pref, err := splitAddr(a)
		if err != nil {
			return "", 0, false, err
		}
		return ip, pref, true, nil
	}
	pi := net.ParseIP(a)
	if pi == nil {
		return "", 0, false, fmt.Errorf("invalid ip")
	}
	return pi.String(), 0, false, nil
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
func (c *IPConfigConfigPhasesHandler) writeIPConfigRunConfig(ipc *ipcv1.IPConfig) error {
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

	recertImage := getRecertImage(ipc)
	if recertImage != "" {
		cfg.RecertImage = recertImage
	}

	if ipc.Spec.Recert != nil &&
		ipc.Spec.Recert.PullSecretRef != nil &&
		ipc.Spec.Recert.PullSecretRef.Name != "" {
		cfg.PullSecretRefName = ipc.Spec.Recert.PullSecretRef.Name
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal ip-config run config: %w", err)
	}
	if err := c.ChrootOps.WriteFile(common.PathOutsideChroot(common.IPConfigRunFlagsFile), data, 0o600); err != nil {
		return fmt.Errorf("failed to write ip-config run config: %w", err)
	}

	return nil
}

// RunLcaCliIPConfigRun schedules an lca-cli ip-config run via systemd-run.
func (c *IPConfigConfigPhasesHandler) RunLcaCliIPConfigRun(
	logger logr.Logger,
) error {
	logger.Info("Scheduling lca-cli ip-config run via systemd-run")

	args := []string{
		"--property", controllerutils.SystemdExitTypeCgroup,
		"--unit", controllerutils.IPConfigRunUnit,
		"--description", controllerutils.IPConfigRunDescription,
		controllerutils.LcaCliBinaryName, "ip-config", "run",
	}

	if _, err := c.ChrootOps.RunSystemdAction(args...); err != nil {
		return fmt.Errorf("failed to schedule lca-cli ip-config run: %w", err)
	}

	return nil
}

// Helpers migrated from prep stage for unified Config flow

func getIPAddresses(ipc *ipcv1.IPConfig) (string, string) {
	ipv4Addr := ""
	if ipc.Spec.IPv4 != nil && ipc.Spec.IPv4.Address != "" {
		ipv4Addr = strings.Split(ipc.Spec.IPv4.Address, "/")[0]
	}
	ipv6Addr := ""
	if ipc.Spec.IPv6 != nil && ipc.Spec.IPv6.Address != "" {
		ipv6Addr = strings.Split(ipc.Spec.IPv6.Address, "/")[0]
	}
	return ipv4Addr, ipv6Addr
}

func runLcaCliIPConfigPrepare(
	chrootOps ops.Ops,
	logger logr.Logger,
	ipv4Addr string,
	ipv6Addr string,
) error {
	logger.Info("Scheduling lca-cli ip-config prepare via systemd-run")

	args := []string{
		"--property", controllerutils.SystemdExitTypeCgroup,
		"--unit", controllerutils.IPConfigPrepareUnit,
		"--description", controllerutils.IPConfigPrepareDescription,
		controllerutils.LcaCliBinaryName, "ip-config", "prepare",
	}

	if ipv4Addr != "" {
		args = append(args, "--ipv4-address", ipv4Addr)
	}
	if ipv6Addr != "" {
		args = append(args, "--ipv6-address", ipv6Addr)
	}

	if _, err := chrootOps.RunSystemdAction(args...); err != nil {
		return fmt.Errorf("failed to schedule lca-cli ip-config prepare: %w", err)
	}
	return nil
}

// startIPConfigInitMonitor writes the auto-rollback config and starts the init-monitor unit post-pivot
func (c *IPConfigConfigPhasesHandler) startIPConfigInitMonitor(
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) error {
	initMonitorDisabled := false
	if val, exists := ipc.GetAnnotations()[common.AutoRollbackOnFailureInitMonitorAnnotation]; exists {
		if val == common.AutoRollbackDisableValue {
			initMonitorDisabled = true
		}
	}

	if initMonitorDisabled {
		logger.Info("IPConfig init monitor disabled via annotation; not starting monitor")
		return nil
	}

	if err := reboot.WriteIPCAutoRollbackConfigFile(logger, ipc); err != nil {
		return fmt.Errorf("failed to write IPConfig auto-rollback config: %w", err)
	}

	monitorArgs := []string{
		"--property", controllerutils.SystemdExitTypeCgroup,
		"--unit", common.IPCInitMonitorUnit,
		"--description", controllerutils.IPConfigInitMonitorDescription,
		controllerutils.LcaCliBinaryName, "init-monitor", "--monitor", "--mode", "ipconfig",
	}

	if _, err := c.ChrootOps.RunSystemdAction(monitorArgs...); err != nil {
		return fmt.Errorf("failed to start ip-config init monitor: %w", err)
	}
	return nil
}
