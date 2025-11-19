package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/internal/ostreeclient"
	"github.com/openshift-kni/lifecycle-agent/internal/reboot"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	rpmostreeclient "github.com/openshift-kni/lifecycle-agent/lca-cli/ostreeclient"
	"github.com/openshift-kni/lifecycle-agent/utils"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	machineconfigv1 "github.com/openshift/api/machineconfiguration/v1"
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
	OstreeClient    ostreeclient.IClient
	ChrootOps       ops.Ops
	RebootClient    reboot.RebootIntf
}

func NewIPConfigConfigPhasesHandler(
	client client.Client,
	noncachedClient client.Reader,
	rpmostreeClient rpmostreeclient.IClient,
	ostreeClient ostreeclient.IClient,
	chrootOps ops.Ops,
	rebootClient reboot.RebootIntf,
) IPConfigConfigPhasesHandlerInterface {
	return &IPConfigConfigPhasesHandler{
		Client:          client,
		NoncachedClient: noncachedClient,
		RPMOstreeClient: rpmostreeClient,
		OstreeClient:    ostreeClient,
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
		if err := h.validateStageTransition(ctx, ipc); err != nil {
			controllerutils.SetIPConfigStatusFailed(
				ipc,
				fmt.Sprintf("validation of Config stage transition failed: %s", err.Error()),
			)
			if uerr := h.Client.Status().Update(ctx, ipc); uerr != nil {
				return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
			}
			return requeueWithError(fmt.Errorf("validation of Config stage transition failed: %w", err))
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

	if onlyDNSResolutionFamilyChanged(ipc) {
		logger.Info("Only DNSResolutionFamily changed; applying dnsmasq filter only")
		if err := common.SetDNSMasqFilterInMachineConfig(ctx, h.Client, ipc.Spec.DNSResolutionFamily); err != nil {
			controllerutils.SetIPConfigStatusFailed(ipc, fmt.Sprintf("failed to update dnsmasq filter: %s", err.Error()))
			if uerr := h.Client.Status().Update(ctx, ipc); uerr != nil {
				return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
			}
			return requeueWithError(fmt.Errorf("failed to update dnsmasq filter in machine config: %w", err))
		}

		ipc.Status.DNSResolutionFamily = ipc.Spec.DNSResolutionFamily
		controllerutils.SetIPConfigStatusCompleted(ipc, "Configuration completed successfully")
		if err := h.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

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

// onlyDNSResolutionFamilyChanged returns true when the only requested change is DNSResolutionFamily.
// It requires that no IPv4/IPv6/VLAN changes are requested compared to status.
func onlyDNSResolutionFamilyChanged(ipc *ipcv1.IPConfig) bool {
	if ipc.Spec.DNSResolutionFamily == "" || ipc.Spec.DNSResolutionFamily == ipc.Status.DNSResolutionFamily {
		return false
	}

	if ipc.Status.Network == nil ||
		ipc.Status.Network.HostNetwork == nil ||
		ipc.Status.Network.ClusterNetwork == nil {
		return false
	}

	if ipc.Spec.VLAN != nil &&
		ipc.Status.Network.HostNetwork.VLANID != ipc.Spec.VLAN.ID {
		return false
	}

	if !familySpecMatchesStatus(
		ipc.Spec.IPv4,
		ipc.Status.Network.HostNetwork.IPv4,
		ipc.Status.Network.ClusterNetwork.IPv4,
	) {
		return false
	}
	if !familySpecMatchesStatus(
		ipc.Spec.IPv6,
		ipc.Status.Network.HostNetwork.IPv6,
		ipc.Status.Network.ClusterNetwork.IPv6,
	) {
		return false
	}

	return true
}

// familySpecMatchesStatus checks that the requested IP family spec does not introduce changes
// compared to current host and cluster status. It returns true when no change is needed.
func familySpecMatchesStatus(
	spec *ipcv1.IPFamilyConfig,
	host *ipcv1.HostIPStatus,
	cluster *ipcv1.ClusterIPStatus,
) bool {
	if spec == nil {
		return true
	}
	if host == nil || cluster == nil || cluster.Address == "" {
		return false
	}
	if spec.Gateway != "" && spec.Gateway != host.Gateway {
		return false
	}
	if spec.DNSServer != "" && spec.DNSServer != host.DNSServer {
		return false
	}
	if spec.Address != "" && !ipEqual(strings.Split(spec.Address, "/")[0], cluster.Address) {
		return false
	}
	if spec.MachineNetwork != "" && !cidrEqual(spec.MachineNetwork, cluster.MachineNetwork) {
		return false
	}

	return true
}

// setDNSMasqFilterInMachineConfig updates the dnsmasq MachineConfig to filter DNS answers
// according to the desired IP family ("ipv4" or "ipv6").
// setDNSMasqFilterInMachineConfig moved to internal/common; use common.SetDNSMasqFilterInMachineConfig

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

	if err := reboot.WriteIPCAutoRollbackConfigFile(logger, ipc); err != nil {
		controllerutils.SetIPConfigStatusFailed(ipc, fmt.Sprintf("failed to write ip-config auto-rollback config: %s", err.Error()))
		if uerr := c.Client.Status().Update(ctx, ipc); uerr != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", uerr))
		}
		return requeueWithError(fmt.Errorf("failed to write ip-config auto-rollback config: %w", err))
	}

	monitorEnabled := true
	if ipc.GetAnnotations()[common.AutoRollbackOnFailureInitMonitorAnnotation] == common.AutoRollbackDisableValue {
		monitorEnabled = false
	}

	var vlanID int
	if ipc.Spec.VLAN != nil {
		vlanID = ipc.Spec.VLAN.ID
	}
	if err := runLcaCliIPConfigPrepare(c.ChrootOps, logger, ipv4Addr, ipv6Addr, vlanID, monitorEnabled); err != nil {
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

	requeue, err := h.enableInitMonitorService()
	if err != nil {
		controllerutils.SetIPConfigStatusFailed(
			ipc,
			fmt.Sprintf("failed to enable init monitor service: %s", err.Error()),
		)
		if err := h.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to enable init monitor service: %w", err))
	}
	if requeue {
		return requeueWithCustomInterval(3 * time.Second), nil
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

	return requeueWithShortInterval(), nil
}

// enableInitMonitorService enables the init monitor service in the new stateroot.
// the init monitor service is disabling itself upon finishing its work, so we need to wait
// for it and only then enable it again to ensure it is the last action performed.
func (h *IPConfigConfigPhasesHandler) enableInitMonitorService() (bool, error) {
	if _, err := h.ChrootOps.SystemctlAction("is-active", common.IPCInitMonitorService); err == nil {
		if _, err := h.ChrootOps.SystemctlAction("stop", common.IPCInitMonitorService); err != nil {
			return true, fmt.Errorf("failed to stop init monitor service: %w", err)
		}
	}

	if _, err := h.ChrootOps.SystemctlAction("is-enabled", common.IPCInitMonitorService); err == nil {
		return true, nil
	}

	if _, err := h.ChrootOps.SystemctlAction("enable", common.IPCInitMonitorService); err != nil {
		return true, fmt.Errorf("failed to disable init monitor service: %w", err)
	}

	return false, nil
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

	if err := h.disableNodeipRerunUnit(); err != nil {
		controllerutils.SetIPConfigStatusFailed(
			ipc,
			fmt.Sprintf("failed to disable %s: %s", utils.NodeipRerunUnitPath, err.Error()),
		)
		if err := h.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to disable %s: %w", utils.NodeipRerunUnitPath, err))
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

func (h *IPConfigConfigPhasesHandler) disableNodeipRerunUnit() error {
	if _, err := h.ChrootOps.SystemctlAction("is-enabled", utils.NodeipRerunUnitPath); err == nil {
		if _, err := h.ChrootOps.SystemctlAction("disable", utils.NodeipRerunUnitPath); err != nil {
			return fmt.Errorf("failed to disable %s: %w", utils.NodeipRerunUnitPath, err)
		}
	}

	return nil
}

func statusIPsMatchSpec(ipc *ipcv1.IPConfig) error {
	mismatches := []string{}

	if ipc.Spec.IPv4 == nil && ipc.Spec.IPv6 == nil {
		return fmt.Errorf("nothing requested, shouldn't happen")
	}

	if ipc.Status.Network == nil || ipc.Status.Network.HostNetwork == nil || ipc.Status.Network.ClusterNetwork == nil {
		return fmt.Errorf("host/cluster network not yet populated")
	}

	if ipc.Spec.DNSResolutionFamily != "" {
		if ipc.Status.DNSResolutionFamily != ipc.Spec.DNSResolutionFamily {
			mismatches = append(mismatches, fmt.Sprintf(
				"dnsResolutionFamily mismatch: spec=%s status=%s",
				ipc.Spec.DNSResolutionFamily, ipc.Status.DNSResolutionFamily,
			))
		}
	}

	if ipc.Spec.VLAN != nil {
		if ipc.Status.Network.HostNetwork.VLANID != ipc.Spec.VLAN.ID {
			mismatches = append(mismatches, fmt.Sprintf(
				"vlan mismatch: spec=%d status=%d",
				ipc.Spec.VLAN.ID, ipc.Status.Network.HostNetwork.VLANID,
			))
		}
	}

	if v4 := ipc.Spec.IPv4; v4 != nil {
		v4Mismatches := checkFamilyStatusMatchesSpec(
			"ipv4",
			v4,
			ipc.Status.Network.HostNetwork.IPv4,
			ipc.Status.Network.ClusterNetwork.IPv4,
		)
		mismatches = append(mismatches, v4Mismatches...)
	}

	if v6 := ipc.Spec.IPv6; v6 != nil {
		v6Mismatches := checkFamilyStatusMatchesSpec(
			"ipv6",
			v6,
			ipc.Status.Network.HostNetwork.IPv6,
			ipc.Status.Network.ClusterNetwork.IPv6,
		)
		mismatches = append(mismatches, v6Mismatches...)
	}

	if len(mismatches) > 0 {
		return fmt.Errorf("desired network not observed in status: %s", strings.Join(mismatches, ", "))
	}

	return nil
}

func checkFamilyStatusMatchesSpec(
	family string,
	spec *ipcv1.IPFamilyConfig,
	host *ipcv1.HostIPStatus,
	cluster *ipcv1.ClusterIPStatus,
) []string {
	mismatches := []string{}

	if host == nil {
		mismatches = append(mismatches, fmt.Sprintf("hostNetwork.%s missing", family))
	} else {
		if spec.Gateway != "" && spec.Gateway != host.Gateway {
			mismatches = append(mismatches, fmt.Sprintf(
				"%s gateway mismatch: spec=%s status=%s",
				family, spec.Gateway, host.Gateway,
			))
		}
		if spec.DNSServer != "" && spec.DNSServer != host.DNSServer {
			mismatches = append(mismatches, fmt.Sprintf(
				"%s dns mismatch: spec=%s status=%s",
				family, spec.DNSServer, host.DNSServer,
			))
		}
	}

	if cluster == nil || cluster.Address == "" {
		mismatches = append(mismatches, fmt.Sprintf("cluster %s not observed: %s address missing", family, family))
	} else if !ipEqual(spec.Address, cluster.Address) {
		mismatches = append(mismatches, fmt.Sprintf(
			"cluster %s not observed: want %s got %s",
			family, spec.Address, cluster.Address,
		))
	}

	if spec.MachineNetwork != "" {
		if cluster == nil || cluster.MachineNetwork == "" {
			mismatches = append(mismatches, fmt.Sprintf("cluster %s machineNetwork not observed: want %s", family, spec.MachineNetwork))
		} else if !cidrEqual(spec.MachineNetwork, cluster.MachineNetwork) {
			mismatches = append(mismatches, fmt.Sprintf(
				"cluster %s machineNetwork not observed: want %s got %s",
				family, spec.MachineNetwork, cluster.MachineNetwork,
			))
		}
	}

	return mismatches
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

	if v := ipc.Spec.VLAN; v != nil {
		cfg.VLANID = v.ID
	}

	recertImage := getRecertImage(ipc)
	if recertImage != "" {
		cfg.RecertImage = recertImage
	}

	if v, ok := ipc.GetAnnotations()[controllerutils.RecertPullSecretAnnotation]; ok && v != "" {
		cfg.PullSecretRefName = v
	}

	if ipc.Spec.DNSResolutionFamily != "" {
		cfg.DNSIPFamily = ipc.Spec.DNSResolutionFamily
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

// getRecertImage resolves the recert image to use in priority: annotation, env, default
func getRecertImage(ipc *ipcv1.IPConfig) string {
	if v := ipc.GetAnnotations()[controllerutils.RecertImageAnnotation]; v != "" {
		return v
	}
	if v := os.Getenv(common.RecertImageEnvKey); v != "" {
		return v
	}
	return common.DefaultRecertImage
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
	vlanID int,
	monitorEnabled bool,
) error {
	logger.Info("Scheduling lca-cli ip-config prepare via systemd-run")

	args := []string{
		"--property", controllerutils.SystemdExitTypeCgroup,
		"--unit", controllerutils.IPConfigPrepareUnit,
		"--description", controllerutils.IPConfigPrepareDescription,
		controllerutils.LcaCliBinaryName, "ip-config", "prepare",
	}

	if monitorEnabled {
		args = append(args, "--install-init-monitor")
	}

	if ipv4Addr != "" {
		args = append(args, "--ipv4-address", ipv4Addr)
	}
	if ipv6Addr != "" {
		args = append(args, "--ipv6-address", ipv6Addr)
	}
	if vlanID > 0 {
		args = append(args, "--vlan-id", fmt.Sprintf("%d", vlanID))
	}

	if _, err := chrootOps.RunSystemdAction(args...); err != nil {
		return fmt.Errorf("failed to schedule lca-cli ip-config prepare: %w", err)
	}
	return nil
}

func (h *IPConfigConfigStageHandler) validateStageTransition(ctx context.Context, ipc *ipcv1.IPConfig) error {
	if err := validateIPConfigStage(ipc); err != nil {
		return fmt.Errorf("validation of Config stage transition failed: %w", err)
	}

	if err := h.validateDNSMasqMCExists(ctx); err != nil {
		return fmt.Errorf("validation of DNSMasq machine config failed: %w", err)
	}

	return nil
}

func (h *IPConfigConfigStageHandler) validateDNSMasqMCExists(ctx context.Context) error {
	mc := &machineconfigv1.MachineConfig{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: common.DnsmasqMachineConfigName}, mc); err != nil {
		return fmt.Errorf("failed to get dnsmasq machine config: %w", err)
	}

	return nil
}
