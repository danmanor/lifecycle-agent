package controllers

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/go-logr/logr"
	ibuv1 "github.com/openshift-kni/lifecycle-agent/api/imagebasedupgrade/v1"
	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/internal/reboot"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type IPConfigPrepHandlerInterface interface {
	PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error)
	PostPivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error)
}

type IPConfigPrepHandler struct {
	Client          client.Client
	NoncachedClient client.Reader
	Executor        ops.Execute
	Ops             ops.Ops
	RebootClient    reboot.RebootIntf
	Scheme          *runtime.Scheme
	Clientset       *kubernetes.Clientset
}

func NewIPConfigPrepHandler(
	client client.Client,
	noncachedClient client.Reader,
	executor ops.Execute,
	ops ops.Ops,
	rebootClient reboot.RebootIntf,
	scheme *runtime.Scheme,
	clientset *kubernetes.Clientset,
) IPConfigPrepHandlerInterface {
	return &IPConfigPrepHandler{
		Client:          client,
		NoncachedClient: noncachedClient,
		Executor:        executor,
		Ops:             ops,
		RebootClient:    rebootClient,
		Scheme:          scheme,
		Clientset:       clientset,
	}
}

func (r *IPConfigReconciler) handlePrep(ctx context.Context, ipc *ipcv1.IPConfig) (res ctrl.Result, err error) {
	logger := log.FromContext(ctx).WithName("IPConfigPrep")
	logger.Info("Starting handlePrep")

	if err := r.validateConfigurationFlowReadiness(ctx, ipc); err != nil {
		controllerutils.SetIPPrepStatusFailed(
			ipc,
			fmt.Sprintf("validation of configuration flow readiness failed: %s", err.Error()),
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return doNotRequeue(), nil
	}

	controllerutils.SetIPIdleStatusFalse(
		ipc,
		controllerutils.ConditionReasons.InProgress,
		"IP configuration is in progress",
	)
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	if err := statusIPsMatchSpec(ipc); err == nil {
		controllerutils.SetIPPrepStatusCompleted(ipc, "Desired IP equals current IP")
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		logger.Info("Spec IPs already match current status")
		return doNotRequeue(), nil
	}

	isBeforePivot := !isTargetStaterootBooted(ipc, r.RPMOstreeClient)

	if isBeforePivot {
		logger.Info("Running prep PrePivot handler")
		return r.PrepHandler.PrePivot(ctx, ipc, logger)
	}

	logger.Info("Running prep PostPivot handler")
	result, err := r.PrepHandler.PostPivot(ctx, ipc, logger)
	if err != nil {
		return result, fmt.Errorf("post pivot failed: %w", err)
	}

	controllerutils.SetIPPrepStatusCompleted(ipc, "Preparation completed")
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Info("Prepare completed successfully")

	return result, nil
}

func (p *IPConfigPrepHandler) PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	if err := CheckHealth(ctx, p.NoncachedClient, logger.WithName("HealthCheck")); err != nil {
		msg := fmt.Sprintf("Waiting for system to stabilize before starting preparation: %s", err.Error())
		logger.Info(msg)
		controllerutils.SetIPPrepStatusInProgress(ipc, msg)
		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithHealthCheckInterval(), nil
	}

	phase, message, err := common.ReadIPConfigStatus(common.PathOutsideChroot(common.IPConfigPrepareStatusFile))
	if err != nil {
		controllerutils.SetIPPrepStatusFailed(
			ipc,
			fmt.Sprintf("failed to read ip-config prepare status: %s", err.Error()),
		)
		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to read ip-config prepare status: %w", err))
	}

	switch phase {
	case common.IPConfigRunPhaseUnknown:
		return p.handlePrepareUnknown(ctx, ipc, logger)
	case common.IPConfigRunPhaseRunning:
		return p.handlePrepareRunning(ctx, ipc)
	case common.IPConfigRunPhaseFailed:
		return p.handlePrepareFailed(ctx, ipc, logger, message)
	case common.IPConfigRunPhaseSucceeded:
		return p.handlePrepareSucceeded(ctx, ipc, logger)
	default:
		return requeueWithShortInterval(), nil
	}
}

func (p *IPConfigPrepHandler) handlePrepareUnknown(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	controllerutils.SetIPPrepStatusInProgress(
		ipc,
		"IP configuration preparation is in progress",
	)
	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	ipv4Addr, ipv6Addr := getIPAddresses(ipc)

	if err := controllerutils.CopyLcaCliToHost(logger); err != nil {
		controllerutils.SetIPPrepStatusFailed(
			ipc,
			fmt.Sprintf("failed to copy lca-cli binary: %s", err.Error()),
		)
		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to copy lca-cli binary: %w", err))
	}

	if err := p.RunLcaCliIPConfigPrepare(logger, ipv4Addr, ipv6Addr); err != nil {
		controllerutils.SetIPPrepStatusFailed(
			ipc,
			fmt.Sprintf("failed to run ip-config prepare: %s", err.Error()),
		)
		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		logger.Error(fmt.Errorf("failed to schedule ip-config prepare: %w", err), "failed to schedule ip-config prepare")
		return doNotRequeue(), nil
	}

	// should not reach here on successful ip-config prepare

	return doNotRequeue(), nil
}

func (p *IPConfigPrepHandler) handlePrepareRunning(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	controllerutils.SetIPPrepStatusInProgress(ipc, "ip-config prepare in progress")
	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}
	return requeueWithShortInterval(), nil
}

func (p *IPConfigPrepHandler) handlePrepareFailed(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
	message string,
) (ctrl.Result, error) {
	controllerutils.SetIPPrepStatusFailed(
		ipc,
		fmt.Sprintf("ip-config prepare failed: %s", message),
	)
	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Error(fmt.Errorf("ip-config prepare failed: %s", message), "ip-config prepare failed")

	return doNotRequeue(), nil
}

func (p *IPConfigPrepHandler) handlePrepareSucceeded(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	controllerutils.SetIPPrepStatusInProgress(
		ipc,
		"ip-config prepare completed. Rebooting to new stateroot",
	)
	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	if p.RebootClient == nil {
		controllerutils.SetIPPrepStatusFailed(
			ipc,
			"reboot client is not set",
		)
		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("reboot client is not set"))
	}

	logger.Info("PrePivot Completed. Rebooting to new stateroot")
	if err := p.RebootClient.RebootToNewStateRoot("ip-config"); err != nil {
		controllerutils.SetIPPrepStatusFailed(
			ipc,
			fmt.Sprintf("failed to reboot to new stateroot: %s", err.Error()),
		)
		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to reboot to new stateroot: %w", err))
	}
	return doNotRequeue(), nil
}

func (p *IPConfigPrepHandler) PostPivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	log.FromContext(ctx).Info("Starting health check for different components")
	if err := CheckHealth(ctx, p.NoncachedClient, logger); err != nil {
		controllerutils.SetIPPrepStatusInProgress(ipc, fmt.Sprintf("Waiting for system to stabilize in new stateroot: %s", err.Error()))
		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithHealthCheckInterval(), fmt.Errorf("Waiting for system to stabilize in new stateroot: %s", err.Error())
	}

	if err := p.startIPConfigInitMonitor(ipc, logger); err != nil {
		return requeueWithError(fmt.Errorf("failed to start ip-config init monitor: %w", err))
	}

	controllerutils.SetIPPrepStatusInProgress(ipc, "Cluster has stabilized in new stateroot")
	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	return doNotRequeue(), nil
}

// startIPConfigInitMonitor writes the auto-rollback config and starts the init-monitor transient unit post-pivot
func (p *IPConfigPrepHandler) startIPConfigInitMonitor(ipc *ipcv1.IPConfig, logger logr.Logger) error {
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
		"--property", "ExitType=cgroup",
		"--unit", common.IPCInitMonitorUnit,
		"--description", "lifecycle-agent: ip-config init monitor",
		"lca-cli", "init-monitor", "--monitor", "--mode", "ipconfig",
	}

	if _, err := p.Executor.Execute("systemd-run", monitorArgs...); err != nil {
		return fmt.Errorf("failed to start ip-config init monitor: %w", err)
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

func (r *IPConfigReconciler) validateConfigurationFlowReadiness(ctx context.Context, ipc *ipcv1.IPConfig) error {
	if err := r.validateIPConfigSpec(ipc); err != nil {
		return fmt.Errorf("validation of IPConfig spec failed: %w", err)
	}

	if err := r.validateIBUIdle(ctx); err != nil {
		return fmt.Errorf("validation of IBU Idle failed: %w", err)
	}

	return nil
}

func (r *IPConfigReconciler) validateIBUIdle(ctx context.Context) error {
	ibu := &ibuv1.ImageBasedUpgrade{}
	if err := r.NoncachedClient.Get(ctx, client.ObjectKey{Name: controllerutils.IBUName}, ibu); err != nil {
		return fmt.Errorf("failed to get IBU: %w", err)
	}

	if !controllerutils.IsIdleConditionTrue(ibu.Status.Conditions) {
		return fmt.Errorf("IBU is not in Idle stage")
	}

	return nil
}

func (r *IPConfigReconciler) validateIPConfigSpec(ipc *ipcv1.IPConfig) error {
	if isIPTransitionRequested(ipc) {
		if err := r.validateIPConfigStage(ipc); err != nil {
			return fmt.Errorf("invalid IPConfig stage: %w", err)
		}
	}

	if err := validateNetworkSpec(ipc); err != nil {
		return fmt.Errorf("network spec is not valid: %w", err)
	}

	return nil
}

// validateSpecIPsInMachineNetworks ensures that, for each provided family, the given IP
// is valid and contained within the provided machine network CIDR.
func validateNetworkSpec(ipc *ipcv1.IPConfig) error {
	atLeastOneStackExists := false
	if v := ipc.Spec.IPv4; v != nil {
		if v.Address == "" {
			return fmt.Errorf("IPv4 address is required for IPv4 stack")
		}
		if v.MachineNetwork == "" {
			return fmt.Errorf("IPv4 machine network is required for IPv4 stack")
		}

		atLeastOneStackExists = true
		ipStr := strings.Split(v.Address, "/")[0]
		ip := net.ParseIP(ipStr)
		if ip == nil || ip.To4() == nil {
			return fmt.Errorf("invalid IPv4 address: %s", ipStr)
		}
		_, ipNet, err := net.ParseCIDR(v.MachineNetwork)
		if err != nil {
			return fmt.Errorf("invalid IPv4 machine network CIDR: %s", v.MachineNetwork)
		}
		if ipNet.IP.To4() == nil {
			return fmt.Errorf("ipv4 machineNetwork must be an IPv4 CIDR: %s", v.MachineNetwork)
		}
		if !ipNet.Contains(ip) {
			return fmt.Errorf("IPv4 address %s is not within machine network %s", ipStr, v.MachineNetwork)
		}
	}

	if v := ipc.Spec.IPv6; v != nil {
		if v.Address == "" {
			return fmt.Errorf("IPv6 address is required for IPv6 stack")
		}
		if v.MachineNetwork == "" {
			return fmt.Errorf("IPv6 machine network is required for IPv6 stack")
		}

		atLeastOneStackExists = true
		addr := strings.Split(v.Address, "/")[0]
		ipStr := strings.Trim(addr, "[]")
		ip := net.ParseIP(ipStr)
		if ip == nil || ip.To4() != nil {
			return fmt.Errorf("invalid IPv6 address: %s", ipStr)
		}
		_, ipNet, err := net.ParseCIDR(v.MachineNetwork)
		if err != nil {
			return fmt.Errorf("invalid IPv6 machine network CIDR: %s", v.MachineNetwork)
		}
		if ipNet.IP.To4() != nil {
			return fmt.Errorf("ipv6 machineNetwork must be an IPv6 CIDR: %s", v.MachineNetwork)
		}
		if !ipNet.Contains(ip) {
			return fmt.Errorf("IPv6 address %s is not within machine network %s", ipStr, v.MachineNetwork)
		}
	}

	if !atLeastOneStackExists && ipc.Spec.Stage != ipcv1.IPStages.Idle {
		return fmt.Errorf("at least one of IPv4 or IPv6 stack must be provided for non-idle stage")
	}

	return nil
}

// RunLcaCliIPConfigPrepare schedules an lca-cli ip-config prepare via systemd-run.
func (c *IPConfigPrepHandler) RunLcaCliIPConfigPrepare(
	logger logr.Logger,
	ipv4Addr string,
	ipv6Addr string,
) error {
	logger.Info("Scheduling lca-cli ip-config prepare via systemd-run")

	args := []string{
		"--property", "ExitType=cgroup",
		"--unit", "lca-ipconfig-prepare",
		"--description", "lifecycle-agent: ip-config prepare",
		"lca-cli", "ip-config", "prepare",
	}

	if ipv4Addr != "" {
		args = append(args, "--ipv4-address", ipv4Addr)
	}
	if ipv6Addr != "" {
		args = append(args, "--ipv6-address", ipv6Addr)
	}

	if _, err := c.Executor.Execute("systemd-run", args...); err != nil {
		return fmt.Errorf("failed to schedule lca-cli ip-config prepare: %w", err)
	}

	return nil
}
