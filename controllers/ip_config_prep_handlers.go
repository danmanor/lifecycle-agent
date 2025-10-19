package controllers

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
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
	PostPivot(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error)
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

func (r *IPConfigReconciler) handlePrep(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("IPConfigPrep")
	logger.Info("Starting handlePrep")

	isBeforePivot := !isTargetStaterootBooted(ipc, r.RPMOstreeClient)

	if isBeforePivot {
		logger.Info("Running prep PrePivot handler")
		return r.PrepHandler.PrePivot(ctx, ipc, logger)
	}

	logger.Info("Running prep PostPivot handler")
	result, err := r.PrepHandler.PostPivot(ctx, ipc)
	if err != nil {
		return result, fmt.Errorf("failed to run PostPivot: %w", err)
	}

	controllerutils.SetIPPrepStatusCompleted(ipc, "Preparation completed")

	validNextStages, err := validNextStages(ipc, r.RPMOstreeClient)
	if err != nil {
		return requeueWithError(fmt.Errorf("failed to get valid next stages: %w", err))
	}
	ipc.Status.ValidNextStages = validNextStages

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
		return requeueWithHealthCheckInterval(), nil
	}

	phase, message, err := common.ReadIPConfigStatus(common.PathOutsideChroot(common.IPConfigPrepareStatusFile))
	if err != nil {
		return requeueWithError(fmt.Errorf("failed to read ip-config prepare status: %w", err))
	}

	switch phase {
	case common.IPConfigRunPhaseUnknown:
		return p.handlePrepareUnknown(ctx, ipc, logger)
	case common.IPConfigRunPhaseRunning:
		return p.handlePrepareRunning(ctx, ipc)
	case common.IPConfigRunPhaseFailed:
		return p.handlePrepareFailed(ctx, ipc, message)
	case common.IPConfigRunPhaseSucceeded:
		return p.handlePrepareSucceeded(ctx, ipc, logger)
	default:
		return requeueWithShortInterval(), nil
	}
}

func (p *IPConfigPrepHandler) handlePrepareUnknown(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	ipv4Addr, ipv6Addr := getIPAddresses(ipc)
	if err := controllerutils.CopyLcaCliToHost(logger); err != nil {
		controllerutils.SetIPPrepStatusFailed(ipc, fmt.Sprintf("failed to copy lca-cli binary: %s", err.Error()))
		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to copy lca-cli binary: %w", err))
	}

	logger.Info("Scheduling lca-cli ip-config prepare via systemd-run")
	controllerutils.SetIPPrepStatusInProgress(ipc, "IP configuration preparation is in progress")
	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

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
	if _, err := p.Executor.Execute("systemd-run", args...); err != nil {
		controllerutils.SetIPPrepStatusFailed(ipc, fmt.Sprintf("failed to run ip-config prepare: %s", err.Error()))
		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return doNotRequeue(), fmt.Errorf("failed to schedule ip-config prepare: %w", err)
	}

	return requeueWithShortInterval(), nil
}

func (p *IPConfigPrepHandler) handlePrepareRunning(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	controllerutils.SetIPPrepStatusInProgress(ipc, "ip-config prepare in progress")
	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}
	return requeueWithShortInterval(), nil
}

func (p *IPConfigPrepHandler) handlePrepareFailed(ctx context.Context, ipc *ipcv1.IPConfig, message string) (ctrl.Result, error) {
	controllerutils.SetIPPrepStatusFailed(ipc, fmt.Sprintf("ip-config prepare failed: %s", message))
	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}
	return doNotRequeue(), fmt.Errorf("ip-config prepare failed: %s", message)
}

func (p *IPConfigPrepHandler) handlePrepareSucceeded(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	controllerutils.SetIPPrepStatusInProgress(ipc, "ip-config prepare completed. Rebooting to new stateroot")
	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}
	if p.RebootClient == nil {
		return requeueWithError(fmt.Errorf("reboot client is not set"))
	}
	logger.Info("PrePivot Completed. Rebooting to new stateroot")
	if err := p.RebootClient.RebootToNewStateRoot("ip-config"); err != nil {
		controllerutils.SetIPPrepStatusFailed(ipc, fmt.Sprintf("failed to reboot to new stateroot: %s", err.Error()))
		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to reboot to new stateroot: %w", err))
	}
	return doNotRequeue(), nil
}

func (p *IPConfigPrepHandler) PostPivot(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	log.FromContext(ctx).Info("Starting health check for different components")
	if err := CheckHealth(ctx, p.NoncachedClient, log.FromContext(ctx)); err != nil {
		controllerutils.SetIPPrepStatusInProgress(ipc, fmt.Sprintf("Waiting for system to stabilize in new stateroot: %s", err.Error()))

		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithHealthCheckInterval(), nil
	}

	controllerutils.SetIPPrepStatusInProgress(ipc, "Cluster has stabilized in new stateroot")

	return doNotRequeue(), nil
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
