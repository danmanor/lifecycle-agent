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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type IPConfigPrepareHandlerInterface interface {
	PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error)
	PostPivot(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error)
}

type IPConfigPrepareHandler struct {
	Client          client.Client
	NoncachedClient client.Reader
	Executor        ops.Execute
	Ops             ops.Ops
	RebootClient    reboot.RebootIntf
	Scheme          *runtime.Scheme
	Clientset       *kubernetes.Clientset
}

func NewIPConfigPrepareHandler(
	client client.Client,
	noncachedClient client.Reader,
	executor ops.Execute,
	ops ops.Ops,
	rebootClient reboot.RebootIntf,
	scheme *runtime.Scheme,
	clientset *kubernetes.Clientset,
) IPConfigPrepareHandlerInterface {
	return &IPConfigPrepareHandler{
		Client:          client,
		NoncachedClient: noncachedClient,
		Executor:        executor,
		Ops:             ops,
		RebootClient:    rebootClient,
		Scheme:          scheme,
		Clientset:       clientset,
	}
}

func (r *IPConfigReconciler) handlePrepare(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("IPConfigPrepare")
	logger.Info("Starting handlePrepare")

	isBeforePivot := !isTargetStaterootBooted(ipc, r.RPMOstreeClient)

	if isBeforePivot {
		logger.Info("Running prepare PrePivot handler")
		return r.PrepareHandler.PrePivot(ctx, ipc, logger)
	}

	logger.Info("Running prepare PostPivot handler")
	result, err := r.PrepareHandler.PostPivot(ctx, ipc)
	if err != nil {
		return result, fmt.Errorf("failed to run PostPivot: %w", err)
	}

	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.Completed,
		metav1.ConditionFalse,
		"Preparation completed",
		ipc.Generation,
	)
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.Completed,
		metav1.ConditionTrue,
		"Preparation completed",
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

	logger.Info("Prepare completed successfully")

	return result, nil
}

func (p *IPConfigPrepareHandler) PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	if err := CheckHealth(ctx, p.NoncachedClient, logger.WithName("HealthCheck")); err != nil {
		msg := fmt.Sprintf("Waiting for system to stabilize before starting preparation: %s", err.Error())
		logger.Info(msg)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.InProgress,
			metav1.ConditionTrue,
			msg,
			ipc.Generation,
		)
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

func (p *IPConfigPrepareHandler) handlePrepareUnknown(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	ipv4Addr, ipv6Addr := getIPAddresses(ipc)
	if err := controllerutils.CopyLcaCliToHost(logger); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to copy lca-cli binary: %s", err.Error()),
			ipc.Generation,
		)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to copy lca-cli binary: %s", err.Error()),
			ipc.Generation,
		)
		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return doNotRequeue(), fmt.Errorf("failed to copy lca-cli binary: %w", err)
	}

	logger.Info("Scheduling lca-cli ip-config prepare via systemd-run")
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.InProgress,
		metav1.ConditionTrue,
		"IP configuration preparation is in progress",
		ipc.Generation,
	)
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
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to run ip-config prepare: %s", err.Error()),
			ipc.Generation,
		)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to run ip-config prepare: %s", err.Error()),
			ipc.Generation,
		)
		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return doNotRequeue(), fmt.Errorf("failed to schedule ip-config prepare: %w", err)
	}

	return requeueWithShortInterval(), nil
}

func (p *IPConfigPrepareHandler) handlePrepareRunning(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.InProgress,
		metav1.ConditionTrue,
		"ip-config prepare in progress",
		ipc.Generation,
	)
	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}
	return requeueWithShortInterval(), nil
}

func (p *IPConfigPrepareHandler) handlePrepareFailed(ctx context.Context, ipc *ipcv1.IPConfig, message string) (ctrl.Result, error) {
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.Failed,
		metav1.ConditionFalse,
		fmt.Sprintf("ip-config prepare failed: %s", message),
		ipc.Generation,
	)
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.Failed,
		metav1.ConditionFalse,
		fmt.Sprintf("ip-config prepare failed: %s", message),
		ipc.Generation,
	)
	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}
	return doNotRequeue(), fmt.Errorf("ip-config prepare failed: %s", message)
}

func (p *IPConfigPrepareHandler) handlePrepareSucceeded(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.InProgress,
		metav1.ConditionTrue,
		"ip-config prepare completed. Rebooting to new stateroot",
		ipc.Generation,
	)
	if err := p.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}
	if p.RebootClient == nil {
		return requeueWithError(fmt.Errorf("reboot client is not set"))
	}
	logger.Info("PrePivot Completed. Rebooting to new stateroot")
	if err := p.RebootClient.RebootToNewStateRoot("ip-config"); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to reboot to new stateroot: %s", err.Error()),
			ipc.Generation,
		)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			fmt.Sprintf("failed to reboot to new stateroot: %s", err.Error()),
			ipc.Generation,
		)
		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to reboot to new stateroot: %w", err))
	}
	return doNotRequeue(), nil
}

func (p *IPConfigPrepareHandler) PostPivot(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
	log.FromContext(ctx).Info("Starting health check for different components")
	if err := CheckHealth(ctx, p.NoncachedClient, log.FromContext(ctx)); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
			controllerutils.ConditionReasons.InProgress,
			metav1.ConditionTrue,
			fmt.Sprintf("Waiting for system to stabilize in new stateroot: %s", err.Error()),
			ipc.Generation,
		)

		if err := p.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithHealthCheckInterval(), nil
	}

	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Prepare),
		controllerutils.ConditionReasons.InProgress,
		metav1.ConditionTrue,
		"Cluster has stabilized in new stateroot",
		ipc.Generation,
	)

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
