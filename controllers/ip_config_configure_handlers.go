package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-logr/logr"
	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/internal/ostreeclient"
	"github.com/openshift-kni/lifecycle-agent/internal/reboot"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	cp "github.com/otiai10/copy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type ConfigureHandlerInterface interface {
	PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error)
	PostPivot(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error)
}

type ConfigureHandler struct {
	Client          client.Client
	NoncachedClient client.Reader
	Executor        ops.Execute
	Ops             ops.Ops
	RebootClient    reboot.RebootIntf
	OstreeClient    ostreeclient.IClient
	logger          logr.Logger
}

func NewConfigureHandler(
	client client.Client,
	noncachedClient client.Reader,
	executor ops.Execute,
	ops ops.Ops,
	rebootClient reboot.RebootIntf,
	logger logr.Logger,
) ConfigureHandlerInterface {
	return &ConfigureHandler{
		Client:          client,
		NoncachedClient: noncachedClient,
		Executor:        executor,
		Ops:             ops,
		RebootClient:    rebootClient,
		logger:          logger,
	}
}

func (c *ConfigureHandler) PrePivot(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) (ctrl.Result, error) {
	controllerutils.SetStatusCondition(&ipc.Status.Conditions,
		controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Configure),
		controllerutils.ConditionReasons.InProgress,
		metav1.ConditionTrue,
		controllerutils.InProgress,
		ipc.Generation,
	)

	if err := c.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	if err := c.writeIPConfigRunConfigToNewStateroot(ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to write ip-config run config to new stateroot: %w", err))
	}

	lcaBinarySrc := "/usr/local/bin/lca-cli"
	lcaBinaryDst := common.PathOutsideChroot("/var/usrlocal/bin/lca-cli")
	logger.Info("Copying lca-cli binary to host for ip-config run", "src", lcaBinarySrc, "dst", lcaBinaryDst)
	if err := cp.Copy(lcaBinarySrc, lcaBinaryDst, cp.Options{AddPermission: os.FileMode(0o777)}); err != nil {
		return requeueWithError(fmt.Errorf("failed to copy lca-cli binary to host: %w", err))
	}

	if c.RebootClient == nil {
		return requeueWithError(fmt.Errorf("reboot client is not set"))
	}

	logger.Info("PrePivot Completed. Rebooting to new stateroot")

	if err := c.RebootClient.RebootToNewStateRoot("ip-config"); err != nil {
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPCompletedConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			err.Error(),
			ipc.Generation,
		)
		controllerutils.SetStatusCondition(&ipc.Status.Conditions,
			controllerutils.GetIPInProgressConditionType(ipcv1.IPStages.Configure),
			controllerutils.ConditionReasons.Failed,
			metav1.ConditionFalse,
			err.Error(),
			ipc.Generation,
		)

		if err := c.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithError(fmt.Errorf("failed to reboot to new stateroot: %w", err))
	}

	// We should no reach here

	return doNotRequeue(), nil
}

func (c *ConfigureHandler) PostPivot(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error) {
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

	if err := refreshCurrentIPs(ctx, ipc, c.Client, c.NoncachedClient); err != nil {
		return requeueWithError(fmt.Errorf("failed to refresh current IPs: %w", err))
	}

	if err := c.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
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

func (r *IPConfigReconciler) handleConfigure(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("IPConfigConfigure")
	logger.Info("Starting handleConfigure")

	isBeforePivot := !isTargetStaterootBooted(ipc, r.RPMOstreeClient)

	if isBeforePivot {
		logger.Info("Running PrePivot handler")
		return r.ConfigureHandler.PrePivot(ctx, ipc, logger)
	}

	logger.Info("Running PostPivot handler")
	result, err := r.ConfigureHandler.PostPivot(ctx, ipc)
	if err != nil {
		return result, fmt.Errorf("failed to run PostPivot: %w", err)
	}

	logger.Info("PostPivot completed successfully")
	return result, nil
}

// writeIPConfigRunConfigToNewStateroot writes the ip-config run configuration file into the new stateroot etc
func (c *ConfigureHandler) writeIPConfigRunConfigToNewStateroot(ipc *ipcv1.IPConfig) error {
	deploymentDir, err := getNewDeploymentDir(ipc, c.OstreeClient)
	if err != nil {
		return err
	}

	etcLcaDir := filepath.Join(deploymentDir, "etc", "lca")
	if err := os.MkdirAll(etcLcaDir, 0o755); err != nil {
		return fmt.Errorf("failed to create /etc/lca dir in new stateroot: %w", err)
	}
	cfg := common.IPConfigRunConfig{
		DisableIPConfigService: true,
	}

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
	// PullSecretRef not yet resolved to a file path in controller
	cfg.RebootAutomatically = ipc.Spec.RebootAutomatically

	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal ip-config flags: %w", err)
	}
	if err := os.WriteFile(filepath.Join(etcLcaDir, "ip-config-run.json"), data, 0o600); err != nil {
		return fmt.Errorf("failed to write ip-config-run.json: %w", err)
	}
	return nil
}
