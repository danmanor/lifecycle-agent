/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ipconfig

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/internal/recert"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	"github.com/openshift-kni/lifecycle-agent/utils"
	machineconfigv1 "github.com/openshift/api/machineconfiguration/v1"
)

// NetworkIPConfig is a minimal representation of an IP and its machine network.
type NetworkIPConfig struct {
	IP             string
	MachineNetwork string
}

// IPConfig handles the IP change process
type IPConfigHandler struct {
	log            *logrus.Logger
	ops            ops.Ops
	executor       ops.Execute
	recertImage    string
	workingDir     string
	IPConfigs      []*NetworkIPConfig
	runtimeClient  runtimeclient.Client
	Proxy          *ProxyConfig
	PullSecretFile string
}

// NewIPConfig creates a new IPConfig instance
func NewIPConfig(
	log *logrus.Logger,
	ops ops.Ops,
	executor ops.Execute,
	runtimeClient runtimeclient.Client,
	recertImage string,
	workingDir string,
	ipConfigs []*NetworkIPConfig,
	proxy *ProxyConfig,
	pullSecretFile string,
) *IPConfigHandler {
	return &IPConfigHandler{
		log:            log,
		ops:            ops,
		executor:       executor,
		recertImage:    recertImage,
		workingDir:     workingDir,
		runtimeClient:  runtimeClient,
		IPConfigs:      ipConfigs,
		Proxy:          proxy,
		PullSecretFile: pullSecretFile,
	}
}

type ProxyConfig struct {
	HTTPProxy  string
	HTTPSProxy string
	NoProxy    string
}

func (i *IPConfigHandler) RunIPConfigChange() error {
	i.log.Infof("Starting IP config process")
	for _, ipConfig := range i.IPConfigs {
		i.log.Infof("Changing IP to %s, machine network to %s", ipConfig.IP, ipConfig.MachineNetwork)
	}

	ctx := context.Background()

	if err := i.createWorkingDir(); err != nil {
		return fmt.Errorf("failed to create working directory: %w", err)
	}
	defer i.cleanupWorkingDir()

	cryptoDir := path.Join(i.workingDir, common.KubeconfigCryptoDir)
	if err := i.createCryptoDir(cryptoDir); err != nil {
		return fmt.Errorf("failed to create crypto directory: %w", err)
	}

	if err := i.collectKubeConfigCrypto(ctx, cryptoDir); err != nil {
		return fmt.Errorf("failed to collect kubeconfig crypto: %w", err)
	}

	ingressCertificateCN, err := utils.GetIngressCertificateCN(ctx, i.runtimeClient)
	if err != nil {
		return fmt.Errorf("failed to get ingress certificate CN: %w", err)
	}
	i.log.Info("Found ingress certificate CN")

	installConfig, err := utils.GetInstallConfig(ctx, i.runtimeClient)
	if err != nil {
		return fmt.Errorf("failed to get install config: %w", err)
	}
	i.log.Info("Found install config")

	if err := i.CreateNetworkConfiguration(ctx); err != nil {
		return err
	}

	if err := i.ops.StopClusterServices(); err != nil {
		return err
	}

	if err := i.runRecert(ctx, installConfig, ingressCertificateCN, cryptoDir); err != nil {
		return err
	}

	if err := i.ops.EnsureNMStateConfigurationServiceEnabled(); err != nil {
		return err
	}

	if err := i.ops.EnableClusterServices(); err != nil {
		return err
	}

	if err := i.ensureNodeIPRerunService(i.IPConfigs[0].MachineNetwork); err != nil {
		return err
	}

	if err := i.configureDNSMasqOverride(); err != nil {
		return err
	}

	if err := i.cleanupNMStateAppliedFiles(); err != nil {
		return err
	}

	if err := i.removeOvnCertsFolders(); err != nil {
		return err
	}

	i.log.Info("Finished IP config process")

	return nil
}

func (i *IPConfigHandler) runRecert(ctx context.Context, installConfig string, ingressCertificateCN string, cryptoDir string) error {
	i.log.Info("Creating recert configuration file")

	oldIPs := make([]string, len(i.IPConfigs))
	newIPs := make([]string, len(i.IPConfigs))
	newMachineNetworks := make([]string, len(i.IPConfigs))

	for i, cfg := range i.IPConfigs {
		oldIPs[i] = cfg.IP
		newIPs[i] = cfg.IP
		newMachineNetworks[i] = cfg.MachineNetwork
	}

	if err := recert.CreateRecertConfigFileForIPConfig(
		oldIPs,
		newIPs,
		newMachineNetworks,
		installConfig,
		cryptoDir,
		ingressCertificateCN,
		i.workingDir,
	); err != nil {
		return fmt.Errorf("failed to create recert configuration file: %w", err)
	}

	if _, err := i.ops.RunInHostNamespace("podman", "image", "exists", i.recertImage); err != nil {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()
		_ = wait.PollUntilContextCancel(ctxWithTimeout, time.Second, true, func(ctx context.Context) (bool, error) {
			i.log.Info("pulling recert image")
			command := "podman"
			if i.Proxy != nil && (i.Proxy.HTTPProxy != "" || i.Proxy.HTTPSProxy != "" || i.Proxy.NoProxy != "") {
				command = fmt.Sprintf("HTTP_PROXY=%s HTTPS_PROXY=%s NO_PROXY=%s %s", i.Proxy.HTTPProxy, i.Proxy.HTTPSProxy, i.Proxy.NoProxy, command)
			}
			authFile := common.ImageRegistryAuthFile
			if i.PullSecretFile != "" {
				authFile = i.PullSecretFile
			}
			if _, err := i.ops.RunBashInHostNamespace(command, "pull", "--authfile", authFile, i.recertImage); err != nil {
				i.log.Warnf("failed to pull recert image, will retry, err: %s", err.Error())
				return false, nil
			}
			return true, nil
		})
	}

	i.log.Info("Starting recert full flow")

	var additionalArgs []string
	if i.Proxy != nil {
		if i.Proxy.HTTPProxy != "" {
			additionalArgs = append(additionalArgs, "-e", fmt.Sprintf("HTTP_PROXY=%s", i.Proxy.HTTPProxy))
		}
		if i.Proxy.HTTPSProxy != "" {
			additionalArgs = append(additionalArgs, "-e", fmt.Sprintf("HTTPS_PROXY=%s", i.Proxy.HTTPSProxy))
		}
		if i.Proxy.NoProxy != "" {
			additionalArgs = append(additionalArgs, "-e", fmt.Sprintf("NO_PROXY=%s", i.Proxy.NoProxy))
		}
	}

	authFile := common.ImageRegistryAuthFile
	if i.PullSecretFile != "" {
		authFile = i.PullSecretFile
	}

	err := i.ops.RecertFullFlow(
		i.recertImage,
		authFile,
		path.Join(i.workingDir, recert.RecertConfigFile),
		nil,
		nil,
		append(additionalArgs, "-v", fmt.Sprintf("%s:%s", i.workingDir, i.workingDir))...,
	)
	if err != nil {
		return fmt.Errorf("failed recert full flow: %w", err)
	}

	return nil
}

func (i *IPConfigHandler) detectBrExNetworkInterface() (string, error) {
	i.log.Info("Detecting br-ex network interface")

	if output, err := i.executor.Execute("ovs-vsctl", "list-ports", "br-ex"); err == nil {
		ports := strings.Fields(string(output))
		for _, port := range ports {
			// We want the actual port used by the node, not the patch port
			// Example output:
			// sudo ovs-vsctl list-ports br-ex
			// ens3
			// patch-br-ex_test-infra-cluster-06d0a16b-master-0-to-br-int
			if !strings.Contains(port, "br-ex") {
				i.log.Infof("Found interface via ovs-vsctl: %s", port)
				return port, nil
			}
		}
	}

	return "", fmt.Errorf("no connected network interface found")
}

func (i *IPConfigHandler) ensureNodeIPRerunService(newMachineNetwork string) error {
	i.log.Infof("Installing one-shot nodeip rerun service at %s", utils.NodeipRerunUnitPath)

	baseIP := strings.Split(newMachineNetwork, "/")[0]
	hintSed := strings.ReplaceAll(baseIP, "/", "\\/")

	templateData := &utils.NodeIPRerunServiceTemplateData{
		BaseIP:  baseIP,
		HintSed: hintSed,
	}

	unitContent, err := utils.GenerateNodeIPRerunService(templateData)
	if err != nil {
		return fmt.Errorf("failed to generate nodeip rerun service content: %w", err)
	}

	if err := os.WriteFile(utils.NodeipRerunUnitPath, []byte(unitContent), 0644); err != nil {
		return fmt.Errorf("failed to write nodeip rerun service file: %w", err)
	}

	if _, err := i.executor.Execute("systemctl", "daemon-reload"); err != nil {
		return fmt.Errorf("failed to reload systemd daemon: %w", err)
	}

	if _, err := i.executor.Execute("systemctl", "enable", "sno-nodeip-rerun.service"); err != nil {
		return fmt.Errorf("failed to enable sno-nodeip-rerun.service: %w", err)
	}

	i.log.Info("Nodeip rerun service configured successfully")
	return nil
}

func (i *IPConfigHandler) configureDNSMasqOverride() error {
	primaryIP := i.IPConfigs[0].IP
	i.log.Infof("Setting new dnsmasq configuration for %s", primaryIP)

	config := []string{
		fmt.Sprintf("SNO_DNSMASQ_IP_OVERRIDE=%s", primaryIP),
	}

	if err := os.WriteFile(common.DnsmasqOverrides, []byte(strings.Join(config, "\n")), 0o600); err != nil {
		return fmt.Errorf("failed to set dnsmasq overrides, err %w", err)
	}

	i.log.Infof("DNSMasq override configured with IP: %s", primaryIP)

	return nil
}

func (i *IPConfigHandler) cleanupNMStateAppliedFiles() error {
	i.log.Info("Cleaning up nmstate residual state files")

	filesToRemove := []string{
		"/etc/nmstate/openshift/applied",
		"/etc/nmstate/cluster.yml",
		"/etc/nmstate/cluster.applied",
	}

	for _, file := range filesToRemove {
		if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
			i.log.Warnf("Failed to remove %s: %v", file, err)
		} else if err == nil {
			i.log.Infof("Removed nmstate file: %s", file)
		}
	}

	i.log.Info("Cleaned up nmstate residual state on node")
	return nil
}

func (i *IPConfigHandler) removeOvnCertsFolders() error {
	i.log.Infof("Removing ovn certs folders")
	dirs := []string{common.OvnNodeCerts, common.MultusCerts}
	if err := utils.RemoveListOfFolders(i.log, dirs); err != nil {
		return fmt.Errorf("failed to remove ovn certs in %s: %w", dirs, err)
	}
	return nil
}

// createMachineConfig creates a machine config for IP configuration changes
func (i *IPConfigHandler) createMachineConfig(interfaceName string) (*machineconfigv1.MachineConfig, error) {
	newIPs := make([]string, len(i.IPConfigs))
	newMachineNetworks := make([]string, len(i.IPConfigs))
	for i, cfg := range i.IPConfigs {
		newIPs[i] = cfg.IP
		newMachineNetworks[i] = cfg.MachineNetwork
	}

	nmstateConfig, err := utils.GenerateNMState(interfaceName, newIPs, newMachineNetworks)
	if err != nil {
		return nil, fmt.Errorf("failed to generate NMState config: %w", err)
	}

	encodedContent := base64.StdEncoding.EncodeToString([]byte(nmstateConfig))
	ignitionConfig := fmt.Sprintf(`{
		"ignition": {
			"version": "3.2.0"
		},
		"storage": {
			"files": [
				{
					"path": "/etc/nmstate/openshift/cluster.yml",
					"mode": 420,
					"contents": {
						"source": "data:text/plain;charset=utf-8;base64,%s"
					}
				}
			]
		}
	}`, encodedContent)

	mc := &machineconfigv1.MachineConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name: "10-br-ex",
			Labels: map[string]string{
				"machineconfiguration.openshift.io/role": "master",
			},
		},
		Spec: machineconfigv1.MachineConfigSpec{
			Config: runtime.RawExtension{
				Raw: []byte(ignitionConfig),
			},
		},
	}

	return mc, nil
}

func (i *IPConfigHandler) applyNetworkConfigurationMachineConfig(ctx context.Context, interfaceName string) error {
	i.log.Info("Applying machine config for IP changes")

	mc, err := i.createMachineConfig(interfaceName)
	if err != nil {
		return fmt.Errorf("failed to create machine config: %w", err)
	}

	if err := i.runtimeClient.Create(ctx, mc); err != nil {
		existingMC := &machineconfigv1.MachineConfig{}
		if getErr := i.runtimeClient.Get(ctx, types.NamespacedName{Name: mc.Name}, existingMC); getErr == nil {
			mc.ResourceVersion = existingMC.ResourceVersion
			if updateErr := i.runtimeClient.Update(ctx, mc); updateErr != nil {
				return fmt.Errorf("failed to update existing machine config: %w", updateErr)
			}
			i.log.Infof("Updated existing machine config: %s", mc.Name)
		} else {
			return fmt.Errorf("failed to create machine config: %w", err)
		}
	} else {
		i.log.Infof("Created machine config: %s", mc.Name)
	}

	return nil
}

// CreateNetworkConfiguration applies MachineConfig to create nmstate configuration
// and waits for rollout on MCP/master and the node.
func (i *IPConfigHandler) CreateNetworkConfiguration(ctx context.Context) error {
	iface, err := i.detectBrExNetworkInterface()
	if err != nil {
		return fmt.Errorf("failed to detect br-ex network interface: %w", err)
	}
	i.log.Infof("Detected br-ex network interface: %s", iface)

	if err := i.applyNetworkConfigurationMachineConfig(ctx, iface); err != nil {
		return fmt.Errorf("failed to apply network configuration machine config: %w", err)
	}

	if err := i.waitForMCPMasterUpdated(ctx); err != nil {
		return err
	}

	if err := i.waitForNodeToApplyRenderedMC(ctx); err != nil {
		return err
	}

	return nil
}

// waitForMCPMasterUpdated mirrors the shell logic: first detect updating/rendered change, then wait for Updated=True and Degraded!=True with new rendered.
func (i *IPConfigHandler) waitForMCPMasterUpdated(ctx context.Context) error {
	i.log.Info("Waiting for MachineConfigPool/master to roll out a new rendered configuration")

	// Fetch initial rendered name
	mcp := &machineconfigv1.MachineConfigPool{}
	if err := i.runtimeClient.Get(ctx, types.NamespacedName{Name: "master"}, mcp); err != nil {
		return fmt.Errorf("failed to get mcp/master: %w", err)
	}
	prevRendered := ""
	if mcp.Status.Configuration.Name != "" {
		prevRendered = mcp.Status.Configuration.Name
	}
	if prevRendered != "" {
		i.log.Infof("Current MCP/master rendered: %s", prevRendered)
	}

	deadlineCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()

	// Phase 1: wait for either updating or rendered change
	phase1 := func() (bool, error) {
		if err := i.runtimeClient.Get(deadlineCtx, types.NamespacedName{Name: "master"}, mcp); err != nil {
			i.log.Warnf("failed to get mcp/master: %v", err)
			return false, nil
		}
		rendered := mcp.Status.Configuration.Name
		updating := mcpConditionStatus(mcp.Status.Conditions, "Updating")
		if rendered != prevRendered || updating == "True" {
			return true, nil
		}
		return false, nil
	}

	if err := wait.PollUntilContextTimeout(deadlineCtx, 5*time.Second, 15*time.Minute, true, func(ctx context.Context) (bool, error) {
		return phase1()
	}); err != nil {
		return fmt.Errorf("timed out waiting for MCP master to begin rollout: %w", err)
	}

	// Phase 2: wait for Updated=True, Degraded!=True and rendered changed
	if err := wait.PollUntilContextTimeout(deadlineCtx, 10*time.Second, 15*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := i.runtimeClient.Get(ctx, types.NamespacedName{Name: "master"}, mcp); err != nil {
			i.log.Warnf("failed to get mcp/master: %v", err)
			return false, nil
		}
		rendered := mcp.Status.Configuration.Name
		updated := mcpConditionStatus(mcp.Status.Conditions, "Updated")
		degraded := mcpConditionStatus(mcp.Status.Conditions, "Degraded")
		if rendered != "" && rendered != prevRendered && updated == "True" && degraded != "True" {
			i.log.Infof("MachineConfigPool master rolled out new rendered %s (previous %s)", rendered, prevRendered)
			return true, nil
		}
		return false, nil
	}); err != nil {
		return fmt.Errorf("timed out waiting for MCP master to reach Updated with new rendered config: %w", err)
	}

	return nil
}

// waitForNodeToApplyRenderedMC waits for the single node to have desired/current annotations equal to MCP rendered.
func (i *IPConfigHandler) waitForNodeToApplyRenderedMC(ctx context.Context) error {
	nodeName, err := utils.GetLocalNodeName(ctx, i.runtimeClient)
	if err != nil {
		return err
	}

	i.log.Infof("Waiting for node %s to apply new MachineConfig", nodeName)

	deadlineCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()

	// Helper to fetch current MCP rendered name (may change during wait)
	getRendered := func(ctx context.Context) string {
		mcp := &machineconfigv1.MachineConfigPool{}
		if err := i.runtimeClient.Get(ctx, types.NamespacedName{Name: "master"}, mcp); err != nil {
			return ""
		}
		return mcp.Status.Configuration.Name
	}

	return wait.PollUntilContextTimeout(deadlineCtx, 10*time.Second, 15*time.Minute, true, func(ctx context.Context) (bool, error) {
		rendered := getRendered(ctx)
		node := &corev1.Node{}
		if err := i.runtimeClient.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
			i.log.Warnf("failed to get node %s: %v", nodeName, err)
			return false, nil
		}
		ann := node.GetAnnotations()
		desired := ann["machineconfiguration.openshift.io/desiredConfig"]
		current := ann["machineconfiguration.openshift.io/currentConfig"]

		if rendered != "" {
			if desired == rendered && current == rendered {
				i.log.Infof("Node %s desiredConfig/currentConfig match MCP configuration %s", nodeName, rendered)
				return true, nil
			}
		} else {
			if desired != "" && desired == current {
				i.log.Infof("Node %s currentConfig equals desiredConfig (%s)", nodeName, desired)
				return true, nil
			}
		}
		return false, nil
	})
}

// mcpConditionStatus returns the Status string for a given MCP condition type if present, otherwise empty string.
func mcpConditionStatus(conds []machineconfigv1.MachineConfigPoolCondition, condType string) string {
	for _, c := range conds {
		if string(c.Type) == condType {
			return string(c.Status)
		}
	}
	return ""
}

func (i *IPConfigHandler) createWorkingDir() error {
	if err := os.MkdirAll(i.workingDir, 0o755); err != nil {
		return fmt.Errorf("failed to ensure working directory %s: %w", i.workingDir, err)
	}
	return nil
}

func (i *IPConfigHandler) cleanupWorkingDir() error {
	if err := os.RemoveAll(i.workingDir); err != nil {
		return fmt.Errorf("failed to remove working directory %s: %w", i.workingDir, err)
	}
	return nil
}

func (i *IPConfigHandler) createCryptoDir(cryptoDir string) error {
	if err := os.MkdirAll(cryptoDir, 0o755); err != nil {
		return fmt.Errorf("failed to create crypto directory: %w", err)
	}
	return nil
}

func (i *IPConfigHandler) collectKubeConfigCrypto(ctx context.Context, cryptoDir string) error {
	if err := utils.BackupKubeconfigCrypto(ctx, i.runtimeClient, cryptoDir); err != nil {
		return fmt.Errorf("failed to collect kubeconfig crypto: %w", err)
	}
	return nil
}
