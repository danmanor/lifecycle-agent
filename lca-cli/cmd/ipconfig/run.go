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

package ipconfigcmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path"
	"time"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	runtimeClient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/openshift-kni/lifecycle-agent/internal/common"
	intOstree "github.com/openshift-kni/lifecycle-agent/internal/ostreeclient"
	"github.com/openshift-kni/lifecycle-agent/internal/reboot"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ipconfig"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	rpmOstree "github.com/openshift-kni/lifecycle-agent/lca-cli/ostreeclient"
	machineconfigv1 "github.com/openshift/api/machineconfiguration/v1"
)

var (
	ipConfigScheme = runtime.NewScheme()

	ipv4Address        string
	ipv4MachineNetwork string
	ipv6Address        string
	ipv6MachineNetwork string
	ipv4Gateway        string
	ipv6Gateway        string
	ipv4DNS            string
	ipv6DNS            string
	httpProxy          string
	httpsProxy         string
	noProxy            string
	pullSecretRefName  string
	recertImage        string
)

const (
	ipFamilyIPv4 = "ipv4"
	ipFamilyIPv6 = "ipv6"
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(ipConfigScheme))
	utilruntime.Must(machineconfigv1.AddToScheme(ipConfigScheme))

	ipConfigRunCmd.Flags().StringVar(&ipv4Address, "ipv4-address", "", "Target IPv4 address")
	ipConfigRunCmd.Flags().StringVar(&ipv4MachineNetwork, "ipv4-machine-network", "", "Target IPv4 machine network CIDR")
	ipConfigRunCmd.Flags().StringVar(&ipv6Address, "ipv6-address", "", "Target IPv6 address")
	ipConfigRunCmd.Flags().StringVar(&ipv6MachineNetwork, "ipv6-machine-network", "", "Target IPv6 machine network CIDR")
	ipConfigRunCmd.Flags().StringVar(&ipv4Gateway, "ipv4-gateway", "", "IPv4 default gateway")
	ipConfigRunCmd.Flags().StringVar(&ipv6Gateway, "ipv6-gateway", "", "IPv6 default gateway")
	ipConfigRunCmd.Flags().StringVar(&ipv4DNS, "ipv4-dns", "", "IPv4 DNS server")
	ipConfigRunCmd.Flags().StringVar(&ipv6DNS, "ipv6-dns", "", "IPv6 DNS server")
	ipConfigRunCmd.Flags().StringVar(&httpProxy, "http-proxy", "", "HTTP proxy to use for network operations")
	ipConfigRunCmd.Flags().StringVar(&httpsProxy, "https-proxy", "", "HTTPS proxy to use for network operations")
	ipConfigRunCmd.Flags().StringVar(&noProxy, "no-proxy", "", "Comma-separated list of hosts that should bypass the proxy")
	ipConfigRunCmd.Flags().StringVar(&recertImage, "recert-image", "", "The full image name for the recert container tool")
	ipConfigRunCmd.Flags().StringVar(&pullSecretRefName, "pull-secret-ref-name", "", "The name of the pull secret to use for the recert container tool")
}

var ipConfigRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Execute IP configuration change",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runIPConfigChange(); err != nil {
			pkgLog.Fatalf("Error executing ip-config run: %v", err)
		}
	},
}

func runIPConfigChange() error {
	err := common.WriteIPConfigStatus(common.IPConfigRunStatusFile,
		common.IPConfigRunStatus{
			Phase:     common.IPConfigPhaseRunning,
			Message:   "ip-config run started",
			StartedAt: time.Now().UTC().Format(time.RFC3339),
		})
	if err != nil {
		return fmt.Errorf("failed to write initial status: %w", err)
	}

	if data, err := os.ReadFile(common.IPConfigRunFlagsFile); err == nil && len(data) > 0 {
		var cfg common.IPConfigRunConfig
		if jsonErr := json.Unmarshal(data, &cfg); jsonErr == nil {
			ipv4Address = cfg.IPv4Address
			ipv4MachineNetwork = cfg.IPv4MachineNetwork
			ipv6Address = cfg.IPv6Address
			ipv6MachineNetwork = cfg.IPv6MachineNetwork
			ipv4Gateway = cfg.IPv4Gateway
			ipv6Gateway = cfg.IPv6Gateway
			ipv4DNS = cfg.IPv4DNSServer
			ipv6DNS = cfg.IPv6DNSServer
			httpProxy = cfg.HTTPProxy
			httpsProxy = cfg.HTTPSProxy
			noProxy = cfg.NoProxy
			pullSecretRefName = cfg.PullSecretRefName
			recertImage = cfg.RecertImage
		} else {
			pkgLog.Warnf("failed to unmarshal ip-config run config: %v", jsonErr)
		}
	} else {
		pkgLog.Info("using command line flags")
	}

	if err := validateIPConfigArgs(ipv4Address, ipv4MachineNetwork, ipv6Address, ipv6MachineNetwork); err != nil {
		return err
	}

	effectivePrimary, err := inferPrimaryStack(ipv4Address, ipv4MachineNetwork, ipv6Address, ipv6MachineNetwork)
	if err != nil {
		return err
	}

	ipConfigs := buildIPConfigs(
		ipv4Address, ipv4MachineNetwork, ipv4Gateway, ipv4DNS,
		ipv6Address, ipv6MachineNetwork, ipv6Gateway, ipv6DNS,
		effectivePrimary,
	)

	if recertImage == "" {
		recertImage = common.DefaultRecertImage
	}

	var hostCommandsExecutor ops.Execute
	if _, err := os.Stat(common.Host); err == nil {
		hostCommandsExecutor = ops.NewChrootExecutor(pkgLog, true, common.Host)
	} else {
		hostCommandsExecutor = ops.NewRegularExecutor(pkgLog, true)
	}

	opsInterface := ops.NewOps(pkgLog, hostCommandsExecutor)

	k8sConfig, err := clientcmd.BuildConfigFromFlags("", common.PathOutsideChroot(common.KubeconfigFile))
	if err != nil {
		return fmt.Errorf("failed to create k8s config: %w", err)
	}

	client, err := runtimeClient.New(k8sConfig, runtimeClient.Options{Scheme: ipConfigScheme})
	if err != nil {
		return fmt.Errorf("failed to create runtime client: %w", err)
	}

	var pullSecretFile = common.ImageRegistryAuthFile
	if pullSecretRefName != "" {
		authPath, err := materializeAuthFileFromPullSecretRef(context.Background(), client, pullSecretRefName)
		if err != nil {
			return err
		}
		defer os.Remove(common.PathOutsideChroot(authPath))
		pullSecretFile = authPath
	}

	ipConfigHandler := ipconfig.NewIPConfig(
		pkgLog,
		opsInterface,
		hostCommandsExecutor,
		client,
		recertImage,
		common.LCAWorkspaceDir,
		ipConfigs,
		&ipconfig.ProxyConfig{HTTPProxy: httpProxy, HTTPSProxy: httpsProxy, NoProxy: noProxy},
		pullSecretFile,
	)

	rpmClient := rpmOstree.NewClient("lca-cli-ip-config-run", hostCommandsExecutor)
	ostreeClient := intOstree.NewClient(hostCommandsExecutor, false)
	rbClient := reboot.NewIPCRebootClient(&logr.Logger{}, hostCommandsExecutor, rpmClient, ostreeClient, opsInterface)

	if err = ipConfigHandler.RunIPConfigChange(); err != nil {
		internalErr := common.FinalizeIPConfigStatus(
			common.IPConfigRunStatusFile,
			common.IPConfigPhaseFailed,
			fmt.Sprintf("ip-config run failed: %v", err),
		)
		if internalErr != nil {
			return fmt.Errorf("failed to finalize IP config run status: %w", internalErr)
		}
	}

	if err := common.FinalizeIPConfigStatus(
		common.IPConfigRunStatusFile,
		common.IPConfigPhaseSucceeded,
		"ip-config run completed successfully",
	); err != nil {
		return fmt.Errorf("failed to mark IP config run as successful: %w", err)
	}

	if err := rbClient.Reboot("ip-config run"); err != nil {
		return fmt.Errorf("failed to reboot: %w", err)
	}

	return nil
}

// materializeAuthFileFromPullSecretRef fetches the dockerconfigjson secret by name in the LCA namespace
// and writes it to an auth file under the LCA workspace, returning the path to the file.
func materializeAuthFileFromPullSecretRef(
	ctx context.Context,
	c runtimeClient.Client,
	secretName string,
) (string, error) {
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: common.LcaNamespace,
		Name:      secretName,
	}, secret); err != nil {
		return "", fmt.Errorf("failed to fetch pull secret %s/%s: %w", common.LcaNamespace, secretName, err)
	}
	dockercfg, ok := secret.Data[corev1.DockerConfigJsonKey]
	if !ok || len(dockercfg) == 0 {
		return "", fmt.Errorf("secret %s/%s missing key %s", common.LcaNamespace, secretName, corev1.DockerConfigJsonKey)
	}
	authPath := path.Join(common.LCAWorkspaceDir, "recert-pull-secret.json")
	if err := os.WriteFile(common.PathOutsideChroot(authPath), dockercfg, 0o600); err != nil {
		return "", fmt.Errorf("failed to write pull secret auth file: %w", err)
	}
	return authPath, nil
}

// validateIPConfigArgs validates the CLI arguments for IP configuration.
func validateIPConfigArgs(ipv4Addr, ipv4Net, ipv6Addr, ipv6Net string) error {
	ipv4Both := ipv4Addr != "" && ipv4Net != ""
	ipv4None := ipv4Addr == "" && ipv4Net == ""
	ipv6Both := ipv6Addr != "" && ipv6Net != ""
	ipv6None := ipv6Addr == "" && ipv6Net == ""

	if (!ipv4Both && !ipv4None) || (!ipv6Both && !ipv6None) {
		return fmt.Errorf("both address and machine-network must be provided together for each IP family")
	}

	if ipv4None && ipv6None {
		return fmt.Errorf("at least one of IPv4 or IPv6 must be provided")
	}

	if ipv4Both {
		ip := net.ParseIP(ipv4Addr)
		if ip == nil || ip.To4() == nil {
			return fmt.Errorf("invalid IPv4 address: %s", ipv4Addr)
		}
		_, ipNet, err := net.ParseCIDR(ipv4Net)
		if err != nil {
			return fmt.Errorf("invalid IPv4 machine network CIDR: %s", ipv4Net)
		}
		if ipNet.IP.To4() == nil {
			return fmt.Errorf("ipv4-machine-network must be an IPv4 CIDR: %s", ipv4Net)
		}
		if !ipNet.Contains(ip) {
			return fmt.Errorf("IPv4 address %s is not within machine network %s", ipv4Addr, ipv4Net)
		}
	}

	if ipv6Both {
		ip := net.ParseIP(ipv6Addr)
		if ip == nil || ip.To4() != nil {
			return fmt.Errorf("invalid IPv6 address: %s", ipv6Addr)
		}
		_, ipNet, err := net.ParseCIDR(ipv6Net)
		if err != nil {
			return fmt.Errorf("invalid IPv6 machine network CIDR: %s", ipv6Net)
		}
		if ipNet.IP.To4() != nil {
			return fmt.Errorf("ipv6-machine-network must be an IPv6 CIDR: %s", ipv6Net)
		}
		if !ipNet.Contains(ip) {
			return fmt.Errorf("IPv6 address %s is not within machine network %s", ipv6Addr, ipv6Net)
		}
	}

	return nil
}

// inferPrimaryStack determines the effective primary stack based on provided inputs.
// Rules:
// - IPv4-only => ipv4
// - IPv6-only => ipv6
// - Dual-stack => default ipv4
func inferPrimaryStack(ipv4Addr, ipv4Net, ipv6Addr, ipv6Net string) (string, error) {
	ipv4Both := ipv4Addr != "" && ipv4Net != ""
	ipv6Both := ipv6Addr != "" && ipv6Net != ""

	switch {
	case ipv4Both && !ipv6Both:
		return ipFamilyIPv4, nil
	case ipv6Both && !ipv4Both:
		return ipFamilyIPv6, nil
	case ipv4Both && ipv6Both:
		return ipFamilyIPv4, nil
	default:
		return "", fmt.Errorf("at least one of IPv4 or IPv6 must be provided")
	}
}

// buildIPConfigs creates the ordered slice of NetworkIPConfig with primary first.
func buildIPConfigs(
	ipv4Addr, ipv4Net, ipv4Gw, ipv4DNS string,
	ipv6Addr, ipv6Net, ipv6Gw, ipv6DNS string,
	primary string,
) []*ipconfig.NetworkIPConfig {
	var ipv4Config *ipconfig.NetworkIPConfig
	if ipv4Addr != "" && ipv4Net != "" {
		ipv4Config = &ipconfig.NetworkIPConfig{IP: ipv4Addr, MachineNetwork: ipv4Net, Gateway: ipv4Gw, DNSServer: ipv4DNS}
	}

	var ipv6Config *ipconfig.NetworkIPConfig
	if ipv6Addr != "" && ipv6Net != "" {
		ipv6Config = &ipconfig.NetworkIPConfig{IP: ipv6Addr, MachineNetwork: ipv6Net, Gateway: ipv6Gw, DNSServer: ipv6DNS}
	}

	ipConfigs := []*ipconfig.NetworkIPConfig{}
	if primary == ipFamilyIPv4 && ipv4Config != nil {
		ipConfigs = append(ipConfigs, ipv4Config)
		if ipv6Config != nil {
			ipConfigs = append(ipConfigs, ipv6Config)
		}
	} else if primary == ipFamilyIPv6 && ipv6Config != nil {
		ipConfigs = append(ipConfigs, ipv6Config)
		if ipv4Config != nil {
			ipConfigs = append(ipConfigs, ipv4Config)
		}
	}
	return ipConfigs
}
