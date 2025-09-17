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

package cmd

import (
	"fmt"
	"net"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	runtimeClient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ipconfig"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	machineconfigv1 "github.com/openshift/api/machineconfiguration/v1"
)

var (
	ipConfigScheme = runtime.NewScheme()

	// IP configuration parameters
	ipv4Address        string
	ipv4MachineNetwork string
	ipv6Address        string
	ipv6MachineNetwork string
	httpProxy          string
	httpsProxy         string
	noProxy            string
	pullSecretFile     string
)

const (
	ipFamilyIPv4 = "ipv4"
	ipFamilyIPv6 = "ipv6"
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(ipConfigScheme))
	utilruntime.Must(machineconfigv1.AddToScheme(ipConfigScheme))
}

// ipConfigCmd represents the ip-config command
var ipConfigCmd = &cobra.Command{
	Use:   "ip-config",
	Short: "Perform IP configuration",
	Long:  "Perform IP configuration",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runIPConfigChange(); err != nil {
			log.Fatalf("Error executing ip-config command: %v", err)
		}
	},
}

func init() {
	// Add ip-config command
	rootCmd.AddCommand(ipConfigCmd)

	// IP configuration flags
	ipConfigCmd.Flags().StringVar(&ipv4Address, "ipv4-address", "", "Target IPv4 address")
	ipConfigCmd.Flags().StringVar(&ipv4MachineNetwork, "ipv4-machine-network", "", "Target IPv4 machine network CIDR")
	ipConfigCmd.Flags().StringVar(&ipv6Address, "ipv6-address", "", "Target IPv6 address")
	ipConfigCmd.Flags().StringVar(&ipv6MachineNetwork, "ipv6-machine-network", "", "Target IPv6 machine network CIDR")
	ipConfigCmd.Flags().StringVar(&httpProxy, "http-proxy", "", "HTTP proxy to use for network operations")
	ipConfigCmd.Flags().StringVar(&httpsProxy, "https-proxy", "", "HTTPS proxy to use for network operations")
	ipConfigCmd.Flags().StringVar(&noProxy, "no-proxy", "", "Comma-separated list of hosts that should bypass the proxy")
	ipConfigCmd.Flags().StringVar(&pullSecretFile, "pull-secret-file", "", "Path to pull secret auth file to use for image pulls")
}

func runIPConfigChange() error {
	if err := validateIPConfigArgs(ipv4Address, ipv4MachineNetwork, ipv6Address, ipv6MachineNetwork); err != nil {
		return err
	}

	effectivePrimary, err := inferPrimaryStack(ipv4Address, ipv4MachineNetwork, ipv6Address, ipv6MachineNetwork)
	if err != nil {
		return err
	}

	ipConfigs := buildIPConfigs(ipv4Address, ipv4MachineNetwork, ipv6Address, ipv6MachineNetwork, effectivePrimary)

	var hostCommandsExecutor ops.Execute
	if _, err := os.Stat(common.Host); err == nil {
		hostCommandsExecutor = ops.NewChrootExecutor(log, true, common.Host)
	} else {
		hostCommandsExecutor = ops.NewRegularExecutor(log, true)
	}

	opsInterface := ops.NewOps(log, hostCommandsExecutor)

	k8sConfig, err := clientcmd.BuildConfigFromFlags("", common.KubeconfigFile)
	if err != nil {
		return fmt.Errorf("failed to create k8s config: %w", err)
	}

	client, err := runtimeClient.New(k8sConfig, runtimeClient.Options{Scheme: ipConfigScheme})
	if err != nil {
		return fmt.Errorf("failed to create runtime client: %w", err)
	}

	ipChangeRunner := ipconfig.NewIPConfig(
		log,
		opsInterface,
		hostCommandsExecutor,
		client,
		recertContainerImage,
		common.OptOpenshift,
		ipConfigs,
		&ipconfig.ProxyConfig{HTTPProxy: httpProxy, HTTPSProxy: httpsProxy, NoProxy: noProxy},
		pullSecretFile,
	)

	if err = ipChangeRunner.RunIPConfigChange(); err != nil {
		return fmt.Errorf("failed to run IP config process: %w", err)
	}

	log.Info("IP Config process finished successfully")
	return nil
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
// - Dual-stack => default ipv4 unless user provided a valid override
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
func buildIPConfigs(ipv4Addr, ipv4Net, ipv6Addr, ipv6Net, primary string) []*ipconfig.NetworkIPConfig {
	var ipv4Config *ipconfig.NetworkIPConfig
	if ipv4Addr != "" && ipv4Net != "" {
		ipv4Config = &ipconfig.NetworkIPConfig{IP: ipv4Addr, MachineNetwork: ipv4Net}
	}

	var ipv6Config *ipconfig.NetworkIPConfig
	if ipv6Addr != "" && ipv6Net != "" {
		ipv6Config = &ipconfig.NetworkIPConfig{IP: ipv6Addr, MachineNetwork: ipv6Net}
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
