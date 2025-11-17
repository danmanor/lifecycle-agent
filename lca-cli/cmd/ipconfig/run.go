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
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
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
	"github.com/openshift-kni/lifecycle-agent/utils"
	ocp_config_v1 "github.com/openshift/api/config/v1"
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
	vlanID             int
	pullSecretRefName  string
	recertImage        string
	dnsIPFamily        string
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(ipConfigScheme))
	utilruntime.Must(machineconfigv1.AddToScheme(ipConfigScheme))
	utilruntime.Must(ocp_config_v1.AddToScheme(ipConfigScheme))

	ipConfigRunCmd.Flags().StringVar(&ipv4Address, "ipv4-address", "", "Target IPv4 address")
	ipConfigRunCmd.Flags().StringVar(&ipv4MachineNetwork, "ipv4-machine-network", "", "Target IPv4 machine network CIDR")
	ipConfigRunCmd.Flags().StringVar(&ipv6Address, "ipv6-address", "", "Target IPv6 address")
	ipConfigRunCmd.Flags().StringVar(&ipv6MachineNetwork, "ipv6-machine-network", "", "Target IPv6 machine network CIDR")
	ipConfigRunCmd.Flags().StringVar(&ipv4Gateway, "ipv4-gateway", "", "IPv4 default gateway")
	ipConfigRunCmd.Flags().StringVar(&ipv6Gateway, "ipv6-gateway", "", "IPv6 default gateway")
	ipConfigRunCmd.Flags().StringVar(&ipv4DNS, "ipv4-dns", "", "IPv4 DNS server")
	ipConfigRunCmd.Flags().StringVar(&ipv6DNS, "ipv6-dns", "", "IPv6 DNS server")
	ipConfigRunCmd.Flags().IntVar(&vlanID, "vlan-id", 0, "Optional VLAN ID to use on the br-ex uplink")
	ipConfigRunCmd.Flags().StringVar(&recertImage, "recert-image", "", "The full image name for the recert container tool")
	ipConfigRunCmd.Flags().StringVar(&pullSecretRefName, "pull-secret-ref-name", "", "The name of the pull secret to use for the recert container tool")
	ipConfigRunCmd.Flags().StringVar(&dnsIPFamily, "dns-ip-family", "", "IP family for DNS resolution (ipv4|ipv6)")
}

var ipConfigRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Execute IP configuration change and reboot to the new configuration",
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
			vlanID = cfg.VLANID
			pullSecretRefName = cfg.PullSecretRefName
			recertImage = cfg.RecertImage
			dnsIPFamily = cfg.DNSIPFamily
		} else {
			pkgLog.Warnf("failed to unmarshal ip-config run config: %v", jsonErr)
		}
	} else {
		pkgLog.Info("using command line flags")
	}

	if err := validateIPConfigArgs(ipv4Address, ipv4MachineNetwork, ipv6Address, ipv6MachineNetwork); err != nil {
		return err
	}

	effectivePrimary, err := inferPrimaryStack()
	if err != nil {
		return err
	}

	ipConfigs := buildIPConfigs(
		ipv4Address, ipv4MachineNetwork, ipv4Gateway, ipv4DNS,
		ipv6Address, ipv6MachineNetwork, ipv6Gateway, ipv6DNS,
		lo.FromPtr(effectivePrimary),
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

	ipConfigHandler := ipconfig.NewIPConfig(
		pkgLog,
		opsInterface,
		hostCommandsExecutor,
		client,
		recertImage,
		ipConfigs,
		pullSecretRefName,
		vlanID,
		dnsIPFamily,
	)

	rpmClient := rpmOstree.NewClient("lca-cli-ip-config-run", hostCommandsExecutor)
	ostreeClient := intOstree.NewClient(hostCommandsExecutor, false)
	rbClient := reboot.NewIPCRebootClient(&logr.Logger{}, hostCommandsExecutor, rpmClient, ostreeClient, opsInterface)

	if err := ipConfigHandler.Run(); err != nil {
		internalErr := common.FinalizeIPConfigStatus(
			common.IPConfigRunStatusFile,
			common.IPConfigPhaseFailed,
			fmt.Sprintf("ip-config run failed: %v", err),
		)
		if internalErr != nil {
			return fmt.Errorf("failed to finalize IP config run status: %w", internalErr)
		}

		return fmt.Errorf("failed to run IP config: %w", err)
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

func inferPrimaryStack() (*string, error) {
	data, err := os.ReadFile(utils.PrimaryIPPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read primary IP: %w", err)
	}

	primaryIP := strings.TrimSpace(string(data))
	if primaryIP == "" {
		return nil, fmt.Errorf("primary IP not found")
	}

	ip := net.ParseIP(primaryIP)
	if ip == nil {
		return nil, fmt.Errorf("invalid primary IP: %s", primaryIP)
	}

	if ip.To4() != nil {
		return lo.ToPtr(common.IPv4FamilyName), nil
	}

	if ip.To16() != nil {
		return lo.ToPtr(common.IPv6FamilyName), nil
	}

	return nil, fmt.Errorf("invalid primary IP: %s", primaryIP)
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
	if primary == common.IPv4FamilyName && ipv4Config != nil {
		ipConfigs = append(ipConfigs, ipv4Config)
		if ipv6Config != nil {
			ipConfigs = append(ipConfigs, ipv6Config)
		}
	} else if primary == common.IPv6FamilyName && ipv6Config != nil {
		ipConfigs = append(ipConfigs, ipv6Config)
		if ipv4Config != nil {
			ipConfigs = append(ipConfigs, ipv4Config)
		}
	}

	return ipConfigs
}
