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
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
	mcfgv1 "github.com/openshift/api/machineconfiguration/v1"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	runtimeClient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/openshift-kni/lifecycle-agent/internal/common"
	intOstree "github.com/openshift-kni/lifecycle-agent/internal/ostreeclient"
	"github.com/openshift-kni/lifecycle-agent/internal/reboot"
	lcacli "github.com/openshift-kni/lifecycle-agent/lca-cli"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ipconfig"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	rpmOstree "github.com/openshift-kni/lifecycle-agent/lca-cli/ostreeclient"
	"github.com/spf13/cobra"
)

var (
	ipPrepareScheme = runtime.NewScheme()

	newIPv4            string
	newIPv6            string
	installInitMonitor bool
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(ipPrepareScheme))
	utilruntime.Must(mcfgv1.AddToScheme(ipPrepareScheme))

	ipConfigPrepareCmd.Flags().StringVar(&newIPv4, "ipv4-address", "", "New IPv4 address")
	ipConfigPrepareCmd.Flags().StringVar(&newIPv6, "ipv6-address", "", "New IPv6 address")
	ipConfigPrepareCmd.Flags().BoolVar(
		&installInitMonitor,
		"install-init-monitor",
		false,
		"Install init monitor service in the new stateroot",
	)
}

var ipConfigPrepareCmd = &cobra.Command{
	Use:   "prepare",
	Short: "Prepare a new stateroot for IP configuration change",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runIPConfigPrepare(); err != nil {
			pkgLog.Fatalf("Error executing ip-config prepare: %v", err)
		}
	},
}

func runIPConfigPrepare() error {
	if newIPv4 == "" && newIPv6 == "" {
		return fmt.Errorf("at least one of --ipv4-address or --ipv6-address must be provided")
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
	client, err := runtimeClient.New(k8sConfig, runtimeClient.Options{Scheme: ipPrepareScheme})
	if err != nil {
		return fmt.Errorf("failed to create runtime client: %w", err)
	}

	rpmClient := rpmOstree.NewClient("lca-cli-ip-config-prepare", hostCommandsExecutor)
	ostreeClient := intOstree.NewClient(hostCommandsExecutor, false)
	rbClient := reboot.NewIPCRebootClient(&logr.Logger{}, hostCommandsExecutor, rpmClient, ostreeClient, opsInterface)

	preparer := ipconfig.NewPrepareHandler(pkgLog, opsInterface, ostreeClient, rpmClient, rbClient, client)
	if err := common.WriteIPConfigStatus(common.IPConfigPrepareStatusFile, common.IPConfigRunStatus{
		Phase:     common.IPConfigPhaseRunning,
		Message:   "ip-config prepare started",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		return fmt.Errorf("failed to write initial prepare status: %w", err)
	}

	ctx := context.Background()
	if err := preparer.RunPrepare(ctx, newIPv4, newIPv6); err != nil {
		internalErr := common.FinalizeIPConfigStatus(
			common.IPConfigPrepareStatusFile,
			common.IPConfigPhaseFailed,
			fmt.Sprintf("ip-config prepare failed: %v", err),
		)
		if internalErr != nil {
			return fmt.Errorf("failed to finalize IP config prepare status: %w", internalErr)
		}
		return err
	}

	newStaterootName := common.BuildNewStaterootNameFromIps(newIPv4, newIPv6)
	if installInitMonitor {
		err := installMonitorInitializationServiceInNewStateroot(ostreeClient, opsInterface, newStaterootName, pkgLog)
		if err != nil {
			return fmt.Errorf("failed to install monitor initialization service: %w", err)
		}
	}

	staterootPath := common.GetStaterootPath(newStaterootName)
	statusFilePath := filepath.Join(staterootPath, common.IPConfigPrepareStatusFile)
	if err := common.FinalizeIPConfigStatus(
		statusFilePath,
		common.IPConfigPhaseSucceeded,
		"ip-config prepare completed successfully",
	); err != nil {
		return fmt.Errorf("failed to mark prepare as successful: %w", err)
	}

	if err := common.FinalizeIPConfigStatus(
		common.IPConfigPrepareStatusFile,
		common.IPConfigPhaseSucceeded,
		"ip-config prepare completed successfully",
	); err != nil {
		return fmt.Errorf("failed to mark prepare as successful: %w", err)
	}

	if err := rbClient.RebootToNewStateRoot("ip-config prepare"); err != nil {
		return fmt.Errorf("failed to reboot to new stateroot: %w", err)
	}

	return nil
}

// installMonitorInitializationServiceInNewStateroot installs and enables the IPC init monitor service
// within the new stateroot deployment. The auto-rollback configuration file should be written
// by the caller (controller) beforehand.
func installMonitorInitializationServiceInNewStateroot(
	ostree intOstree.IClient,
	ops ops.Ops,
	newStaterootName string,
	logger *logrus.Logger,
) error {
	common.OstreeDeployPathPrefix = "/sysroot"
	deploymentDir, err := ostree.GetDeploymentDir(newStaterootName)
	if err != nil {
		return fmt.Errorf("failed to get deployment dir for %s: %w", newStaterootName, err)
	}

	initMonitorServiceFile, err := fs.ReadFile(lcacli.InstallationConfigurationServices, common.IPCInitMonitorService)
	if err != nil {
		return fmt.Errorf("failed to read init monitor service file: %w", err)
	}

	destinationFilePath := filepath.Join(deploymentDir, "etc/systemd/system", common.IPCInitMonitorService)
	logger.Infof("Creating service %s", common.IPCInitMonitorService)

	if err := os.MkdirAll(path.Dir(destinationFilePath), 0o755); err != nil {
		return fmt.Errorf("failed to create destination directory for %s: %w", common.IPCInitMonitorService, err)
	}
	if err := os.WriteFile(destinationFilePath, initMonitorServiceFile, 0o644); err != nil {
		return fmt.Errorf("failed to write init monitor service file: %w", err)
	}

	logger.Infof("Enabling service %s", common.IPCInitMonitorService)
	if _, err := ops.SystemctlAction(
		"enable",
		"--root",
		deploymentDir,
		common.IPCInitMonitorService,
	); err != nil {
		return fmt.Errorf("failed enabling service %s: %w", common.IPCInitMonitorService, err)
	}
	return nil
}
