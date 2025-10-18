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
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	intOstree "github.com/openshift-kni/lifecycle-agent/internal/ostreeclient"
	"github.com/openshift-kni/lifecycle-agent/internal/reboot"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ipconfig"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	rpmOstree "github.com/openshift-kni/lifecycle-agent/lca-cli/ostreeclient"
	"github.com/spf13/cobra"
)

var (
	rollbackStateroot string
)

func init() {
	// subcommand is added by NewIPConfigCmd after globals are initialized
	ipConfigRollbackCmd.Flags().StringVar(&rollbackStateroot, "stateroot", "", "Target stateroot to roll back to (e.g., rhcos_10-0-0-5)")
}

var ipConfigRollbackCmd = &cobra.Command{
	Use:   "rollback",
	Short: "Rollback to the state before the IP configuration change",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runIPConfigRollback(); err != nil {
			pkgLog.Fatalf("Error executing ip-config rollback: %v", err)
		}
	},
}

func runIPConfigRollback() error {
	if rollbackStateroot == "" {
		return fmt.Errorf("--stateroot is required")
	}

	// Use existing clients
	var hostCommandsExecutor ops.Execute
	if _, err := os.Stat(common.Host); err == nil {
		hostCommandsExecutor = ops.NewChrootExecutor(pkgLog, true, common.Host)
	} else {
		hostCommandsExecutor = ops.NewRegularExecutor(pkgLog, true)
	}
	opsInterface := ops.NewOps(pkgLog, hostCommandsExecutor)
	rpmClient := rpmOstree.NewClient("lca-cli-ip-config-rollback", hostCommandsExecutor)
	ostreeClient := intOstree.NewClient(hostCommandsExecutor, false)

	rb := reboot.NewRebootClient(&logr.Logger{}, hostCommandsExecutor, rpmClient, ostreeClient, opsInterface)

	// Write initial rollback status
	if err := common.WriteIPConfigStatus(common.IPConfigRollbackStatusFile, common.IPConfigRunStatus{
		Phase:     common.IPConfigRunPhaseRunning,
		Message:   "ip-config rollback started",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		return fmt.Errorf("failed to write initial rollback status: %w", err)
	}

	exec := ipconfig.NewRollbackHandler(pkgLog, opsInterface, ostreeClient, rpmClient, rb)
	if err := exec.RunRollback(rollbackStateroot); err != nil {
		internalErr := common.FinalizeIPConfigStatus(common.IPConfigRollbackStatusFile, common.IPConfigRunPhaseFailed, fmt.Sprintf("ip-config rollback failed: %v", err))
		if internalErr != nil {
			return fmt.Errorf("failed to finalize IP config rollback status: %w", internalErr)
		}
		return err
	}

	if err := common.FinalizeIPConfigStatus(
		common.IPConfigRollbackStatusFile,
		common.IPConfigRunPhaseSucceeded,
		"ip-config rollback completed successfully",
	); err != nil {
		return fmt.Errorf("failed to mark rollback as successful: %w", err)
	}

	// Schedule reboot on the host namespace
	hostExec := ops.NewNsenterExecutor(pkgLog, true)
	if _, err := hostExec.Execute(
		"systemd-run",
		"--unit", "lca-ipconfig-rollback-reboot",
		"--description", "lifecycle-agent: ip-config rollback reboot",
		"systemctl", "reboot",
	); err != nil {
		return fmt.Errorf("failed to schedule reboot: %w", err)
	}

	return nil
}
