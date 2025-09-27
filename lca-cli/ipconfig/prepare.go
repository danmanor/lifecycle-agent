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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	mcfgv1 "github.com/openshift/api/machineconfiguration/v1"
	"github.com/sirupsen/logrus"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/openshift-kni/lifecycle-agent/internal/common"
	intOstree "github.com/openshift-kni/lifecycle-agent/internal/ostreeclient"
	"github.com/openshift-kni/lifecycle-agent/internal/reboot"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	rpmOstree "github.com/openshift-kni/lifecycle-agent/lca-cli/ostreeclient"
)

type PrepareHandler struct {
	log    *logrus.Logger
	ops    ops.Ops
	ostree intOstree.IClient
	rpm    rpmOstree.IClient
	reboot reboot.RebootIntf
	k8s    runtimeclient.Client
}

func NewPrepareHandler(log *logrus.Logger, ops ops.Ops, ostree intOstree.IClient, rpm rpmOstree.IClient, reboot reboot.RebootIntf, k8s runtimeclient.Client) *PrepareHandler {
	return &PrepareHandler{log: log, ops: ops, ostree: ostree, rpm: rpm, reboot: reboot, k8s: k8s}
}

func (p *PrepareHandler) RunPrepare(ctx context.Context, newIPv4, newIPv6 string) error {
	newStateroot, err := p.buildStaterootName(newIPv4, newIPv6)
	if err != nil {
		return err
	}

	if err := p.ensureSysrootWritable(); err != nil {
		return err
	}

	kargs, err := fetchCurrentKernelArgs()
	if err != nil {
		return fmt.Errorf("failed to get current kernel args: %w", err)
	}

	currentStateroot, bootedCommit, err := p.getCurrentStaterootAndBootedCommit()
	if err != nil {
		return err
	}

	if err := p.deployNewStateroot(newStateroot, bootedCommit, kargs); err != nil {
		return err
	}

	if err := p.copyStateRootData(currentStateroot, newStateroot, bootedCommit); err != nil {
		return err
	}

	if err := p.setDefaultDeploymentIfEnabled(newStateroot); err != nil {
		return err
	}

	return p.reboot.RebootToNewStateRoot("ip-config prepare")
}

func (p *PrepareHandler) buildStaterootName(newIPv4, newIPv6 string) (string, error) {
	nameParts := []string{"rhcos"}
	if newIPv4 != "" {
		nameParts = append(nameParts, sanitizeForOsname(newIPv4))
	}
	if newIPv6 != "" {
		nameParts = append(nameParts, sanitizeForOsname(newIPv6))
	}
	return strings.Join(nameParts, "_"), nil
}

func (p *PrepareHandler) ensureSysrootWritable() error {
	common.OstreeDeployPathPrefix = "/sysroot"

	if err := p.ops.RemountSysroot(); err != nil {
		return fmt.Errorf("failed to remount /sysroot rw: %w", err)
	}
	return nil
}

func (p *PrepareHandler) getCurrentStaterootAndBootedCommit() (string, string, error) {
	currentStateroot, err := p.rpm.GetCurrentStaterootName()
	if err != nil {
		return "", "", fmt.Errorf("failed to get current stateroot: %w", err)
	}

	status, err := p.rpm.QueryStatus()
	if err != nil {
		return "", "", fmt.Errorf("failed to query rpm-ostree status: %w", err)
	}

	var bootedCommit string
	for _, d := range status.Deployments {
		if d.Booted {
			bootedCommit = d.Checksum
			break
		}
	}
	if bootedCommit == "" {
		return "", "", fmt.Errorf("failed to determine booted deployment commit")
	}

	return currentStateroot, bootedCommit, nil
}

func (p *PrepareHandler) deployNewStateroot(newStateroot, bootedCommit string, kargs []string) error {
	if err := p.ostree.OSInit(newStateroot); err != nil {
		return fmt.Errorf("failed ostree admin os-init: %w", err)
	}
	if err := p.ostree.Deploy(newStateroot, bootedCommit, kargs, p.rpm, false); err != nil {
		return fmt.Errorf("failed ostree admin deploy: %w", err)
	}
	return nil
}

func (p *PrepareHandler) copyStateRootData(currentStateroot, newStateroot, bootedCommit string) error {
	oldSRPath := common.GetStaterootPath(currentStateroot)
	newSRPath := common.GetStaterootPath(newStateroot)
	oldVar := filepath.Join(oldSRPath, "var")
	newVarParent := newSRPath
	oldCommitDir := filepath.Join(oldSRPath, "deploy", fmt.Sprintf("%s.0", bootedCommit))
	newCommitDir := filepath.Join(newSRPath, "deploy", fmt.Sprintf("%s.0", bootedCommit))

	if _, err := p.ops.RunInHostNamespace("cp", "-r", "-a", "--preserve=context", filepath.Join("/", oldVar)+"/", filepath.Join("/", newVarParent)+"/"); err != nil {
		return fmt.Errorf("failed to copy var: %w", err)
	}
	if _, err := p.ops.RunInHostNamespace("cp", "-r", "-a", "--preserve=context", filepath.Join("/", oldCommitDir, "etc")+"/", filepath.Join("/", newCommitDir)+"/"); err != nil {
		return fmt.Errorf("failed to copy etc: %w", err)
	}
	if _, err := p.ops.RunInHostNamespace("cp", "-a", "--preserve=context", filepath.Join("/", oldCommitDir)+".origin", filepath.Join("/", newCommitDir)+".origin"); err != nil {
		return fmt.Errorf("failed to copy origin file: %w", err)
	}
	return nil
}

func (p *PrepareHandler) setDefaultDeploymentIfEnabled(newStateroot string) error {
	if !p.ostree.IsOstreeAdminSetDefaultFeatureEnabled() {
		return nil
	}

	idx, err := p.rpm.GetDeploymentIndex(newStateroot)
	if err != nil {
		return fmt.Errorf("failed to get deployment index for %s: %w", newStateroot, err)
	}
	if err := p.ostree.SetDefaultDeployment(idx); err != nil {
		return fmt.Errorf("failed to set default deployment: %w", err)
	}
	return nil
}

func fetchCurrentKernelArgs() ([]string, error) {
	var (
		data []byte
		err  error
	)

	if data, err = os.ReadFile(common.MCDCurrentConfig); err != nil || data == nil {
		return nil, fmt.Errorf("failed to read MCD currentconfig: %w", err)
	}

	currentConfig := strings.TrimSpace(string(data))
	mc := &mcfgv1.MachineConfig{}
	if err := json.Unmarshal(data, mc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal MachineConfig %s: %w", currentConfig, err)
	}

	return buildKernelArgsFromMachineConfig(mc)
}

func buildKernelArgsFromMachineConfig(mc *mcfgv1.MachineConfig) ([]string, error) {
	args := make([]string, len(mc.Spec.KernelArguments)*2)
	for i, karg := range mc.Spec.KernelArguments {
		val, err := json.Marshal(karg)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal karg %s: %w", karg, err)
		}
		args[2*i] = "--karg-append"
		args[2*i+1] = string(val)
	}
	if mc.Spec.FIPS {
		args = append(args,
			"--karg-append", "fips=1",
			"--karg-append", "boot=LABEL=boot",
		)
	}
	return args, nil
}

func sanitizeForOsname(s string) string {
	s = strings.Trim(s, "[]")
	s = strings.Split(s, "/")[0]
	re := regexp.MustCompile(`[^A-Za-z0-9]+`)
	return re.ReplaceAllString(s, "-")
}
