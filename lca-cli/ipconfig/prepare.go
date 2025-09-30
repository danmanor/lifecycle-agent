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
	p.log.Infof("IP config prepare started with IPv4: %s and IPv6: %s", newIPv4, newIPv6)

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

	p.log.Info("IP config prepare done successfully")

	return nil
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
	if existing, _ := p.ostree.GetDeployment(newStateroot); existing != "" {
		if strings.HasPrefix(existing, bootedCommit) {
			p.log.Infof("stateroot %s already deployed with commit %s, skipping deploy", newStateroot, bootedCommit)
			return nil
		}
	}

	if err := p.ostree.OSInit(newStateroot); err != nil {
		p.log.Warnf("os-init for %s returned error, assuming already initialized: %v", newStateroot, err)
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

	_ = os.MkdirAll(newCommitDir, 0o755)
	_ = os.MkdirAll(newCommitDir, 0o755)

	if _, err := p.ops.RunInHostNamespace(
		"bash", "-c",
		fmt.Sprintf(
			"cp -ar --preserve=context '%s/' '%s/'",
			oldVar,
			newVarParent,
		),
	); err != nil {
		return fmt.Errorf("failed to copy var: %w", err)
	}

	if _, err := p.ops.RunInHostNamespace(
		"bash", "-c",
		fmt.Sprintf(
			"cp -ar --preserve=context '%s/' '%s/'",
			filepath.Join(oldCommitDir, "etc"),
			newCommitDir,
		),
	); err != nil {
		return fmt.Errorf("failed to copy etc: %w", err)
	}

	if _, err := p.ops.RunInHostNamespace(
		"bash", "-c",
		fmt.Sprintf(
			"cp -a --preserve=context '%s' '%s'",
			fmt.Sprintf("%s.%s", filepath.Join(oldSRPath, "deploy", fmt.Sprintf("%s.0", bootedCommit)), "origin"),
			fmt.Sprintf("%s.%s", filepath.Join(newSRPath, "deploy", fmt.Sprintf("%s.0", bootedCommit)), "origin"),
		),
	); err != nil {
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

	if data, err = os.ReadFile(common.PathOutsideChroot(common.MCDCurrentConfig)); err != nil || data == nil {
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
