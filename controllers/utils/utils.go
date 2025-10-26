package utils

import (
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	cp "github.com/otiai10/copy"
)

// CopyLcaCliToHost copies the lca-cli binary from the container into /var/usrlocal/bin
func CopyLcaCliToHost(logger logr.Logger) error {
	src := LcaCliBinaryContainerPath
	dst := common.PathOutsideChroot(LcaCliBinaryHostPath)
	logger.Info("Copying lca-cli binary", "src", src, "dst", dst)
	if err := cp.Copy(src, dst, cp.Options{AddPermission: os.FileMode(0o777)}); err != nil {
		return fmt.Errorf("failed to copy lca-cli binary: %w", err)
	}
	return nil
}

// CommandExecutor is a minimal interface satisfied by types that can Execute commands.
type CommandExecutor interface {
	Execute(command string, args ...string) (string, error)
}
