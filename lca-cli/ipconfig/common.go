package ipconfig

import (
	"strings"

	"github.com/openshift-kni/lifecycle-agent/internal/common"
)

func BuildStaterootName(newIPv4, newIPv6 string) (string, error) {
	nameParts := []string{"rhcos"}
	if newIPv4 != "" {
		nameParts = append(nameParts, common.SanitizeForOsname(newIPv4))
	}
	if newIPv6 != "" {
		nameParts = append(nameParts, common.SanitizeForOsname(newIPv6))
	}
	return strings.Join(nameParts, "_"), nil
}
