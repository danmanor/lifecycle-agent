package common

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// FileOpsReader defines minimal file operations needed to read status
type FileOpsReader interface {
	ReadFile(string) ([]byte, error)
	IsNotExist(error) bool
}

// WriteIPConfigStatus writes the given status struct to the provided file path.
func WriteIPConfigStatus(filePath string, st IPConfigRunStatus) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	data, err := json.Marshal(st)
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0o600)
}

// FinalizeIPConfigStatus sets final phase, message and finishedAt in the given file.
// If a previous status exists, StartedAt is preserved.
func FinalizeIPConfigStatus(filePath string, phase IPConfigRunStatusPhase, msg string) error {
	st := IPConfigRunStatus{Phase: phase, Message: msg, FinishedAt: time.Now().UTC().Format(time.RFC3339)}
	if data, err := os.ReadFile(filePath); err == nil && len(data) > 0 {
		var prev IPConfigRunStatus
		if jsonErr := json.Unmarshal(data, &prev); jsonErr == nil {
			st.StartedAt = prev.StartedAt
		}
	}
	return WriteIPConfigStatus(filePath, st)
}

// ReadIPConfigStatus reads and parses the status file, returning phase and message.
// Returns Unknown when the file is not found.
func ReadIPConfigStatus(filePath string, fops FileOpsReader) (IPConfigRunStatusPhase, string, error) {
	data, err := fops.ReadFile(filePath)
	if err != nil {
		if fops.IsNotExist(err) {
			return IPConfigPhaseUnknown, "", nil
		}
		return IPConfigPhaseUnknown, "", fmt.Errorf("failed to read status file %s: %w", filePath, err)
	}

	var st IPConfigRunStatus
	if err := json.Unmarshal(data, &st); err != nil {
		return IPConfigPhaseUnknown, "", fmt.Errorf("failed to parse status file %s: %w", filePath, err)
	}

	switch st.Phase {
	case IPConfigPhaseRunning, IPConfigPhaseSucceeded, IPConfigPhaseFailed:
		return st.Phase, st.Message, nil
	default:
		return IPConfigPhaseUnknown, st.Message, nil
	}
}

type IPConfigRunConfig struct {
	IPv4Address        string `json:"ipv4-address,omitempty"`
	IPv4MachineNetwork string `json:"ipv4-machine-network,omitempty"`
	IPv6Address        string `json:"ipv6-address,omitempty"`
	IPv6MachineNetwork string `json:"ipv6-machine-network,omitempty"`
	IPv4Gateway        string `json:"ipv4-gateway,omitempty"`
	IPv6Gateway        string `json:"ipv6-gateway,omitempty"`
	IPv4DNSServer      string `json:"ipv4-dns,omitempty"`
	IPv6DNSServer      string `json:"ipv6-dns,omitempty"`
	VLANID             int    `json:"vlan-id,omitempty"`
	HTTPProxy          string `json:"http-proxy,omitempty"`
	HTTPSProxy         string `json:"https-proxy,omitempty"`
	NoProxy            string `json:"no-proxy,omitempty"`
	StatusHTTPProxy    string `json:"status-http-proxy,omitempty"`
	StatusHTTPSProxy   string `json:"status-https-proxy,omitempty"`
	StatusNoProxy      string `json:"status-no-proxy,omitempty"`
	PullSecretRefName  string `json:"pull-secret-ref-name,omitempty"`
	RecertImage        string `json:"recert-image,omitempty"`
	DNSIPFamily        string `json:"dns-ip-family,omitempty"`
}

// IPConfigRunStatusPhase enumerates phases of the ip-config run lifecycle.
// Values are persisted to a JSON file; keep names stable.
type IPConfigRunStatusPhase string

const (
	IPConfigPhaseUnknown   IPConfigRunStatusPhase = "unknown"
	IPConfigPhaseRunning   IPConfigRunStatusPhase = "running"
	IPConfigPhaseSucceeded IPConfigRunStatusPhase = "succeeded"
	IPConfigPhaseFailed    IPConfigRunStatusPhase = "failed"
)

// IPConfigRunStatus describes current state of lca-cli ip-config run.
type IPConfigRunStatus struct {
	// Phase: running/succeeded/failed
	Phase IPConfigRunStatusPhase `json:"phase"`
	// Message: short human-readable summary
	Message string `json:"message,omitempty"`
	// StartedAt/FinishedAt are RFC3339 timestamps for observability
	StartedAt  string `json:"startedAt,omitempty"`
	FinishedAt string `json:"finishedAt,omitempty"`
}
