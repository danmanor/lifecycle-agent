package common

type IPConfigRunConfig struct {
	IPv4Address        string `json:"ipv4-address,omitempty"`
	IPv4MachineNetwork string `json:"ipv4-machine-network,omitempty"`
	IPv6Address        string `json:"ipv6-address,omitempty"`
	IPv6MachineNetwork string `json:"ipv6-machine-network,omitempty"`
	IPv4Gateway        string `json:"ipv4-gateway,omitempty"`
	IPv6Gateway        string `json:"ipv6-gateway,omitempty"`
	IPv4DNSServer      string `json:"ipv4-dns,omitempty"`
	IPv6DNSServer      string `json:"ipv6-dns,omitempty"`
	HTTPProxy          string `json:"http-proxy,omitempty"`
	HTTPSProxy         string `json:"https-proxy,omitempty"`
	NoProxy            string `json:"no-proxy,omitempty"`
	PullSecretFile     string `json:"pull-secret-file,omitempty"`
	RecertImage        string `json:"recert-image,omitempty"`
}

// IPConfigRunStatusPhase enumerates phases of the ip-config run lifecycle.
// Values are persisted to a JSON file; keep names stable.
type IPConfigRunStatusPhase string

const (
	IPConfigRunPhaseUnknown   IPConfigRunStatusPhase = "unknown"
	IPConfigRunPhaseRunning   IPConfigRunStatusPhase = "running"
	IPConfigRunPhaseSucceeded IPConfigRunStatusPhase = "succeeded"
	IPConfigRunPhaseFailed    IPConfigRunStatusPhase = "failed"
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
