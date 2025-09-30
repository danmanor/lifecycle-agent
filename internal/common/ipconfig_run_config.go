package common

type IPConfigRunConfig struct {
	IPv4Address            string `json:"ipv4-address,omitempty"`
	IPv4MachineNetwork     string `json:"ipv4-machine-network,omitempty"`
	IPv6Address            string `json:"ipv6-address,omitempty"`
	IPv6MachineNetwork     string `json:"ipv6-machine-network,omitempty"`
	HTTPProxy              string `json:"http-proxy,omitempty"`
	HTTPSProxy             string `json:"https-proxy,omitempty"`
	NoProxy                string `json:"no-proxy,omitempty"`
	PullSecretFile         string `json:"pull-secret-file,omitempty"`
	DisableIPConfigService bool   `json:"disable-ip-config-service,omitempty"`
	RebootAutomatically    bool   `json:"reboot-automatically,omitempty"`
}
