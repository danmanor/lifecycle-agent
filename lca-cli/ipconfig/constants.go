package ipconfig

// Constants used by the lca-cli ipconfig commands to avoid magic strings
const (
	// External bridge name and related identifiers
	BridgeExternalName    = "br-ex"
	BrExMachineConfigName = "10-br-ex"

	// IP family names used in messages
	IPv4FamilyName = "IPv4"
	IPv6FamilyName = "IPv6"

	// Systemd unit name for nodeip rerun
	NodeipRerunUnitName = "sno-nodeip-rerun.service"
)
