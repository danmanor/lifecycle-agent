/*
Copyright 2025.

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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=ipconfigs,scope=Cluster,shortName=ipc
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Desired Stage",type="string",JSONPath=".spec.stage"
// +kubebuilder:printcolumn:name="State",type="string",JSONPath=".status.conditions[-1:].reason"
// +kubebuilder:printcolumn:name="Details",type="string",JSONPath=".status.conditions[-1:].message"
// +kubebuilder:validation:XValidation:message="ipconfig is a singleton, metadata.name must be 'ipconfig'", rule="self.metadata.name == 'ipconfig'"

// IPConfig is the Schema for controlling node IP configuration lifecycle via lca-cli ip-config.
type IPConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   IPConfigSpec   `json:"spec,omitempty"`
	Status IPConfigStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// IPConfigList contains a list of IPConfig
type IPConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []IPConfig `json:"items"`
}

// IPConfigStage defines the type for the stage field
type IPConfigStage string

var IPStages = struct {
	Idle      IPConfigStage
	Prepare   IPConfigStage
	Configure IPConfigStage
	Rollback  IPConfigStage
}{
	Idle:      "Idle",
	Prepare:   "Prepare",
	Configure: "Configure",
	Rollback:  "Rollback",
}

// IPFamilyConfig represents a single stack configuration
type IPFamilyConfig struct {
	// Address is the full address with prefix length (e.g., 192.0.2.10/24)
	Address string `json:"address,omitempty"`
	// Gateway is the default gateway address
	Gateway string `json:"gateway,omitempty"`
	// MachineNetwork is the CIDR of the machine network (e.g., 192.0.2.0/24)
	MachineNetwork string `json:"machineNetwork,omitempty"`
	// DNSServer is the DNS server IP to use
	DNSServer string `json:"dnsServer,omitempty"`
}

// VLANConfig represents optional VLAN configuration for the detected br-ex path
type VLANConfig struct {
	ID int `json:"id,omitempty"`
}

// ProxyConfig represents optional proxy configuration
type ProxyConfig struct {
	HTTPProxy  string   `json:"httpProxy,omitempty"`
	HTTPSProxy string   `json:"httpsProxy,omitempty"`
	NoProxy    []string `json:"noProxy,omitempty"`
}

// IPConfigSpec defines the desired state of IPConfig
type IPConfigSpec struct {
	// +kubebuilder:validation:Enum=Idle;Prepare;Configure;Rollback
	Stage IPConfigStage `json:"stage,omitempty"`

	// pullSecretRef is the name of a Secret in the openshift-config namespace containing .dockerconfigjson
	PullSecretRef string `json:"pullSecretRef,omitempty"`

	// IPv4 stack (omit for IPv6-only)
	IPv4 *IPFamilyConfig `json:"ipv4,omitempty"`

	// IPv6 stack (omit for IPv4-only)
	IPv6 *IPFamilyConfig `json:"ipv6,omitempty"`

	// Optional VLAN applied to br-ex path
	VLAN *VLANConfig `json:"vlan,omitempty"`

	// Optional proxy settings
	Proxy *ProxyConfig `json:"proxy,omitempty"`

	// Recert image for certificate rotation during configure stage
	RecertImage string `json:"recertImage,omitempty"`

	// RebootAutomatically, when true, will reboot the node on the same stateroot
	// after a successful lca-cli ip-config run. Defaults to false.
	// +kubebuilder:default=false
	RebootAutomatically bool `json:"rebootAutomatically,omitempty"`
}

// IPConfigStatus defines the observed state of IPConfig
type IPConfigStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`

	// ValidNextStages enumerates allowed next transitions from current stage
	ValidNextStages []IPConfigStage `json:"validNextStages,omitempty"`

	// ClusterIPs reflects the node internal IPs known to the cluster
	ClusterIPs *ClusterIPsStatus `json:"clusterIPs,omitempty"`
}

// HostNetworkStatus summarizes current host network
type HostNetworkStatus struct {
	VLAN *VLANConfig     `json:"vlan,omitempty"`
	IPv4 *IPFamilyConfig `json:"ipv4,omitempty"`
	IPv6 *IPFamilyConfig `json:"ipv6,omitempty"`
}

// ClusterIPsStatus contains node internal IPs
type ClusterIPsStatus struct {
	NodeInternalIPs []FamilyIP `json:"nodeInternalIPs,omitempty"`
}

// FamilyIP pairs IP family and address
type FamilyIP struct {
	Family  string `json:"family"`
	Address string `json:"address"`
}

func init() {
	SchemeBuilder.Register(&IPConfig{}, &IPConfigList{})
}
