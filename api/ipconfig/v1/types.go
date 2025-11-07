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
// +operator-sdk:csv:customresourcedefinitions:displayName="IP Configuration",resources={{Namespace, v1}}
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Desired Stage",type="string",JSONPath=".spec.stage"
// +kubebuilder:printcolumn:name="State",type="string",JSONPath=".status.conditions[-1:].reason"
// +kubebuilder:printcolumn:name="Details",type="string",JSONPath=".status.conditions[-1:].message"
// +kubebuilder:printcolumn:name="Current IPv4",type="string",JSONPath=".status.network.clusterNetwork.ipv4.address",priority=1
// +kubebuilder:printcolumn:name="Desired IPv4",type="string",JSONPath=".spec.ipv4.address",priority=1
// +kubebuilder:printcolumn:name="Current IPv6",type="string",JSONPath=".status.network.clusterNetwork.ipv6.address",priority=1
// +kubebuilder:printcolumn:name="Desired IPv6",type="string",JSONPath=".spec.ipv6.address",priority=1
// +kubebuilder:validation:XValidation:message="ipconfig is a singleton, metadata.name must be 'ipconfig'", rule="self.metadata.name == 'ipconfig'"
// +kubebuilder:validation:XValidation:message="can not change spec.ipv4 while ipconfig is in progress",rule="!has(oldSelf.status) || oldSelf.status.conditions.exists(c, c.type=='Idle' && c.status=='True') || has(oldSelf.spec.ipv4) && has(self.spec.ipv4) && oldSelf.spec.ipv4==self.spec.ipv4 || !has(self.spec.ipv4) && !has(oldSelf.spec.ipv4)"
// +kubebuilder:validation:XValidation:message="can not change spec.ipv6 while ipconfig is in progress",rule="!has(oldSelf.status) || oldSelf.status.conditions.exists(c, c.type=='Idle' && c.status=='True') || has(oldSelf.spec.ipv6) && has(self.spec.ipv6) && oldSelf.spec.ipv6==self.spec.ipv6 || !has(self.spec.ipv6) && !has(oldSelf.spec.ipv6)"

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
	Idle     IPConfigStage
	Config   IPConfigStage
	Rollback IPConfigStage
}{
	Idle:     "Idle",
	Config:   "Config",
	Rollback: "Rollback",
}

// IPFamilyConfig represents a single stack configuration
type IPFamilyConfig struct {
	// +kubebuilder:validation:Required
	// Address is the full address with prefix length (e.g., 192.0.2.10/24)
	//+operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	Address string `json:"address,omitempty"`
	// Gateway is the default gateway address
	//+operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	Gateway string `json:"gateway,omitempty"`
	// +kubebuilder:validation:Required
	// MachineNetwork is the CIDR of the machine network (e.g., 192.0.2.0/24)
	//+operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	MachineNetwork string `json:"machineNetwork,omitempty"`
	// DNSServer is the DNS server IP to use
	//+operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	DNSServer string `json:"dnsServer,omitempty"`
}

// VLANConfig represents optional VLAN configuration for the detected br-ex path
type VLANConfig struct {
	// +kubebuilder:validation:Required
	//+operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:number"}
	ID int `json:"id,omitempty"`
}

// ProxyConfig represents optional proxy configuration
type ProxyConfig struct {
	//+operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	HTTPProxy string `json:"httpProxy,omitempty"`
	//+operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	HTTPSProxy string `json:"httpsProxy,omitempty"`
	//+operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:arrayFieldGroup"}
	NoProxy []string `json:"noProxy,omitempty"`
}

type PullSecretRef struct {
	// +kubebuilder:validation:Required
	// +required
	//+operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	Name string `json:"name"`
}

// RecertSpec defines image pull settings for recert usage in IP config flow
type RecertSpec struct {
	// pullSecretRef is the name of a Secret in the lifecycle-agent namespace containing .dockerconfigjson
	//+operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	PullSecretRef *PullSecretRef `json:"pullSecretRef,omitempty"`
	// image is the full pull-spec of the recert container image to use
	//+operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	Image string `json:"image,omitempty"`
	// cacheInterval defines how often the controller attempts to cache the recert image on the host
	// when IPConfig is Idle. If unset, a default is used.
	//+operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:text"}
	CacheInterval metav1.Duration `json:"cacheInterval,omitempty"`
}

// IPConfigSpec defines the desired state of IPConfig
type IPConfigSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=Idle;Config;Rollback
	//+operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Stage"
	Stage IPConfigStage `json:"stage,omitempty"`

	// IPv4 stack (omit for IPv6-only)
	//+operator-sdk:csv:customresourcedefinitions:type=spec,displayName="IPv4"
	IPv4 *IPFamilyConfig `json:"ipv4,omitempty"`

	// IPv6 stack (omit for IPv4-only)
	//+operator-sdk:csv:customresourcedefinitions:type=spec,displayName="IPv6"
	IPv6 *IPFamilyConfig `json:"ipv6,omitempty"`

	// Optional VLAN applied to br-ex path
	//+operator-sdk:csv:customresourcedefinitions:type=spec,displayName="VLAN"
	VLAN *VLANConfig `json:"vlan,omitempty"`

	// Optional proxy settings
	//+operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Proxy"
	Proxy *ProxyConfig `json:"proxy,omitempty"`

	// Recert configuration
	//+operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Recert"
	Recert *RecertSpec `json:"recert,omitempty"`

	// AutoRollbackOnFailure defines automatic rollback settings for IPConfig if the configuration
	// does not complete within the specified time limit. Behavior mirrors IBU.
	// +optional
	//+operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Auto Rollback On Failure"
	AutoRollbackOnFailure *AutoRollbackOnFailure `json:"autoRollbackOnFailure,omitempty"`
}

// AutoRollbackOnFailure defines automatic rollback settings if the IP configuration does not
// complete within the specified time limit.
type AutoRollbackOnFailure struct {
	// InitMonitorTimeoutSeconds defines the time frame in seconds. If not defined or set to 0,
	// the default value of 1800 seconds (30 minutes) is used.
	// +kubebuilder:validation:Minimum=0
	//+operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:number"}
	InitMonitorTimeoutSeconds int `json:"initMonitorTimeoutSeconds,omitempty"`
}

// IPConfigStatus defines the observed state of IPConfig
type IPConfigStatus struct {
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Observed Generation"
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Conditions",xDescriptors={"urn:alm:descriptor:io.kubernetes.conditions"}
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ValidNextStages enumerates allowed next transitions from current stage
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Valid Next Stage"
	ValidNextStages []IPConfigStage `json:"validNextStages,omitempty"`

	// Network groups host and cluster network information
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Network"
	Network *NetworkStatus `json:"network,omitempty"`

	// History stores timing info of different IPConfig stages and their important phases
	// +optional
	History []*IPHistory `json:"history,omitempty"`
}

// HostNetworkStatus summarizes current host network
type HostNetworkStatus struct {
	IPv4 *IPFamilyConfig `json:"ipv4,omitempty"`
	IPv6 *IPFamilyConfig `json:"ipv6,omitempty"`
}

// ClusterNetworkStatus summarizes cluster network using lists of strings
type ClusterNetworkStatus struct {
	// IPv4 summarizes the current IPv4 on the cluster network
	IPv4 *ClusterIPStatus `json:"ipv4,omitempty"`
	// IPv6 summarizes the current IPv6 on the cluster network
	IPv6 *ClusterIPStatus `json:"ipv6,omitempty"`
}

// NetworkStatus groups host and cluster network views
type NetworkStatus struct {
	// HostNetwork summarizes current host network
	HostNetwork *HostNetworkStatus `json:"hostNetwork,omitempty"`
	// ClusterNetwork summarizes cluster network using lists of strings
	ClusterNetwork *ClusterNetworkStatus `json:"clusterNetwork,omitempty"`
}

// ClusterIPStatus represents a single IP family view on the cluster network
type ClusterIPStatus struct {
	// Address is the node internal IP (plain address, no prefix)
	Address string `json:"address,omitempty"`
	// MachineNetwork is the matching machine network CIDR for the IP
	MachineNetwork string `json:"machineNetwork,omitempty"`
}

// IPHistory mirrors IBU history for IPConfig stages
type IPHistory struct {
	// Stage The desired stage name read from spec
	Stage IPConfigStage `json:"stage,omitempty"`
	// Phases allows a granular view of important tasks within a Stage
	Phases []*IPPhase `json:"phases,omitempty"`
	// StartTime A timestamp to indicate the Stage has started
	StartTime metav1.Time `json:"startTime,omitempty"`
	// CompletionTime A timestamp indicating the Stage completed successfully
	CompletionTime metav1.Time `json:"completionTime,omitempty"`
}

// IPPhase represents a sub-step within a stage
type IPPhase struct {
	// Phase current phase within a Stage
	Phase string `json:"phase,omitempty"`
	// StartTime A timestamp indicating the Phase has started
	StartTime metav1.Time `json:"startTime,omitempty"`
	// CompletionTime A timestamp indicating the phase completed successfully
	CompletionTime metav1.Time `json:"completionTime,omitempty"`
}

func init() {
	SchemeBuilder.Register(&IPConfig{}, &IPConfigList{})
}
