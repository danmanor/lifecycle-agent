package controllers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	igntypes "github.com/coreos/ignition/v2/config/v3_2/types"
	"github.com/go-logr/logr"
	machineconfigv1 "github.com/openshift/api/machineconfiguration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	syaml "sigs.k8s.io/yaml"

	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/internal/ostreeclient"
	"github.com/openshift-kni/lifecycle-agent/internal/reboot"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	rpmostreeclient "github.com/openshift-kni/lifecycle-agent/lca-cli/ostreeclient"
	lcautils "github.com/openshift-kni/lifecycle-agent/utils"
	"github.com/samber/lo"
)

//+kubebuilder:rbac:groups=lca.openshift.io,resources=ipconfigs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=lca.openshift.io,resources=ipconfigs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=machineconfiguration.openshift.io,resources=machineconfigs,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get
//+kubebuilder:rbac:groups="",resources=pods,verbs=get
//+kubebuilder:rbac:groups="",resources=nodes,verbs=get
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get
//+kubebuilder:rbac:groups=config.openshift.io,resources=proxies,verbs=get;list;watch

// IPConfigReconciler reconciles an IPConfig object
type IPConfigReconciler struct {
	client.Client
	NoncachedClient client.Reader
	Scheme          *runtime.Scheme
	ChrootOps       ops.Ops
	NsenterOps      ops.Ops
	RebootClient    reboot.RebootIntf
	RPMOstreeClient rpmostreeclient.IClient
	OstreeClient    ostreeclient.IClient
	Clientset       *kubernetes.Clientset
	IdleHandler     IPConfigStageHandler
	ConfigHandler   IPConfigStageHandler
	RollbackHandler IPConfigStageHandler
	Mux             *sync.Mutex
}

//go:generate mockgen -source=ipc_controller.go -package=controllers -destination=ipc_controller_mock.go
type IPConfigStageHandler interface {
	Handle(ctx context.Context, ipc *ipcv1.IPConfig) (ctrl.Result, error)
}

func (r *IPConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	if r.Mux != nil {
		r.Mux.Lock()
		defer r.Mux.Unlock()
	}

	logger := log.FromContext(ctx).WithName("IPConfig")
	logger.Info(
		"Start reconciling IPConfig",
		"name", req.NamespacedName.Name,
		"namespace", req.NamespacedName.Namespace,
	)

	ipc, err := r.getIPConfig(ctx, logger)
	if err != nil {
		return requeueWithError(fmt.Errorf("failed to get IPConfig: %w", err))
	}
	ipc.Status.ObservedGeneration = ipc.Generation

	defer func() {
		vns, vErr := validNextStages(ipc, r.RPMOstreeClient)
		if vErr != nil {
			if err != nil {
				err = fmt.Errorf("%w; also failed to get valid next stages: %v", err, vErr)
			} else {
				err = fmt.Errorf("failed to get valid next stages: %w", vErr)
			}
			return
		}
		ipc.Status.ValidNextStages = vns
		if uErr := r.Client.Status().Update(ctx, ipc); uErr != nil {
			if err != nil {
				err = fmt.Errorf("%w; also failed to update ipconfig status: %v", err, uErr)
			} else {
				err = fmt.Errorf("failed to update ipconfig status: %w", uErr)
			}
		}
	}()

	if ipc.Status.ValidNextStages == nil {
		vns, err := validNextStages(ipc, r.RPMOstreeClient)
		if err != nil {
			return requeueWithError(fmt.Errorf("failed to get valid next stages: %w", err))
		}
		ipc.Status.ValidNextStages = vns
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
	}

	if err := r.refreshHostAndClusterNetwork(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to refresh host/cluster network: %w", err))
	}

	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	if err := r.cacheRecertImageIfNeeded(ctx, ipc, logger); err != nil {
		logger.Error(err, "recert image caching failed")
	}

	annotations := ipc.GetAnnotations()
	if annotations != nil && annotations[controllerutils.TriggerReconcileAnnotation] != "" {
		delete(annotations, controllerutils.TriggerReconcileAnnotation)
		ipc.SetAnnotations(annotations)
	}

	// Start stage history timer. The timer is stopped from inside the handlers when they complete successfully
	controllerutils.StartIPStageHistory(r.Client, logger, ipc)
	// .status.history is reset as long as the desired stage is Idle
	controllerutils.ResetIPHistory(r.Client, logger, ipc)

	switch ipc.Spec.Stage {
	case ipcv1.IPStages.Idle:
		return r.IdleHandler.Handle(ctx, ipc)
	case ipcv1.IPStages.Config:
		return r.ConfigHandler.Handle(ctx, ipc)
	case ipcv1.IPStages.Rollback:
		return r.RollbackHandler.Handle(ctx, ipc)
	default:
		// Shouldn't happen
		logger.Error(nil, "invalid IPConfig stage", "stage", ipc.Spec.Stage)
		return doNotRequeue(), nil
	}
}

func validNextStages(ipc *ipcv1.IPConfig, rpmOstreeClient rpmostreeclient.IClient) ([]ipcv1.IPConfigStage, error) {
	inProgressStage := controllerutils.GetIPInProgressStage(ipc)

	if inProgressStage == ipcv1.IPStages.Idle ||
		inProgressStage == ipcv1.IPStages.Rollback ||
		controllerutils.IsIPStageFailed(ipc, ipcv1.IPStages.Rollback) {
		return []ipcv1.IPConfigStage{}, nil
	}

	if inProgressStage == ipcv1.IPStages.Config ||
		controllerutils.IsIPStageFailed(ipc, ipcv1.IPStages.Config) {
		if isTargetStaterootBooted(ipc, rpmOstreeClient) {
			return []ipcv1.IPConfigStage{ipcv1.IPStages.Rollback}, nil
		}
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Idle}, nil
	}

	// no in progress stage, check completed stages in reverse order
	if controllerutils.IsIPStageCompleted(ipc, ipcv1.IPStages.Rollback) {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Idle}, nil
	}
	if controllerutils.IsIPStageCompleted(ipc, ipcv1.IPStages.Config) {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Idle, ipcv1.IPStages.Rollback}, nil
	}
	if controllerutils.IsIPStageCompleted(ipc, ipcv1.IPStages.Idle) {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Config}, nil
	}

	// initial IPConfig creation - no idle condition
	idleCondition := meta.FindStatusCondition(ipc.Status.Conditions, string(controllerutils.ConditionTypes.Idle))
	if idleCondition == nil {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Idle}, nil
	}

	return []ipcv1.IPConfigStage{}, nil
}

// isTargetStaterootBooted determines whether the stateroot prepared for this IP change is currently booted.
// It reconstructs the expected stateroot name from the spec (matching the lca-cli prepare logic) and queries rpm-ostree.
func isTargetStaterootBooted(ipc *ipcv1.IPConfig, rpmOstreeClient rpmostreeclient.IClient) bool {
	if rpmOstreeClient == nil {
		return false
	}
	target := buildIPConfigStaterootName(ipc)
	if target == "" {
		return false
	}
	booted, err := rpmOstreeClient.IsStaterootBooted(target)
	if err != nil {
		return false
	}
	return booted
}

// buildIPConfigStaterootName mirrors the lca-cli ip-config prepare naming scheme: rhcos_<ipv4>_<ipv6>
// where IPs are sanitized to alphanumeric and dashes, and IPv6 brackets are stripped.
func buildIPConfigStaterootName(ipc *ipcv1.IPConfig) string {
	var ipv4, ipv6 string
	if ipc.Spec.IPv4 != nil {
		ipv4 = ipc.Spec.IPv4.Address
	}

	if ipc.Spec.IPv6 != nil {
		ipv6 = ipc.Spec.IPv6.Address
	}

	return common.BuildNewStaterootNameFromIps(ipv4, ipv6)
}

// SetupWithManager sets up the controller with the Manager.
func (r *IPConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	//nolint:wrapcheck
	return ctrl.NewControllerManagedBy(mgr).
		For(&ipcv1.IPConfig{}, builder.WithPredicates(predicate.Funcs{
			UpdateFunc: func(e event.UpdateEvent) bool {
				if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
					return true
				}

				// trigger reconcile upon adding or removing ManualCleanupAnnotation
				_, oldExist := e.ObjectOld.GetAnnotations()[controllerutils.ManualCleanupAnnotation]
				_, newExist := e.ObjectNew.GetAnnotations()[controllerutils.ManualCleanupAnnotation]
				if oldExist != newExist {
					return true
				}

				// trigger reconcile upon adding or updating TriggerReconcileAnnotation
				oldValue, oldHas := e.ObjectOld.GetAnnotations()[controllerutils.TriggerReconcileAnnotation]
				newValue, newHas := e.ObjectNew.GetAnnotations()[controllerutils.TriggerReconcileAnnotation]
				if (!oldHas && newHas) || (oldHas && newHas && oldValue != newValue) {
					return true
				}

				return false
			},
			CreateFunc:  func(ce event.CreateEvent) bool { return true },
			GenericFunc: func(ge event.GenericEvent) bool { return false },
			DeleteFunc: func(de event.DeleteEvent) bool {
				if de.Object.GetName() == common.IPConfigName {
					ipc := de.Object.(*ipcv1.IPConfig)
					filePath := common.PathOutsideChroot(controllerutils.IPCFilePath)
					if controllerutils.IsIPStageCompleted(ipc, ipcv1.IPStages.Idle) ||
						controllerutils.IsIPStageFailed(ipc, ipcv1.IPStages.Rollback) {
						if err := os.Remove(filePath); err != nil {
							if !os.IsNotExist(err) {
								fmt.Printf("Failed to remove IPConfig from %s: %v", filePath, err)
							}
						}
					} else {
						if err := lcautils.MarshalToFile(de.Object, filePath); err != nil {
							fmt.Printf("Failed to save deleted IPConfig to %s: %v", filePath, err)
						}
					}
					return true
				}
				return false
			},
		})).
		Complete(r)
}

// getIPConfig tries to get the IPConfig CR by performing the following operations in order:
//   - Fetching from the API from the non-cached client or initializes it if it doesn't exist.
//   - If the latter fails, it attempts to restore the IPConfig CR from the file system.
//   - If the restoration fails, it creates a new IPConfig CR.
func (r *IPConfigReconciler) getIPConfig(ctx context.Context, logger logr.Logger) (*ipcv1.IPConfig, error) {
	ipc := &ipcv1.IPConfig{}
	if err := r.NoncachedClient.Get(ctx, client.ObjectKey{Name: common.IPConfigName}, ipc); err != nil {
		if errors.IsNotFound(err) {
			if initErr := lcautils.InitIPConfig(ctx, r.Client, &logger); initErr != nil {
				return nil, fmt.Errorf("failed to initialize IPConfig: %w", initErr)
			}
			return ipc, nil
		}
		return nil, fmt.Errorf("failed to get IPConfig: %w", err)
	}
	return ipc, nil
}

func validateIPConfigStage(ipc *ipcv1.IPConfig) error {
	if !lo.Contains(ipc.Status.ValidNextStages, ipc.Spec.Stage) {
		return fmt.Errorf("invalid IPConfig stage: %s", ipc.Spec.Stage)
	}

	return nil
}

// cacheRecertImageIfNeeded pulls and caches the recert image if it hasn't been cached yet.
// If the annotation is not provided, it resolves the image via getRecertImage.
func (r *IPConfigReconciler) cacheRecertImageIfNeeded(ctx context.Context, ipc *ipcv1.IPConfig, logger logr.Logger) error {
	annotations := ipc.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}

	image := annotations[controllerutils.RecertImageAnnotation]
	if image == "" {
		image = getRecertImage(ipc)
	}

	if cached := annotations[controllerutils.RecertCachedImageAnnotation]; cached == image {
		return nil
	}

	authFile := common.ImageRegistryAuthFile
	if name := annotations[controllerutils.RecertPullSecretAnnotation]; name != "" {
		pullSecret, err := lcautils.GetSecretData(
			ctx,
			name,
			common.LcaNamespace,
			corev1.DockerConfigJsonKey,
			r.Client,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to get pull-secret with the name %s in namespace %s holding the key %s: %w",
				name,
				common.LcaNamespace,
				corev1.DockerConfigJsonKey,
				err,
			)
		}

		tempFile, err := os.CreateTemp(os.TempDir(), "recert-pull-secret.json")
		if err != nil {
			return fmt.Errorf("failed to create temp file: %w", err)
		}
		defer os.Remove(tempFile.Name())

		if _, err := tempFile.WriteString(pullSecret); err != nil {
			return fmt.Errorf("failed to write pull secret to temp file: %w", err)
		}
		tempFile.Close()
		authFile = tempFile.Name()
	}

	if _, err := r.ChrootOps.RunBashInHostNamespace(
		"podman",
		"pull",
		"--authfile",
		authFile,
		image,
	); err != nil {
		return fmt.Errorf("failed to pull recert image %s: %w", image, err)
	}

	annotations[controllerutils.RecertCachedImageAnnotation] = image
	ipc.SetAnnotations(annotations)
	if err := r.Client.Update(ctx, ipc); err != nil {
		return fmt.Errorf("failed to update annotations after caching recert image: %w", err)
	}

	logger.Info("recert image cached on host", "image", image)

	return nil
}

func isIPTransitionRequested(ipc *ipcv1.IPConfig) bool {
	desiredStage := ipc.Spec.Stage
	if desiredStage == ipcv1.IPStages.Idle {
		return !(controllerutils.IsIPStageCompleted(ipc, desiredStage) ||
			controllerutils.IsIPStageInProgress(ipc, desiredStage))
	}
	return !(controllerutils.IsIPStageCompletedOrFailed(ipc, desiredStage) ||
		controllerutils.IsIPStageInProgress(ipc, desiredStage))
}

type nmAddr struct {
	IP           string `json:"ip"`
	PrefixLength int    `json:"prefix-length"`
}

type nmIPConf struct {
	Enabled bool     `json:"enabled"`
	Address []nmAddr `json:"address"`
}

type nmIf struct {
	Name   string   `json:"name"`
	Type   string   `json:"type"`
	IPv4   nmIPConf `json:"ipv4"`
	IPv6   nmIPConf `json:"ipv6"`
	Bridge nmBridge `json:"bridge,omitempty"`
	VLAN   *nmVLAN  `json:"vlan,omitempty"`
}

type nmRoute struct {
	Destination      string `json:"destination"`
	NextHopAddress   string `json:"next-hop-address"`
	NextHopInterface string `json:"next-hop-interface"`
}

type nmRoutes struct {
	Running []nmRoute `json:"running"`
	Config  []nmRoute `json:"config"`
}

type nmDNSList struct {
	Server []string `json:"server"`
}

type nmDNS struct {
	Running nmDNSList `json:"running"`
	Config  nmDNSList `json:"config"`
}

type nmState struct {
	Interfaces  []nmIf   `json:"interfaces"`
	Routes      nmRoutes `json:"routes"`
	DNSResolver nmDNS    `json:"dns-resolver"`
}

type nmBridge struct {
	Port []struct {
		Name string `json:"name"`
	} `json:"port"`
}

type nmVLAN struct {
	BaseIface string `json:"base-iface"`
	ID        int    `json:"id"`
}

// refreshHostAndClusterNetwork orchestrates nmstate collection and status population
func (r *IPConfigReconciler) refreshHostAndClusterNetwork(ctx context.Context, ipc *ipcv1.IPConfig) error {
	output, err := r.nmstateShowJSON()
	if err != nil {
		return err
	}

	state, err := parseNmstate(output)
	if err != nil {
		return err
	}

	dnsV4, dnsV6 := extractDNS(state)
	gw4, gw6 := findDefaultGateways(state)
	vlanID, err := extractBrExVLANID(state)
	if err != nil {
		return err
	}

	nodeIPs, err := r.findNodeIPs(ctx)
	if err != nil {
		return fmt.Errorf("failed to find node IPs: %w", err)
	}
	machineCIDRs, err := r.findMachineNetworks(ctx)
	if err != nil {
		return fmt.Errorf("failed to find machine networks: %w", err)
	}

	host, cluster := buildHostAndCluster(
		gw4,
		gw6,
		dnsV4,
		dnsV6,
		nodeIPs,
		machineCIDRs,
		vlanID,
	)

	if ipc.Status.Network == nil {
		ipc.Status.Network = &ipcv1.NetworkStatus{}
	}

	ipc.Status.Network.HostNetwork = host
	ipc.Status.Network.ClusterNetwork = cluster

	fam, err := r.inferDNSResolutionFamilyFromMC(ctx)
	if err != nil {
		return fmt.Errorf("failed to infer DNS resolution family from MC: %w", err)
	}
	ipc.Status.DNSResolutionFamily = lo.FromPtr(fam)

	return nil
}

// inferDNSResolutionFamilyFromMC inspects the MachineConfig used to configure dnsmasq
// and infers the active DNS filter: "ipv4", "ipv6" or "none" when not set.
// It is assumed that the dnsmasq MachineConfig is the only one that contains the dnsmasq filter file.
// and it can only container one of the known filters or not exist.
func (r *IPConfigReconciler) inferDNSResolutionFamilyFromMC(ctx context.Context) (*string, error) {
	mc := &machineconfigv1.MachineConfig{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: common.DnsmasqMachineConfigName}, mc); err != nil {
		return nil, fmt.Errorf("failed to get dnsmasq machine config: %w", err)
	}

	var cfg igntypes.Config
	if len(mc.Spec.Config.Raw) > 0 {
		if err := json.Unmarshal(mc.Spec.Config.Raw, &cfg); err != nil {
			return nil, fmt.Errorf("failed to parse ignition config: %w", err)
		}
	}

	v4Encoded := base64.StdEncoding.EncodeToString([]byte(common.DnsmasqFilterIPv4))
	v6Encoded := base64.StdEncoding.EncodeToString([]byte(common.DnsmasqFilterIPv6))
	v4Source := fmt.Sprintf(common.DataURLBase64Template, v4Encoded)
	v6Source := fmt.Sprintf(common.DataURLBase64Template, v6Encoded)

	for _, f := range cfg.Storage.Files {
		if f.Path != common.DnsmasqFilterTargetPath {
			continue
		}

		if f.Contents.Source == nil {
			return nil, fmt.Errorf("contents source is nil for file %s", f.Path)
		}

		switch *f.Contents.Source {
		case v4Source:
			return lo.ToPtr(common.IPv4FamilyName), nil
		case v6Source:
			return lo.ToPtr(common.IPv6FamilyName), nil
		default:
			return nil, fmt.Errorf("unknown contents source: %s", *f.Contents.Source)
		}
	}

	return lo.ToPtr("none"), nil
}

// installConfigSubset captures only the fields we need from install-config
type installConfigSubset struct {
	Networking struct {
		MachineNetwork []struct {
			CIDR string `yaml:"cidr"`
		} `yaml:"machineNetwork"`
	} `yaml:"networking"`
}

func (r *IPConfigReconciler) findNodeIPs(ctx context.Context) ([]string, error) {
	podName := os.Getenv("MY_POD_NAME")
	podNS := os.Getenv("MY_POD_NAMESPACE")
	if podName == "" || podNS == "" {
		podNS = common.LcaNamespace
	}

	pod := &corev1.Pod{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: podName, Namespace: podNS}, pod); err != nil {
		return nil, fmt.Errorf("failed to get controller pod: %w", err)
	}

	node := &corev1.Node{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, node); err != nil {
		return nil, fmt.Errorf("failed to get node %s: %w", pod.Spec.NodeName, err)
	}

	var nodeIPs []string
	for _, a := range node.Status.Addresses {
		if a.Type != corev1.NodeInternalIP {
			continue
		}
		nodeIPs = append(nodeIPs, a.Address)
	}

	return nodeIPs, nil
}

func (r *IPConfigReconciler) findMachineNetworks(ctx context.Context) ([]string, error) {
	cm := &corev1.ConfigMap{}
	if err := r.Client.Get(
		ctx, types.NamespacedName{
			Name:      common.InstallConfigCM,
			Namespace: common.InstallConfigCMNamespace,
		}, cm,
	); err != nil {
		return nil, fmt.Errorf("failed to get cluster-config-v1 configmap: %w", err)
	}
	icRaw, ok := cm.Data[common.InstallConfigCMInstallConfigDataKey]
	if !ok {
		return nil, fmt.Errorf("install-config key missing in cluster-config-v1 configmap")
	}

	var ic installConfigSubset
	if err := syaml.Unmarshal([]byte(icRaw), &ic); err != nil {
		return nil, fmt.Errorf("failed to parse install-config yaml: %w", err)
	}

	var machineCIDRs []string
	for _, mn := range ic.Networking.MachineNetwork {
		if mn.CIDR != "" {
			machineCIDRs = append(machineCIDRs, mn.CIDR)
		}
	}
	return machineCIDRs, nil
}

func (r *IPConfigReconciler) nmstateShowJSON() (string, error) {
	output, err := r.NsenterOps.RunInHostNamespace("nmstatectl", "show", "--json", "-q")
	if err != nil {
		return "", fmt.Errorf("failed to run nmstatectl show --json: %w", err)
	}

	return output, nil
}

func parseNmstate(output string) (nmState, error) {
	var state nmState

	if err := json.Unmarshal([]byte(output), &state); err != nil {
		return state, fmt.Errorf("failed to parse nmstate JSON: %w", err)
	}
	return state, nil
}

func extractDNS(state nmState) (string, string) {
	dnsServers := state.DNSResolver.Running.Server
	if len(dnsServers) == 0 {
		dnsServers = state.DNSResolver.Config.Server
	}

	var dnsV4, dnsV6 string
	for _, s := range dnsServers {
		if strings.Contains(s, ":") {
			if dnsV6 == "" {
				dnsV6 = s
			}
		} else {
			if dnsV4 == "" {
				dnsV4 = s
			}
		}
	}
	return dnsV4, dnsV6
}

func findDefaultGateways(state nmState) (string, string) {
	findGW := func(dest string) string {
		for _, rt := range state.Routes.Running {
			if rt.Destination == dest && (rt.NextHopInterface == "" || rt.NextHopInterface == controllerutils.BridgeExternalName) {
				return rt.NextHopAddress
			}
		}
		for _, rt := range state.Routes.Config {
			if rt.Destination == dest && (rt.NextHopInterface == "" || rt.NextHopInterface == controllerutils.BridgeExternalName) {
				return rt.NextHopAddress
			}
		}
		return ""
	}
	return findGW(controllerutils.DefaultRouteV4), findGW(controllerutils.DefaultRouteV6)
}

func buildHostAndCluster(
	gw4 string,
	gw6 string,
	dnsV4 string,
	dnsV6 string,
	nodeIPs []string,
	machineCIDRs []string,
	vlanID *int,
) (*ipcv1.HostNetworkStatus, *ipcv1.ClusterNetworkStatus) {
	host := &ipcv1.HostNetworkStatus{}
	cluster := &ipcv1.ClusterNetworkStatus{}

	var nodeIPv4, nodeIPv6 string
	for _, ip := range nodeIPs {
		if strings.Contains(ip, ":") {
			if nodeIPv6 == "" {
				nodeIPv6 = ip
			}
		} else {
			if nodeIPv4 == "" {
				nodeIPv4 = ip
			}
		}
	}

	if nodeIPv4 != "" {
		cluster.IPv4 = &ipcv1.ClusterIPStatus{
			Address:        nodeIPv4,
			MachineNetwork: findMatchingCIDR(nodeIPv4, machineCIDRs),
		}
	}
	if nodeIPv6 != "" {
		cluster.IPv6 = &ipcv1.ClusterIPStatus{
			Address:        nodeIPv6,
			MachineNetwork: findMatchingCIDR(nodeIPv6, machineCIDRs),
		}
	}

	host.IPv4 = &ipcv1.HostIPStatus{
		Gateway:   gw4,
		DNSServer: dnsV4,
	}

	host.IPv6 = &ipcv1.HostIPStatus{
		Gateway:   gw6,
		DNSServer: dnsV6,
	}

	if vlanID != nil {
		host.VLANID = *vlanID
	}

	return host, cluster
}

// findMatchingCIDR returns the first CIDR from the list that contains the given IP
// and matches its IP family. If none is found, returns an empty string.
func findMatchingCIDR(ipStr string, cidrs []string) string {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return ""
	}
	isV4 := ip.To4() != nil
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil || n == nil {
			continue
		}
		if (n.IP.To4() != nil) != isV4 {
			continue
		}
		if n.Contains(ip) {
			return c
		}
	}
	return ""
}

// extractBrExUplinkName returns the uplink port name connected to br-ex (excluding the br-ex internal and patch ports)
func extractBrExUplinkName(state nmState) (*string, error) {
	for _, intf := range state.Interfaces {
		if intf.Name == controllerutils.BridgeExternalName && intf.Type == "ovs-bridge" {
			for _, p := range intf.Bridge.Port {
				if !strings.Contains(p.Name, controllerutils.BridgeExternalName) && p.Name != "" {
					return &p.Name, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("br-ex uplink port not found")
}

// extractBrExVLANID inspects the br-ex uplink port; if it's a VLAN interface, returns its VLAN ID.
func extractBrExVLANID(state nmState) (*int, error) {
	uplink, err := extractBrExUplinkName(state)
	if err != nil {
		return nil, err
	}

	for _, intf := range state.Interfaces {
		if intf.Name == lo.FromPtr(uplink) && intf.Type == "vlan" && intf.VLAN != nil {
			return &intf.VLAN.ID, nil
		}
	}

	return nil, nil
}
