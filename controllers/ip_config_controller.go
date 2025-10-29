package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	"github.com/samber/lo"
)

//+kubebuilder:rbac:groups=lca.openshift.io,resources=ipconfigs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=lca.openshift.io,resources=ipconfigs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get
//+kubebuilder:rbac:groups="",resources=pods,verbs=get
//+kubebuilder:rbac:groups="",resources=nodes,verbs=get

// IPConfigReconciler reconciles an IPConfig object
type IPConfigReconciler struct {
	client.Client
	NoncachedClient client.Reader
	Scheme          *runtime.Scheme
	Executor        ops.Execute
	ChrootOps       ops.Ops
	NsenterOps      ops.Ops
	RebootClient    reboot.RebootIntf
	RPMOstreeClient rpmostreeclient.IClient
	OstreeClient    ostreeclient.IClient
	Clientset       *kubernetes.Clientset
	PrepHandler     IPConfigPrepHandlerInterface
	ConfigHandler   IPConfigConfigurationHandlerInterface
	RollbackHandler IPConfigRollbackHandlerInterface
	Mux             *sync.Mutex
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

	ipc, err := r.getOrCreateIPConfig(ctx)
	if err != nil {
		return requeueWithError(fmt.Errorf("failed to get or create IPConfig: %w", err))
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

	if err := r.refreshHostAndClusterNetwork(ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to refresh host/cluster network: %w", err))
	}
	if err := r.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	// Start stage history timer. The timer is stopped from inside the handlers when they complete successfully
	controllerutils.StartIPStageHistory(r.Client, logger, ipc)
	// .status.history is reset as long as the desired stage is Idle
	controllerutils.ResetIPHistory(r.Client, logger, ipc)

	switch ipc.Spec.Stage {
	case ipcv1.IPStages.Idle:
		return r.handleIdle(ctx, ipc)
	case ipcv1.IPStages.Prep:
		return r.handlePrep(ctx, ipc)
	case ipcv1.IPStages.Config:
		return r.handleConfig(ctx, ipc)
	case ipcv1.IPStages.Rollback:
		return r.handleRollback(ctx, ipc)
	default:
		// Shouldn't happen
		logger.Error(nil, "invalid IPConfig stage", "stage", ipc.Spec.Stage)
		return doNotRequeue(), nil
	}
}

func validNextStages(ipc *ipcv1.IPConfig, rpmOstreeClient rpmostreeclient.IClient) ([]ipcv1.IPConfigStage, error) {
	inProgressStage := controllerutils.GetIPInProgressStage(ipc)

	if inProgressStage == ipcv1.IPStages.Idle || inProgressStage == ipcv1.IPStages.Rollback || controllerutils.IsIPStageFailed(ipc, ipcv1.IPStages.Rollback) {
		// no valid transition if aborting/abort failed/finalizing/finalize failed/rollback in progress/rollback failed
		return []ipcv1.IPConfigStage{}, nil
	}

	if inProgressStage == ipcv1.IPStages.Prep || controllerutils.IsIPStageFailed(ipc, ipcv1.IPStages.Prep) {
		isInNewStateroot := isTargetStaterootBooted(ipc, rpmOstreeClient)
		if isInNewStateroot {
			return []ipcv1.IPConfigStage{ipcv1.IPStages.Rollback}, nil
		} else {
			return []ipcv1.IPConfigStage{ipcv1.IPStages.Idle}, nil
		}
	}

	if inProgressStage == ipcv1.IPStages.Config || controllerutils.IsIPStageFailed(ipc, ipcv1.IPStages.Config) {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Rollback}, nil
	}

	// no in progress stage, check completed stages in reverse order
	if controllerutils.IsIPStageCompleted(ipc, ipcv1.IPStages.Rollback) {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Idle}, nil
	}
	if controllerutils.IsIPStageCompleted(ipc, ipcv1.IPStages.Config) {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Idle, ipcv1.IPStages.Rollback}, nil
	}
	if controllerutils.IsIPStageCompleted(ipc, ipcv1.IPStages.Prep) {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Config, ipcv1.IPStages.Rollback}, nil
	}
	if controllerutils.IsIPStageCompleted(ipc, ipcv1.IPStages.Idle) {
		return []ipcv1.IPConfigStage{ipcv1.IPStages.Prep}, nil
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
	parts := []string{"rhcos"}
	if v := ipc.Spec.IPv4; v != nil && v.Address != "" {
		parts = append(parts, common.SanitizeForOsname(strings.Split(v.Address, "/")[0]))
	}
	if v := ipc.Spec.IPv6; v != nil && v.Address != "" {
		addr := strings.Split(v.Address, "/")[0]
		addr = strings.Trim(addr, "[]")
		parts = append(parts, common.SanitizeForOsname(addr))
	}
	if len(parts) == 1 {
		return ""
	}
	return strings.Join(parts, "_")
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
			DeleteFunc:  func(de event.DeleteEvent) bool { return false },
		})).
		Complete(r)
}

func (r *IPConfigReconciler) getOrCreateIPConfig(ctx context.Context) (*ipcv1.IPConfig, error) {
	ipc := &ipcv1.IPConfig{}
	if err := r.NoncachedClient.Get(ctx, client.ObjectKey{Name: common.IPConfigName}, ipc); err != nil {
		if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get IPConfig: %w", err)
		}

		ipc = &ipcv1.IPConfig{
			ObjectMeta: metav1.ObjectMeta{Name: common.IPConfigName},
			Spec:       ipcv1.IPConfigSpec{Stage: ipcv1.IPStages.Idle},
		}

		if createErr := r.Client.Create(ctx, ipc); createErr != nil {
			return nil, fmt.Errorf("failed to create IPConfig: %w", createErr)
		}

		if getErr := r.NoncachedClient.Get(ctx, client.ObjectKey{Name: common.IPConfigName}, ipc); getErr != nil {
			return nil, fmt.Errorf("failed to get IPConfig after creation: %w", getErr)
		}
	}

	return ipc, nil
}

func (r *IPConfigReconciler) validateIPConfigStage(ipc *ipcv1.IPConfig) error {
	if !lo.Contains(ipc.Status.ValidNextStages, ipc.Spec.Stage) {
		return fmt.Errorf("invalid IPConfig stage: %s", ipc.Spec.Stage)
	}

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
	Name string   `json:"name"`
	Type string   `json:"type"`
	IPv4 nmIPConf `json:"ipv4"`
	IPv6 nmIPConf `json:"ipv6"`
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

// refreshHostAndClusterNetwork orchestrates nmstate collection and status population
func (r *IPConfigReconciler) refreshHostAndClusterNetwork(ipc *ipcv1.IPConfig) error {
	output, err := r.nmstateShowJSON()
	if err != nil {
		return err
	}

	state, err := parseNmstate(output)
	if err != nil {
		return err
	}

	br := pickBrExInterface(state)
	dnsV4, dnsV6 := extractDNS(state)
	gw4, gw6 := findDefaultGateways(state)

	nodeIPs, err := r.findNodeIPs(context.TODO())
	if err != nil {
		return fmt.Errorf("failed to find node IPs: %w", err)
	}
	machineCIDRs, err := r.findMachineNetworks(context.TODO())
	if err != nil {
		return fmt.Errorf("failed to find machine networks: %w", err)
	}

	host, cluster := buildHostAndCluster(
		br,
		gw4,
		gw6,
		dnsV4,
		dnsV6,
		nodeIPs,
		machineCIDRs,
	)

	ipc.Status.HostNetwork = host
	ipc.Status.ClusterNetwork = cluster

	return nil
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

	if err := json.Unmarshal([]byte(output), &state); err == nil {
		fmt.Println("parsed JSON state:", state)
		return state, nil
	}

	return state, nil
}

func pickBrExInterface(state nmState) nmIf {
	var chosen nmIf
	for _, i := range state.Interfaces {
		if i.Name == controllerutils.BridgeExternalName && i.Type == controllerutils.OvsInterfaceType {
			chosen = i
			break
		}
	}
	return chosen
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

func toCIDR(ip string, prefix int) string {
	if ip == "" || prefix <= 0 {
		return ""
	}
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return ""
	}
	var mask net.IPMask
	if parsed.To4() != nil {
		mask = net.CIDRMask(prefix, controllerutils.IPv4TotalBits)
	} else {
		mask = net.CIDRMask(prefix, controllerutils.IPv6TotalBits)
	}
	network := parsed.Mask(mask)
	return fmt.Sprintf("%s/%d", network.String(), prefix)
}

func buildHostAndCluster(
	br nmIf,
	gw4 string,
	gw6 string,
	dnsV4 string,
	dnsV6 string,
	nodeIPs []string,
	machineCIDRs []string,
) (*ipcv1.HostNetworkStatus, *ipcv1.ClusterNetworkStatus) {
	host := &ipcv1.HostNetworkStatus{}
	cluster := &ipcv1.ClusterNetworkStatus{}

	if len(br.IPv4.Address) > 0 {
		ip := br.IPv4.Address[0]
		host.IPv4 = &ipcv1.IPFamilyConfig{
			Address:        fmt.Sprintf("%s/%d", ip.IP, ip.PrefixLength),
			Gateway:        gw4,
			MachineNetwork: toCIDR(ip.IP, ip.PrefixLength),
			DNSServer:      dnsV4,
		}
	}
	if len(br.IPv6.Address) > 0 {
		ip := br.IPv6.Address[0]
		host.IPv6 = &ipcv1.IPFamilyConfig{
			Address:        fmt.Sprintf("%s/%d", ip.IP, ip.PrefixLength),
			Gateway:        gw6,
			MachineNetwork: toCIDR(ip.IP, ip.PrefixLength),
			DNSServer:      dnsV6,
		}
	}

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
