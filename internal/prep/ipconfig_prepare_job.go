package prep

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
)

const (
	// IPConfigPrepareJobName is deprecated; prepare now runs via systemd unit
	IPConfigPrepareJobName      = "lca-ipconfig-prepare"
	ipConfigPrepareJobFinalizer = "lca.openshift.io/ipconfig-prepare-finalizer"
)

// IPConfigPrepareTerminationGracePeriodSeconds sets max wait before k8s SIGKILLs the job pod
var IPConfigPrepareTerminationGracePeriodSeconds int64 = 1800

// GetIPConfigPrepareJob retrieves the existing ip-config prepare job if present
func GetIPConfigPrepareJob(ctx context.Context, c client.Client, log logr.Logger) (*batchv1.Job, error) {
	job := &batchv1.Job{}
	if err := c.Get(ctx, types.NamespacedName{Name: IPConfigPrepareJobName, Namespace: common.LcaNamespace}, job); err != nil {
		return job, err //nolint:wrapcheck
	}

	log.Info("Got ip-config prepare job", "namespace", job.Namespace, "name", job.Name)
	return job, nil
}

// LaunchIPConfigPrepareJob renders and creates a new ip-config prepare job
func LaunchIPConfigPrepareJob(ctx context.Context, c client.Client, ipc *ipcv1.IPConfig, scheme *runtime.Scheme, log logr.Logger, ipv4Addr, ipv6Addr string) (*batchv1.Job, error) {
	job, err := constructIPConfigPrepareJob(ctx, c, ipc, scheme, log, ipv4Addr, ipv6Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to render job: %w", err)
	}

	if err := c.Create(ctx, job); err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			return nil, err //nolint:wrapcheck
		}
	}

	log.Info("Successfully created ip-config prepare job", "job", job.Name)
	return job, nil
}

func constructIPConfigPrepareJob(
	ctx context.Context,
	c client.Client,
	ipc *ipcv1.IPConfig,
	scheme *runtime.Scheme,
	log logr.Logger,
	ipv4Addr,
	ipv6Addr string,
) (*batchv1.Job, error) {
	log.Info("Getting lca deployment to configure ip-config prepare job")
	lcaDeployment := appsv1.Deployment{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: common.LcaNamespace, Name: "lifecycle-agent-controller-manager"}, &lcaDeployment); err != nil {
		return nil, fmt.Errorf("failed to get lifecycle-agent-controller-manager deployment: %w", err)
	}

	log.Info("Selecting 'manager' from LCA deployment", "deployment", lcaDeployment.Name)
	manager, ok := getManagerContainer(lcaDeployment)
	if !ok {
		return nil, fmt.Errorf("no 'manager' container found in deployment")
	}

	// Build command with optional flags
	cmd := []string{"lca-cli", "ip-config", "prepare"}
	if ipv4Addr != "" {
		cmd = append(cmd, "--ipv4-address", ipv4Addr)
	}
	if ipv6Addr != "" {
		cmd = append(cmd, "--ipv6-address", ipv6Addr)
	}

	var backoffLimit int32 = 0
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      IPConfigPrepareJobName,
			Namespace: common.LcaNamespace,
			Annotations: map[string]string{
				"app.kubernetes.io/name": IPConfigPrepareJobName,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						common.WorkloadManagementAnnotationKey: common.WorkloadManagementAnnotationValue,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            IPConfigPrepareJobName,
							Image:           manager.Image,
							ImagePullPolicy: manager.ImagePullPolicy,
							Command:         cmd,
							Env:             manager.Env,
							EnvFrom:         manager.EnvFrom,
							SecurityContext: manager.SecurityContext,
							VolumeMounts:    manager.VolumeMounts,
							Resources:       manager.Resources,
						},
					},
					HostPID:                       lcaDeployment.Spec.Template.Spec.HostPID,
					ServiceAccountName:            lcaDeployment.Spec.Template.Spec.ServiceAccountName,
					RestartPolicy:                 corev1.RestartPolicyNever,
					Volumes:                       lcaDeployment.Spec.Template.Spec.Volumes,
					TerminationGracePeriodSeconds: &IPConfigPrepareTerminationGracePeriodSeconds,
				},
			},
		},
	}

	// set owner reference to IPConfig CR
	if err := ctrl.SetControllerReference(ipc, job, scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}

	// set finalizer
	controllerutil.AddFinalizer(job, ipConfigPrepareJobFinalizer)

	log.Info("Done rendering a new ip-config prepare job", "job", job.Name)
	return job, nil
}

// DeleteIPConfigPrepareJob deletes the ip-config prepare job and waits until its pod is removed
func DeleteIPConfigPrepareJob(ctx context.Context, c client.Client, log logr.Logger) error {
	if err := removeIPConfigPrepareJobFinalizer(ctx, c, log); err != nil {
		return fmt.Errorf("failed to remove finalizer during cleanup: %w", err)
	}

	job := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      IPConfigPrepareJobName,
			Namespace: common.LcaNamespace,
		},
	}
	if err := c.Delete(ctx, &job, common.GenerateDeleteOptions()); err != nil {
		if !k8serrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete ip-config prepare job: %w", err)
		}
	}

	log.Info(fmt.Sprintf("Waiting up to additional %s to verify that job's pod no longer exists", time.Duration(IPConfigPrepareTerminationGracePeriodSeconds)*time.Second), "job", job.GetName())
	if err := waitUntilIPConfigPreparePodIsRemoved(ctx, c); err != nil {
		return fmt.Errorf("failed to wait until ip-config prepare job pod is removed: %w", err)
	}

	log.Info("Successfully removed all ip-config prepare job resources", "job", job.GetName())
	return nil
}

// waitUntilIPConfigPreparePodIsRemoved waits until the pod created by the job is removed
func waitUntilIPConfigPreparePodIsRemoved(ctx context.Context, c client.Client) error {
	return wait.PollUntilContextTimeout(ctx, time.Second, time.Duration(IPConfigPrepareTerminationGracePeriodSeconds)*time.Second, true, func(context.Context) (bool, error) { //nolint:wrapcheck
		opts := []client.ListOption{
			client.InNamespace(common.LcaNamespace),
			client.MatchingLabels{"job-name": IPConfigPrepareJobName},
		}
		podList := &corev1.PodList{}
		if err := c.List(ctx, podList, opts...); err != nil {
			return false, fmt.Errorf("failed to list pods: %w", err)
		}

		return len(podList.Items) == 0, nil
	})
}

// removeIPConfigPrepareJobFinalizer removes the finalizer from the job if present
func removeIPConfigPrepareJobFinalizer(ctx context.Context, c client.Client, log logr.Logger) error {
	job, err := GetIPConfigPrepareJob(ctx, c, log)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get ip-config prepare job to remove finalizer: %w", err)
	}

	if controllerutil.ContainsFinalizer(job, ipConfigPrepareJobFinalizer) {
		finalizerRemoved := controllerutil.RemoveFinalizer(job, ipConfigPrepareJobFinalizer)
		if finalizerRemoved {
			if err := c.Update(ctx, job); err != nil {
				return fmt.Errorf("failed to remove finalizer during update: %w", err)
			}
		}
	}

	log.Info("Removed ip-config prepare finalizer", "finalizer", ipConfigPrepareJobFinalizer)
	return nil
}
