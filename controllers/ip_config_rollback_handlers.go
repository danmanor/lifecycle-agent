package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/go-logr/logr"
	ipcv1 "github.com/openshift-kni/lifecycle-agent/api/ipconfig/v1"
	controllerutils "github.com/openshift-kni/lifecycle-agent/controllers/utils"
	"github.com/openshift-kni/lifecycle-agent/internal/common"
	"github.com/openshift-kni/lifecycle-agent/lca-cli/ops"
	rpmostreeclient "github.com/openshift-kni/lifecycle-agent/lca-cli/ostreeclient"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	IPConfigRollbackPhasePrepivot  string = "RollbackPrePivot"
	IPConfigRollbackPhasePostpivot string = "RollbackPostPivot"
)

type IPConfigRollbackStageHandler struct {
	Client                  client.Client
	ChrootOps               ops.Ops
	TwoPhaseRollbackHandler IPConfigTwoPhaseStageHandler
}

func NewIPConfigRollbackStageHandler(
	client client.Client,
	chrootOps ops.Ops,
	twoPhaseHandler IPConfigTwoPhaseStageHandler,
) IPConfigStageHandler {
	return &IPConfigRollbackStageHandler{
		Client:                  client,
		ChrootOps:               chrootOps,
		TwoPhaseRollbackHandler: twoPhaseHandler,
	}
}

type IPConfigTwoPhaseRollbackHandler struct {
	Client          client.Client
	NoncachedClient client.Reader
	RPMOstreeClient rpmostreeclient.IClient
	Ops             ops.Ops
}

func NewIPConfigTwoPhaseRollbackHandler(
	client client.Client,
	noncachedClient client.Reader,
	rpmostreeClient rpmostreeclient.IClient,
	ops ops.Ops,
) IPConfigTwoPhaseStageHandler {
	return &IPConfigTwoPhaseRollbackHandler{
		Client:          client,
		NoncachedClient: noncachedClient,
		RPMOstreeClient: rpmostreeClient,
		Ops:             ops,
	}
}

func (r *IPConfigTwoPhaseRollbackHandler) PrePivot(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
	controllerutils.StartIPPhase(r.Client, logger, ipc, IPConfigRollbackPhasePrepivot)
	if r.RPMOstreeClient == nil {
		return requeueWithError(fmt.Errorf("rpm-ostree client is not set"))
	}

	stateroot, err := r.RPMOstreeClient.GetUnbootedStaterootName()
	if err != nil {
		controllerutils.SetIPRollbackStatusFailed(
			ipc,
			fmt.Errorf("failed to determine unbooted stateroot: %w", err).Error(),
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to determine unbooted stateroot: %w", err))
	}

	if err := r.Ops.RemountSysroot(); err != nil {
		controllerutils.SetIPRollbackStatusFailed(
			ipc,
			fmt.Errorf("failed to remount sysroot: %w", err).Error(),
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to remount sysroot: %w", err))
	}

	logger.Info("Save the IPConfig CR to the old state root before pivot")
	ipcsavePath := common.PathOutsideChroot(
		filepath.Join(
			common.GetStaterootPath(stateroot),
			controllerutils.IPCFilePath,
		),
	)

	data, err := json.Marshal(ipc)
	if err != nil {
		controllerutils.SetIPRollbackStatusFailed(
			ipc,
			fmt.Errorf("failed to marshal IPConfig CR: %w", err).Error(),
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to marshal IPConfig CR: %w", err))
	}

	if err := r.Ops.WriteFile(ipcsavePath, data, 0o600); err != nil {
		controllerutils.SetIPRollbackStatusFailed(
			ipc,
			fmt.Errorf("failed to save IPConfig CR before pivot: %w", err).Error(),
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(fmt.Errorf("failed to save IPConfig CR before pivot: %w", err))
	}

	if err := r.scheduleIPConfigRollback(ctx, ipc, logger, stateroot); err != nil {
		controllerutils.SetIPRollbackStatusFailed(
			ipc,
			err.Error(),
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}
		return requeueWithError(err)
	}

	// should not reach here on successful ip-config rollback

	return doNotRequeue(), nil
}

func (r *IPConfigTwoPhaseRollbackHandler) scheduleIPConfigRollback(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
	stateroot string,
) error {
	logger.Info("Scheduling lca-cli ip-config rollback via systemd-run", "stateroot", stateroot)
	args := []string{
		"--property", controllerutils.SystemdExitTypeCgroup,
		"--unit", controllerutils.IPConfigRollbackUnit,
		"--description", controllerutils.IPConfigRollbackDescription,
		controllerutils.LcaCliBinaryName, "ip-config", "rollback",
		"--stateroot", stateroot,
	}
	if _, err := r.Ops.SystemctlAction("run", args...); err != nil {
		controllerutils.SetIPRollbackStatusFailed(
			ipc,
			fmt.Errorf("failed to schedule ip-config rollback: %w", err).Error(),
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return fmt.Errorf("failed to update ipconfig status: %w", err)
		}

		return fmt.Errorf("failed to schedule ip-config rollback: %w", err)
	}

	return nil
}

func (r *IPConfigTwoPhaseRollbackHandler) PostPivot(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
	controllerutils.StartIPPhase(r.Client, logger, ipc, IPConfigRollbackPhasePostpivot)
	isAfterPivot := !isTargetStaterootBooted(ipc, r.RPMOstreeClient)
	if !isAfterPivot {
		return requeueWithError(fmt.Errorf("ip-config rollback cli command succeeded but old stateroot is not booted"))
	}

	log.FromContext(ctx).Info("Starting health check after rollback")
	if err := CheckHealth(ctx, r.NoncachedClient, log.FromContext(ctx)); err != nil {
		controllerutils.SetIPRollbackStatusInProgress(
			ipc,
			fmt.Sprintf("Waiting for system to stabilize: %s", err.Error()),
		)
		if err := r.Client.Status().Update(ctx, ipc); err != nil {
			return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
		}

		return requeueWithHealthCheckInterval(), fmt.Errorf("waiting for system to stabilize: %s", err.Error())
	}

	controllerutils.StopIPPhase(r.Client, logger, ipc, IPConfigRollbackPhasePostpivot)

	return doNotRequeue(), nil
}

// Handle executes the Rollback stage state machine
func (h *IPConfigRollbackStageHandler) Handle(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("IPConfigRollback")
	logger.Info("Starting handleRollback")

	if isIPTransitionRequested(ipc) {
		if err := validateIPConfigStage(ipc); err != nil {
			controllerutils.SetIPRollbackStatusFailed(
				ipc,
				"invalid transition: "+string(ipc.Spec.Stage),
			)
			if err := h.Client.Status().Update(ctx, ipc); err != nil {
				return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
			}
			return doNotRequeue(), nil
		}
	}

	phase, message, err := common.ReadIPConfigStatus(
		common.PathOutsideChroot(common.IPConfigRollbackStatusFile),
		h.ChrootOps,
	)
	if err != nil {
		return requeueWithError(fmt.Errorf("failed to read ip-config rollback status: %w", err))
	}

	switch phase {
	case common.IPConfigRunPhaseUnknown:
		return h.handleRollbackUnknown(ctx, ipc, logger)
	case common.IPConfigRunPhaseRunning:
		return h.handleRollbackRunning(logger)
	case common.IPConfigRunPhaseFailed:
		return h.handleRollbackFailed(ctx, ipc, logger, message)
	case common.IPConfigRunPhaseSucceeded:
		return h.handleRollbackSucceeded(ctx, ipc, logger)
	default:
		return requeueWithShortInterval(), nil
	}
}

func (h *IPConfigRollbackStageHandler) handleRollbackUnknown(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
	controllerutils.SetIPRollbackStatusInProgress(
		ipc,
		"Rollback is in progress",
	)
	if err := h.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Info("Running IP config Rollback PrePivot handler")
	result, err := h.TwoPhaseRollbackHandler.PrePivot(ctx, ipc, logger)
	if err != nil {
		return result, fmt.Errorf("failed to run rollback pre pivot: %w", err)
	}
	return result, nil
}

func (h *IPConfigRollbackStageHandler) handleRollbackRunning(logger logr.Logger) (ctrl.Result, error) {
	logger.Info("ip-config rollback in progress; requeueing")
	return requeueWithShortInterval(), nil
}

func (h *IPConfigRollbackStageHandler) handleRollbackFailed(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
	message string,
) (ctrl.Result, error) {
	controllerutils.SetIPRollbackStatusFailed(ipc, fmt.Sprintf("ip-config rollback failed: %s", message))
	if err := h.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	logger.Error(fmt.Errorf("ip-config rollback failed: %s", message), "ip-config rollback failed")
	return doNotRequeue(), nil
}

func (h *IPConfigRollbackStageHandler) handleRollbackSucceeded(
	ctx context.Context,
	ipc *ipcv1.IPConfig,
	logger logr.Logger,
) (ctrl.Result, error) {
	controllerutils.StopIPPhase(h.Client, logger, ipc, IPConfigRollbackPhasePrepivot)

	logger.Info("Running PostPivot handler")
	result, err := h.TwoPhaseRollbackHandler.PostPivot(ctx, ipc, logger)
	if err != nil {
		return result, fmt.Errorf("failed to run rollback post pivot: %w", err)
	}

	controllerutils.SetIPRollbackStatusCompleted(ipc, "Rollback completed")
	if err := h.Client.Status().Update(ctx, ipc); err != nil {
		return requeueWithError(fmt.Errorf("failed to update ipconfig status: %w", err))
	}

	controllerutils.StopIPStageHistory(h.Client, logger, ipc)

	logger.Info("rollback completed successfully")

	return requeueWithShortInterval(), nil
}
