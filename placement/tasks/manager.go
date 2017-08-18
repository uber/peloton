package tasks

import (
	"context"
	"errors"

	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	log "github.com/sirupsen/logrus"
)

// Manager will manage gangs/tasks and placements used by any placement strategy.
type Manager interface {
	// Dequeue will get some gangs from the resource manager.
	Dequeue(ctx context.Context, taskType resmgr.TaskType, batchSize int, timeout int) (gangs []*resmgrsvc.Gang, err error)

	// Enqueue will put some gangs into the resource manager which where not placed.
	Enqueue(ctx context.Context, gangs []*resmgrsvc.Gang) (err error)

	// SetPlacements will set the given placements in the resource manager.
	SetPlacements(ctx context.Context, placements []*resmgr.Placement) (err error)
}

// NewManager will create a new task manager.
func NewManager(resourceManager resmgrsvc.ResourceManagerServiceYARPCClient) Manager {
	return &taskManager{
		resourceManager: resourceManager,
	}
}

type taskManager struct {
	resourceManager resmgrsvc.ResourceManagerServiceYARPCClient
}

func (manager *taskManager) Dequeue(ctx context.Context, taskType resmgr.TaskType, batchSize int, timeout int) ([]*resmgrsvc.Gang, error) {
	request := &resmgrsvc.DequeueGangsRequest{
		Limit:   uint32(batchSize),
		Type:    taskType,
		Timeout: uint32(timeout),
	}
	log.WithField("request", request).Debug("Dequeuing gangs")
	response, err := manager.resourceManager.DequeueGangs(ctx, request)
	if err != nil {
		log.WithField("error", err).Error("Dequeue gangs failed")
		return nil, err
	}
	log.WithField("tasks", response.Gangs).Debug("Dequeued gangs")
	return response.Gangs, nil
}

func (manager *taskManager) SetPlacements(ctx context.Context, placements []*resmgr.Placement) error {
	if len(placements) == 0 {
		log.Debug("No task to place")
		err := errors.New("No placements to set")
		return err
	}
	var request = &resmgrsvc.SetPlacementsRequest{
		Placements: placements,
	}
	log.WithField("request", request).Debug("Calling Set")
	response, err := manager.resourceManager.SetPlacements(ctx, request)
	// TODO: add retry / put back offer and tasks in failure scenarios
	if err != nil {
		log.WithFields(log.Fields{
			"num_placements": len(placements),
			"error":          err.Error(),
		}).WithError(errors.New("Failed to set placements"))
		return err
	}
	log.WithField("response", response).Debug("Set call returned")
	if response.GetError() != nil {
		log.WithFields(log.Fields{
			"num_placements": len(placements),
			"error":          response.Error.String(),
		}).Error("Failed to place tasks")
		return errors.New("Failed to place tasks")
	}
	return nil
}

func (manager *taskManager) Enqueue(ctx context.Context, gangs []*resmgrsvc.Gang) error {
	// TODO(mu): send unplaced tasks back to correct state (READY) per T1028631.
	log.Warnf("Enqueue in %T is not implemented yet", manager)
	return nil
}
