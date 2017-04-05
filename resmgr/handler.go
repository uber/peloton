package resmgr

import (
	"context"
	"reflect"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/task"

	"peloton/private/resmgr"
	"peloton/private/resmgrsvc"
)

// serviceHandler implements peloton.private.resmgr.ResourceManagerService
// TODO: add placing and placed task queues
type serviceHandler struct {
	metrics     *Metrics
	resPoolTree respool.Tree
	placements  queue.Queue
}

// InitServiceHandler initializes the handler for ResourceManagerService
func InitServiceHandler(d yarpc.Dispatcher, parent tally.Scope) {

	handler := serviceHandler{
		metrics:     NewMetrics(parent.SubScope("resmgr")),
		resPoolTree: respool.GetTree(),
		placements: queue.NewQueue(
			"placement-queue",
			reflect.TypeOf(resmgr.Placement{}),
			maxPlacementQueueSize,
		),
	}

	json.Register(d, json.Procedure("ResourceManagerService.EnqueueTasks", handler.EnqueueTasks))
	json.Register(d, json.Procedure("ResourceManagerService.DequeueTasks", handler.DequeueTasks))
	json.Register(d, json.Procedure("ResourceManagerService.SetPlacements", handler.SetPlacements))
	json.Register(d, json.Procedure("ResourceManagerService.GetPlacements", handler.GetPlacements))
}

// EnqueueTasks implements ResourceManagerService.EnqueueTasks
func (h *serviceHandler) EnqueueTasks(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *resmgrsvc.EnqueueTasksRequest,
) (*resmgrsvc.EnqueueTasksResponse, yarpc.ResMeta, error) {

	log.WithField("request", req).Info("EnqueueTasks called.")
	h.metrics.APIEnqueueTasks.Inc(1)

	// Lookup respool from the resource pool tree
	respoolID := req.GetResPool()
	respool, err := respool.GetTree().Get(respoolID)
	if err != nil {
		h.metrics.EnqueueTaskFail.Inc(1)
		return &resmgrsvc.EnqueueTasksResponse{
			Error: &resmgrsvc.EnqueueTasksResponse_Error{
				NotFound: &resmgrsvc.ResourcePoolNotFound{
					Id:      respoolID,
					Message: err.Error(),
				},
			},
		}, nil, nil
	}
	// TODO: check if the user has permission to run tasks in the
	// respool

	// Enqueue tasks to the pending queue of the respool
	var failed []*resmgrsvc.EnqueueTasksFailure_FailedTask
	for _, task := range req.GetTasks() {
		err = respool.EnqueueTask(task)
		if err != nil {
			failed = append(
				failed,
				&resmgrsvc.EnqueueTasksFailure_FailedTask{
					Task:    task,
					Message: err.Error(),
				},
			)
			h.metrics.EnqueueTaskFail.Inc(1)
		} else {
			h.metrics.EnqueueTaskSuccess.Inc(1)
		}
	}

	if len(failed) > 0 {
		return &resmgrsvc.EnqueueTasksResponse{
			Error: &resmgrsvc.EnqueueTasksResponse_Error{
				Failure: &resmgrsvc.EnqueueTasksFailure{
					Failed: failed,
				},
			},
		}, nil, nil
	}

	response := resmgrsvc.EnqueueTasksResponse{}
	log.Debug("Enqueue Returned")
	return &response, nil, nil
}

// DequeueTasks implements ResourceManagerService.DequeueTasks
func (h *serviceHandler) DequeueTasks(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *resmgrsvc.DequeueTasksRequest,
) (*resmgrsvc.DequeueTasksResponse, yarpc.ResMeta, error) {

	log.WithField("request", req).Info("DequeueTasks called.")
	h.metrics.APIDequeueTasks.Inc(1)

	limit := req.GetLimit()
	timeout := time.Duration(req.GetTimeout())
	readyQueue := task.GetScheduler().GetReadyQueue()

	var tasks []*resmgr.Task
	for i := uint32(0); i < limit; i++ {
		item, err := readyQueue.Dequeue(timeout * time.Millisecond)
		if err != nil {
			log.WithError(err).Warning("Failed to dequeue task from ready queue")
			h.metrics.DequeueTaskFail.Inc(1)
			break
		}
		task := item.(*resmgr.Task)
		tasks = append(tasks, task)
		h.metrics.DequeueTaskSuccess.Inc(1)

		// TODO: We should move the task from READY to PLACING queue
	}
	// TODO: handle the dequeue errors better
	response := resmgrsvc.DequeueTasksResponse{Tasks: tasks}
	log.WithField("response", response).Debug("DequeueTasks succeeded")
	log.Debug("Dequeue Returned")
	return &response, nil, nil
}

// SetPlacements implements ResourceManagerService.SetPlacements
func (h *serviceHandler) SetPlacements(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *resmgrsvc.SetPlacementsRequest,
) (*resmgrsvc.SetPlacementsResponse, yarpc.ResMeta, error) {

	log.WithField("request", req).Debug("SetPlacements called.")
	h.metrics.APISetPlacements.Inc(1)

	var failed []*resmgrsvc.SetPlacementsFailure_FailedPlacement
	var err error
	for _, placement := range req.GetPlacements() {
		err = h.placements.Enqueue(placement)
		if err != nil {
			log.WithField("placement", placement).
				WithError(err).Error("Failed to enqueue placement")
			failed = append(
				failed,
				&resmgrsvc.SetPlacementsFailure_FailedPlacement{
					Placement: placement,
					Message:   err.Error(),
				},
			)
			h.metrics.SetPlacementFail.Inc(1)
		} else {
			h.metrics.SetPlacementSuccess.Inc(1)
			// TODO: We should move the tasks from PLACING to PLACED queue
		}
	}
	if len(failed) > 0 {
		return &resmgrsvc.SetPlacementsResponse{
			Error: &resmgrsvc.SetPlacementsResponse_Error{
				Failure: &resmgrsvc.SetPlacementsFailure{
					Failed: failed,
				},
			},
		}, nil, nil
	}
	response := resmgrsvc.SetPlacementsResponse{}
	h.metrics.PlacementQueueLen.Update(float64(h.placements.Length()))
	log.Debug("Set Placement Returned")
	return &response, nil, nil
}

// GetPlacements implements ResourceManagerService.GetPlacements
func (h *serviceHandler) GetPlacements(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *resmgrsvc.GetPlacementsRequest,
) (*resmgrsvc.GetPlacementsResponse, yarpc.ResMeta, error) {

	log.WithField("request", req).Debug("GetPlacements called.")
	h.metrics.APIGetPlacements.Inc(1)

	limit := req.GetLimit()
	timeout := time.Duration(req.GetTimeout())

	h.metrics.APIGetPlacements.Inc(1)
	var placements []*resmgr.Placement
	for i := 0; i < int(limit); i++ {
		item, err := h.placements.Dequeue(timeout * time.Millisecond)

		if err != nil {
			h.metrics.GetPlacementFail.Inc(1)
			break
		}
		placement := item.(*resmgr.Placement)
		placements = append(placements, placement)
		h.metrics.GetPlacementSuccess.Inc(1)
	}
	response := resmgrsvc.GetPlacementsResponse{Placements: placements}
	h.metrics.PlacementQueueLen.Update(float64(h.placements.Length()))
	log.Debug("Get Placement Returned")

	return &response, nil, nil
}
