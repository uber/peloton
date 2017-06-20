package resmgr

import (
	"container/list"
	"context"
	"reflect"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/uber-go/tally"

	"go.uber.org/yarpc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/resmgr/respool"
	rmtask "code.uber.internal/infra/peloton/resmgr/task"
	"code.uber.internal/infra/peloton/util"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	t "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
)

var (
	errFailingGangMemberTask = errors.New("task fail because other gang member failed")
)

// ServiceHandler implements peloton.private.resmgr.ResourceManagerService
// TODO: add placing and placed task queues
type ServiceHandler struct {
	metrics            *Metrics
	resPoolTree        respool.Tree
	placements         queue.Queue
	eventStreamHandler *eventstream.Handler
	rmTracker          rmtask.Tracker
	maxOffset          *uint64
}

// InitServiceHandler initializes the handler for ResourceManagerService
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	rmTracker rmtask.Tracker) *ServiceHandler {

	var maxOffset uint64
	handler := &ServiceHandler{
		metrics:     NewMetrics(parent.SubScope("resmgr")),
		resPoolTree: respool.GetTree(),
		placements: queue.NewQueue(
			"placement-queue",
			reflect.TypeOf(resmgr.Placement{}),
			maxPlacementQueueSize,
		),
		rmTracker: rmTracker,
		maxOffset: &maxOffset,
	}
	// TODO: move eventStreamHandler buffer size into config
	handler.eventStreamHandler = initEventStreamHandler(d, 1000, parent.SubScope("resmgr"))

	d.Register(resmgrsvc.BuildResourceManagerServiceYarpcProcedures(handler))
	return handler
}

func initEventStreamHandler(d *yarpc.Dispatcher, bufferSize int, parentScope tally.Scope) *eventstream.Handler {
	eventStreamHandler := eventstream.NewEventStreamHandler(
		bufferSize,
		[]string{
			common.PelotonJobManager,
			common.PelotonResourceManager,
		},
		nil,
		parentScope)

	d.Register(pb_eventstream.BuildEventStreamServiceYarpcProcedures(eventStreamHandler))

	return eventStreamHandler
}

// EnqueueGangs implements ResourceManagerService.EnqueueGangs
func (h *ServiceHandler) EnqueueGangs(
	ctx context.Context,
	req *resmgrsvc.EnqueueGangsRequest,
) (*resmgrsvc.EnqueueGangsResponse, error) {

	log.WithField("request", req).Info("EnqueueGangs called.")
	h.metrics.APIEnqueueGangs.Inc(1)

	// Lookup respool from the resource pool tree
	respoolID := req.GetResPool()
	respool, err := respool.GetTree().Get(respoolID)
	if err != nil {
		h.metrics.EnqueueGangFail.Inc(1)
		return &resmgrsvc.EnqueueGangsResponse{
			Error: &resmgrsvc.EnqueueGangsResponse_Error{
				NotFound: &resmgrsvc.ResourcePoolNotFound{
					Id:      respoolID,
					Message: err.Error(),
				},
			},
		}, nil
	}
	// TODO: check if the user has permission to run tasks in the
	// respool

	// Enqueue the gangs sent in an API call to the pending queue of the respool.
	// For each gang, add its tasks to the state machine, enqueue the gang, and
	// return per-task success/failure.
	var failed []*resmgrsvc.EnqueueGangsFailure_FailedTask
	for _, gang := range req.GetGangs() {
		gangTasks := new(list.List)
		for _, task := range gang.GetTasks() {
			// Adding task to state machine
			err := h.rmTracker.AddTask(
				task,
				h.eventStreamHandler,
				respool,
			)
			if err != nil {
				failed = append(
					failed,
					&resmgrsvc.EnqueueGangsFailure_FailedTask{
						Task:    task,
						Message: err.Error(),
					},
				)
				h.metrics.EnqueueGangFail.Inc(1)
				continue
			}
			if h.rmTracker.GetTask(task.Id) != nil {
				err = h.rmTracker.GetTask(task.Id).TransitTo(
					t.TaskState_PENDING.String())
				if err != nil {
					log.Error(err)
				} else {
					gangTasks.PushBack(task)
				}
			}
		}
		if len(failed) == 0 {
			err = respool.EnqueueGang(gang)
		} else {
			err = errFailingGangMemberTask
		}
		// Report per-task success/failure for all tasks in gang
		for gangTask := gangTasks.Front(); gangTask != nil; gangTask = gangTask.Next() {
			if err != nil {
				failed = append(
					failed,
					&resmgrsvc.EnqueueGangsFailure_FailedTask{
						Task:    gangTask.Value.(*resmgr.Task),
						Message: err.Error(),
					},
				)
				h.metrics.EnqueueGangFail.Inc(1)
			} else {
				h.metrics.EnqueueGangSuccess.Inc(1)
			}
		}
	}

	if len(failed) > 0 {
		return &resmgrsvc.EnqueueGangsResponse{
			Error: &resmgrsvc.EnqueueGangsResponse_Error{
				Failure: &resmgrsvc.EnqueueGangsFailure{
					Failed: failed,
				},
			},
		}, nil
	}

	response := resmgrsvc.EnqueueGangsResponse{}
	log.Debug("Enqueue Returned")
	return &response, nil
}

// DequeueGangs implements ResourceManagerService.DequeueGangs
func (h *ServiceHandler) DequeueGangs(
	ctx context.Context,
	req *resmgrsvc.DequeueGangsRequest,
) (*resmgrsvc.DequeueGangsResponse, error) {

	h.metrics.APIDequeueGangs.Inc(1)

	limit := req.GetLimit()
	timeout := time.Duration(req.GetTimeout())
	sched := rmtask.GetScheduler()

	var gangs []*resmgrsvc.Gang
	for i := uint32(0); i < limit; i++ {
		gang, err := sched.DequeueGang(timeout*time.Millisecond, req.Type)
		if err != nil {
			log.Debug("Timeout to dequeue gang from ready queue")
			h.metrics.DequeueGangTimeout.Inc(1)
			break
		}
		gangs = append(gangs, gang)
		for _, task := range gang.GetTasks() {
			h.metrics.DequeueGangSuccess.Inc(1)

			// Moving task to Placing state
			if h.rmTracker.GetTask(task.Id) != nil {
				err = h.rmTracker.GetTask(task.Id).TransitTo(
					t.TaskState_PLACING.String())
				if err != nil {
					log.WithError(err).WithField(
						"taskID", task.Id.Value).
						Error("Failed to transit state " +
							"for task")
				}
			}
		}
	}
	// TODO: handle the dequeue errors better
	response := resmgrsvc.DequeueGangsResponse{Gangs: gangs}
	log.WithField("response", response).Debug("DequeueGangs succeeded")
	return &response, nil
}

// SetPlacements implements ResourceManagerService.SetPlacements
func (h *ServiceHandler) SetPlacements(
	ctx context.Context,
	req *resmgrsvc.SetPlacementsRequest,
) (*resmgrsvc.SetPlacementsResponse, error) {

	log.WithField("request", req).Debug("SetPlacements called.")
	h.metrics.APISetPlacements.Inc(1)

	var failed []*resmgrsvc.SetPlacementsFailure_FailedPlacement
	var err error
	for _, placement := range req.GetPlacements() {
		var notValidTasks []*peloton.TaskID
		// Transitioning tasks from Placing to Placed
		for _, taskID := range placement.Tasks {
			if h.rmTracker.GetTask(taskID) != nil {
				log.WithFields(log.Fields{
					"Current state": h.rmTracker.
						GetTask(taskID).
						GetCurrentState().
						String(),
					"Task": taskID.Value,
				}).Info("Set Placement for task")
				if h.rmTracker.GetTask(taskID).GetCurrentState() == t.TaskState_PLACED {
					notValidTasks = append(notValidTasks, taskID)
				} else {
					err := h.rmTracker.GetTask(taskID).
						TransitTo(t.TaskState_PLACED.String())
					if err != nil {
						log.WithError(
							errors.WithStack(err)).
							Error("Not able " +
								"to transition to placed " +
								"for task " + taskID.Value)
					}
				}
				log.WithFields(log.Fields{
					"Task":  taskID.Value,
					"State": h.rmTracker.GetTask(taskID).GetCurrentState().String(),
				}).Debug("Latest state in Set Placement")
			} else {
				notValidTasks = append(notValidTasks, taskID)
				log.WithFields(log.Fields{
					"Task": taskID.Value,
				}).Debug("Task is not present in tracker, " +
					"Removing it from placement")
			}
		}
		newplacement := h.removeTasksFromPlacements(placement, notValidTasks)
		err = h.placements.Enqueue(newplacement)
		if err != nil {
			log.WithField("placement", newplacement).
				WithError(err).Error("Failed to enqueue placement")
			failed = append(
				failed,
				&resmgrsvc.SetPlacementsFailure_FailedPlacement{
					Placement: newplacement,
					Message:   err.Error(),
				},
			)
			h.metrics.SetPlacementFail.Inc(1)
		} else {
			h.metrics.SetPlacementSuccess.Inc(1)
		}
	}

	if len(failed) > 0 {
		return &resmgrsvc.SetPlacementsResponse{
			Error: &resmgrsvc.SetPlacementsResponse_Error{
				Failure: &resmgrsvc.SetPlacementsFailure{
					Failed: failed,
				},
			},
		}, nil
	}
	response := resmgrsvc.SetPlacementsResponse{}
	h.metrics.PlacementQueueLen.Update(float64(h.placements.Length()))
	log.Debug("Set Placement Returned")
	return &response, nil
}

func (h *ServiceHandler) removeTasksFromPlacements(
	placement *resmgr.Placement,
	tasks []*peloton.TaskID,
) *resmgr.Placement {
	if tasks == nil || len(tasks) == 0 {
		return placement
	}
	var newTasks []*peloton.TaskID
	log.WithFields(log.Fields{
		"Removed Tasks":  tasks,
		"Original Tasks": placement.GetTasks(),
	}).Debug("Removing Tasks")

	for _, pt := range placement.GetTasks() {
		match := false
		for _, t := range tasks {
			if pt.Value == t.Value {
				match = true
			}
		}
		if !match {
			newTasks = append(newTasks, pt)
		}
	}
	placement.Tasks = newTasks
	return placement
}

// GetPlacements implements ResourceManagerService.GetPlacements
func (h *ServiceHandler) GetPlacements(
	ctx context.Context,
	req *resmgrsvc.GetPlacementsRequest,
) (*resmgrsvc.GetPlacementsResponse, error) {

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

	return &response, nil
}

// NotifyTaskUpdates is called by HM to notify task updates
func (h *ServiceHandler) NotifyTaskUpdates(
	ctx context.Context,
	req *resmgrsvc.NotifyTaskUpdatesRequest) (*resmgrsvc.NotifyTaskUpdatesResponse, error) {
	var response resmgrsvc.NotifyTaskUpdatesResponse

	if len(req.Events) > 0 {
		for _, event := range req.Events {
			taskState := util.MesosStateToPelotonState(
				event.MesosTaskStatus.GetState())
			if !util.IsPelotonStateTerminal(taskState) {
				continue
			}

			ptID, err := util.ParseTaskIDFromMesosTaskID(
				*(event.MesosTaskStatus.TaskId.Value))
			if err != nil {
				log.WithField("event", event).Error("Could not parse mesos ID")
				continue
			}
			// TODO: We probably want to terminate all the tasks in gang
			err = rmtask.GetTracker().MarkItDone(&peloton.TaskID{
				Value: ptID,
			})

			if err != nil {
				log.WithField("event", event).Error("Could not be updated")
			}

			log.WithField("Offset", event.Offset).
				Debug("Event received by resource manager")
			if event.Offset > atomic.LoadUint64(h.maxOffset) {
				atomic.StoreUint64(h.maxOffset, event.Offset)
			}
		}
		response.PurgeOffset = atomic.LoadUint64(h.maxOffset)
	} else {
		log.Warn("Empty events received by resource manager")
	}
	return &response, nil
}
