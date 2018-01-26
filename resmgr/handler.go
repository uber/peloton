package resmgr

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	t "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/resmgr/preemption"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/scalar"
	rmtask "code.uber.internal/infra/peloton/resmgr/task"
	"code.uber.internal/infra/peloton/util"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	errFailingGangMemberTask    = errors.New("task fail because other gang member failed")
	errSameTaskPresent          = errors.New("same task present in tracker, Ignoring new task")
	errGangNotEnqueued          = errors.New("Could not enqueue gang to ready after retry")
	errEnqueuedAgain            = errors.New("enqueued again after retry")
	errUnplacedTaskInWrongState = errors.New("unplaced task should be in state placing")
)

// ServiceHandler implements peloton.private.resmgr.ResourceManagerService
// TODO: add placing and placed task queues
type ServiceHandler struct {
	metrics            *Metrics
	resPoolTree        respool.Tree
	placements         queue.Queue
	eventStreamHandler *eventstream.Handler
	rmTracker          rmtask.Tracker
	preemptor          preemption.Preemptor
	maxOffset          *uint64
	config             Config
}

// InitServiceHandler initializes the handler for ResourceManagerService
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	rmTracker rmtask.Tracker,
	preemptor preemption.Preemptor,
	conf Config) *ServiceHandler {

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
		preemptor: preemptor,
		maxOffset: &maxOffset,
		config:    conf,
	}
	// TODO: move eventStreamHandler buffer size into config
	handler.eventStreamHandler = initEventStreamHandler(d, 1000, parent.SubScope("resmgr"))

	d.Register(resmgrsvc.BuildResourceManagerServiceYARPCProcedures(handler))
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

	d.Register(pb_eventstream.BuildEventStreamServiceYARPCProcedures(eventStreamHandler))

	return eventStreamHandler
}

// GetStreamHandler returns the stream handler
func (h *ServiceHandler) GetStreamHandler() *eventstream.Handler {
	return h.eventStreamHandler
}

func (h *ServiceHandler) returnExistingTasks(gang *resmgrsvc.Gang) (
	[]*resmgrsvc.EnqueueGangsFailure_FailedTask, error) {
	allTasksExist := true
	for _, task := range gang.GetTasks() {
		task := h.rmTracker.GetTask(task.GetId())
		if task == nil {
			allTasksExist = false
			break
		}
	}
	if allTasksExist {
		var failed []*resmgrsvc.EnqueueGangsFailure_FailedTask
		for _, task := range gang.GetTasks() {
			err := h.requeueUnplacedTask(task)
			if err != nil {
				failed = append(
					failed,
					&resmgrsvc.EnqueueGangsFailure_FailedTask{
						Task:    task,
						Message: err.Error(),
					},
				)
			}
		}
		var err error
		if len(failed) > 0 {
			err = fmt.Errorf("some tasks failed to be re-enqueued")
		}
		return failed, err
	}
	return nil, fmt.Errorf("not all tasks for the gang exists in the resource manager")
}

// EnqueueGangs implements ResourceManagerService.EnqueueGangs
func (h *ServiceHandler) EnqueueGangs(
	ctx context.Context,
	req *resmgrsvc.EnqueueGangsRequest,
) (*resmgrsvc.EnqueueGangsResponse, error) {

	log.WithField("request", req).Info("EnqueueGangs called.")
	h.metrics.APIEnqueueGangs.Inc(1)

	// Lookup respool from the resource pool tree
	var err error
	var resourcePool respool.ResPool
	respoolID := req.GetResPool()
	if respoolID != nil {
		resourcePool, err = respool.GetTree().Get(respoolID)
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
	}

	// Enqueue the gangs sent in an API call to the pending queue of the respool.
	// For each gang, add its tasks to the state machine, enqueue the gang, and
	// return per-task success/failure.
	var failed []*resmgrsvc.EnqueueGangsFailure_FailedTask
	for _, gang := range req.GetGangs() {
		if resourcePool == nil {
			failed, err = h.returnExistingTasks(gang)
		} else {
			failed, err = h.enqueueGang(gang, resourcePool)
		}
		// Report per-task success/failure for all tasks in gang
		for _, task := range gang.GetTasks() {
			if err != nil {
				failed = append(
					failed,
					&resmgrsvc.EnqueueGangsFailure_FailedTask{
						Task:    task,
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

func (h *ServiceHandler) enqueueGang(
	gang *resmgrsvc.Gang,
	respool respool.ResPool) (
	[]*resmgrsvc.EnqueueGangsFailure_FailedTask,
	error) {
	totalGangResources := &scalar.Resources{}
	var failed []*resmgrsvc.EnqueueGangsFailure_FailedTask
	var err error
	for _, task := range gang.GetTasks() {
		err := h.requeueTask(task)
		if err != nil {
			failed = append(
				failed,
				&resmgrsvc.EnqueueGangsFailure_FailedTask{
					Task:    task,
					Message: err.Error(),
				},
			)
			continue
		}

		// Adding task to state machine
		err = h.rmTracker.AddTask(
			task,
			h.eventStreamHandler,
			respool,
			h.config.RmTaskConfig,
		)
		if err != nil {
			failed = append(
				failed,
				&resmgrsvc.EnqueueGangsFailure_FailedTask{
					Task:    task,
					Message: err.Error(),
				},
			)
			continue
		}
		totalGangResources = totalGangResources.Add(
			scalar.ConvertToResmgrResource(
				task.GetResource()))

		if h.rmTracker.GetTask(task.Id) != nil {
			err = h.rmTracker.GetTask(task.Id).TransitTo(
				t.TaskState_PENDING.String())
			if err != nil {
				log.Error(err)
			}
		}
	}
	if len(failed) == 0 {
		err = respool.EnqueueGang(gang)
		if err == nil {
			err = respool.AddToDemand(totalGangResources)
			log.WithFields(log.Fields{
				"respool_ID":           respool.ID(),
				"total_gang_resources": totalGangResources,
			}).Debug("Resources added for Gang")
			if err != nil {
				log.Error(err)
			}
		} else {
			// We need to remove gang tasks from tracker
			h.removeTasksFromTracker(gang)
		}
	} else {
		err = errFailingGangMemberTask
	}
	return failed, err
}

// removeTasksFromTracker removes the  task from the tracker
func (h *ServiceHandler) removeTasksFromTracker(gang *resmgrsvc.Gang) {
	for _, task := range gang.Tasks {
		h.rmTracker.DeleteTask(task.Id)
	}
}

func (h *ServiceHandler) requeueUnplacedTask(requeuedTask *resmgr.Task) error {
	rmTask := h.rmTracker.GetTask(requeuedTask.Id)
	if rmTask == nil {
		return nil
	}
	currentTaskState := rmTask.GetCurrentState()
	if currentTaskState == t.TaskState_READY {
		return nil
	}
	if currentTaskState == t.TaskState_PLACING {
		// Transitioning back to Ready State
		rmTask.TransitTo(t.TaskState_READY.String())
		// Adding to ready Queue
		var tasks []*resmgr.Task
		gang := &resmgrsvc.Gang{
			Tasks: append(tasks, rmTask.Task()),
		}
		err := rmtask.GetScheduler().EnqueueGang(gang)
		if err != nil {
			log.WithField("gang", gang).Error(errGangNotEnqueued.Error())
			return err
		}
		return nil
	}
	return errUnplacedTaskInWrongState
}

// requeueTask validates the enqueued task has the same mesos task id or not
// If task has same mesos task id => return error
// If task has different mesos task id then check state and based on the state
// act accordingly
func (h *ServiceHandler) requeueTask(requeuedTask *resmgr.Task) error {
	rmTask := h.rmTracker.GetTask(requeuedTask.Id)
	if rmTask == nil {
		return nil
	}

	if *requeuedTask.TaskId.Value == *rmTask.Task().TaskId.Value {
		return errSameTaskPresent
	}

	currentTaskState := rmTask.GetCurrentState()

	// If state is Launching, Launched or Running then only
	// put task to ready queue with update of
	// mesos task id otherwise ignore
	if currentTaskState == t.TaskState_LAUNCHING ||
		currentTaskState == t.TaskState_LAUNCHED ||
		currentTaskState == t.TaskState_RUNNING {
		// Updating the New Mesos Task ID
		rmTask.Task().TaskId = requeuedTask.TaskId
		// Transitioning back to Ready State
		rmTask.TransitTo(t.TaskState_READY.String())
		// Adding to ready Queue
		var tasks []*resmgr.Task
		gang := &resmgrsvc.Gang{
			Tasks: append(tasks, rmTask.Task()),
		}
		err := rmtask.GetScheduler().EnqueueGang(gang)
		if err != nil {
			log.WithField("gang", gang).Error(errGangNotEnqueued.Error())
			return err
		}
		log.WithField("gang", gang).Debug(errEnqueuedAgain.Error())
		return errEnqueuedAgain
	}
	return errSameTaskPresent
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
		tasksToRemove := make(map[string]*resmgr.Task)
		for _, task := range gang.GetTasks() {
			h.metrics.DequeueGangSuccess.Inc(1)

			// Moving task to Placing state
			if h.rmTracker.GetTask(task.Id) != nil {
				err = h.rmTracker.GetTask(task.Id).TransitTo(
					t.TaskState_PLACING.String())
				if err != nil {
					log.WithError(err).WithField(
						"task_ID", task.Id.Value).
						Error("Failed to transit state " +
							"for task")
				}
			} else {
				tasksToRemove[task.Id.Value] = task
			}
		}
		gang = h.removeFromGang(gang, tasksToRemove)
		gangs = append(gangs, gang)
	}
	// TODO: handle the dequeue errors better
	response := resmgrsvc.DequeueGangsResponse{Gangs: gangs}
	log.WithField("response", response).Debug("DequeueGangs succeeded")
	return &response, nil
}

func (h *ServiceHandler) removeFromGang(
	gang *resmgrsvc.Gang,
	tasksToRemove map[string]*resmgr.Task) *resmgrsvc.Gang {
	if len(tasksToRemove) == 0 {
		return gang
	}
	var newTasks []*resmgr.Task
	for _, gt := range gang.GetTasks() {
		if _, ok := tasksToRemove[gt.Id.Value]; !ok {
			newTasks = append(newTasks, gt)
		}
	}
	gang.Tasks = newTasks
	return gang
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
		newplacement := h.transitTasksInPlacement(placement,
			t.TaskState_PLACING,
			t.TaskState_PLACED)
		h.rmTracker.SetPlacementHost(newplacement, newplacement.Hostname)
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

// GetTasksByHosts returns all tasks of the given task type running on the given list of hosts.
func (h *ServiceHandler) GetTasksByHosts(ctx context.Context,
	req *resmgrsvc.GetTasksByHostsRequest) (*resmgrsvc.GetTasksByHostsResponse, error) {
	hostTasksMap := map[string]*resmgrsvc.TaskList{}
	for hostname, tasks := range h.rmTracker.TasksByHosts(req.Hostnames, req.Type) {
		if _, exists := hostTasksMap[hostname]; !exists {
			hostTasksMap[hostname] = &resmgrsvc.TaskList{
				Tasks: make([]*resmgr.Task, 0, len(tasks)),
			}
		}
		for _, task := range tasks {
			hostTasksMap[hostname].Tasks = append(hostTasksMap[hostname].Tasks, task.Task())
		}
	}
	res := &resmgrsvc.GetTasksByHostsResponse{
		HostTasksMap: hostTasksMap,
	}
	return res, nil
}

func (h *ServiceHandler) removeTasksFromPlacements(
	placement *resmgr.Placement,
	tasks map[string]*peloton.TaskID,
) *resmgr.Placement {
	if len(tasks) == 0 {
		return placement
	}
	var newTasks []*peloton.TaskID

	log.WithFields(log.Fields{
		"tasks_to_remove": tasks,
		"orig_tasks":      placement.GetTasks(),
	}).Debug("Removing Tasks")

	for _, pt := range placement.GetTasks() {
		if _, ok := tasks[pt.Value]; !ok {
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
		newPlacement := h.transitTasksInPlacement(placement,
			t.TaskState_PLACED,
			t.TaskState_LAUNCHING)
		placements = append(placements, newPlacement)
		h.metrics.GetPlacementSuccess.Inc(1)
	}

	response := resmgrsvc.GetPlacementsResponse{Placements: placements}
	h.metrics.PlacementQueueLen.Update(float64(h.placements.Length()))
	log.Debug("Get Placement Returned")

	return &response, nil
}

// transitTasksInPlacement transition to Launching upon getplacement
// or remove tasks from placement which are not in placed state.
func (h *ServiceHandler) transitTasksInPlacement(
	placement *resmgr.Placement,
	expectedState t.TaskState,
	newState t.TaskState) *resmgr.Placement {
	invalidTasks := make(map[string]*peloton.TaskID)
	for _, taskID := range placement.Tasks {
		rmTask := h.rmTracker.GetTask(taskID)
		if rmTask == nil {
			invalidTasks[taskID.Value] = taskID
			log.WithFields(log.Fields{
				"task_ID": taskID.Value,
			}).Debug("Task is not present in tracker, " +
				"Removing it from placement")
			continue
		}
		state := rmTask.GetCurrentState()
		log.WithFields(log.Fields{
			"task_ID":       taskID.Value,
			"current_state": state.String(),
		}).Debug("Get Placement for task")
		if state != expectedState {
			log.WithFields(log.Fields{
				"task_id":        taskID.GetValue(),
				"expected_state": expectedState.String(),
				"actual_state":   state.String(),
			}).Error("Unable to transit tasks in placement: " +
				"task is not in expected state")
			invalidTasks[taskID.Value] = taskID

		} else {
			err := rmTask.TransitTo(newState.String())
			if err != nil {
				log.WithError(errors.WithStack(err)).
					WithField("task_ID", taskID.GetValue()).
					Info("not able to transition to launching for task")
				invalidTasks[taskID.Value] = taskID
			}
		}
		log.WithFields(log.Fields{
			"task_ID":       taskID.Value,
			"current_state": state.String(),
		}).Debug("Latest state in Get Placement")
	}
	return h.removeTasksFromPlacements(placement, invalidTasks)
}

// NotifyTaskUpdates is called by HM to notify task updates
func (h *ServiceHandler) NotifyTaskUpdates(
	ctx context.Context,
	req *resmgrsvc.NotifyTaskUpdatesRequest) (*resmgrsvc.NotifyTaskUpdatesResponse, error) {
	var response resmgrsvc.NotifyTaskUpdatesResponse

	if len(req.Events) == 0 {
		log.Warn("Empty events received by resource manager")
		return &response, nil
	}

	for _, event := range req.Events {
		taskState := util.MesosStateToPelotonState(
			event.MesosTaskStatus.GetState())
		if taskState != t.TaskState_RUNNING &&
			!util.IsPelotonStateTerminal(taskState) {
			h.acknowledgeEvent(event.Offset)
			continue
		}
		ptID, err := util.ParseTaskIDFromMesosTaskID(
			*(event.MesosTaskStatus.TaskId.Value))
		if err != nil {
			log.WithFields(log.Fields{
				"event":         event,
				"mesos_task_ID": *(event.MesosTaskStatus.TaskId.Value),
			}).Error("Could not parse mesos ID")
			h.acknowledgeEvent(event.Offset)
			continue
		}
		taskID := &peloton.TaskID{
			Value: ptID,
		}
		rmTask := h.rmTracker.GetTask(taskID)
		if rmTask == nil {
			h.acknowledgeEvent(event.Offset)
			continue
		}

		if *(rmTask.Task().TaskId.Value) !=
			*(event.MesosTaskStatus.TaskId.Value) {
			log.WithFields(log.Fields{
				"task_ID": rmTask.Task().TaskId.Value,
				"event":   event,
			}).Error("could not be updated due to" +
				"different mesos taskID")
			h.acknowledgeEvent(event.Offset)
			continue
		}
		if taskState == t.TaskState_RUNNING {
			err = rmTask.TransitTo(t.TaskState_RUNNING.String())
			if err != nil {
				log.WithError(errors.WithStack(err)).
					WithField("task_ID", taskID.Value).
					Info("Not able to transition to RUNNING for task")
			}
			// update the start time
			rmTask.UpdateStartTime(time.Now().UTC())
		} else {
			// TODO: We probably want to terminate all the tasks in gang
			err = rmtask.GetTracker().MarkItDone(taskID)
			if err != nil {
				log.WithField("event", event).Error("Could not be updated")
			}
			log.WithFields(log.Fields{
				"task_ID":       taskID.Value,
				"current_state": taskState.String(),
			}).Info("Task is completed and removed from tracker")
			rmtask.GetTracker().UpdateCounters(
				t.TaskState_RUNNING.String(), taskState.String())
		}
		h.acknowledgeEvent(event.Offset)
	}
	response.PurgeOffset = atomic.LoadUint64(h.maxOffset)
	return &response, nil
}

func (h *ServiceHandler) acknowledgeEvent(offset uint64) {
	log.WithField("offset", offset).
		Debug("Event received by resource manager")
	if offset > atomic.LoadUint64(h.maxOffset) {
		atomic.StoreUint64(h.maxOffset, offset)
	}
}

// GetActiveTasks returns task to state map
func (h *ServiceHandler) GetActiveTasks(
	ctx context.Context,
	req *resmgrsvc.GetActiveTasksRequest,
) (*resmgrsvc.GetActiveTasksResponse, error) {
	taskStates := h.rmTracker.GetActiveTasks(req.GetJobID(), req.GetRespoolID())
	return &resmgrsvc.GetActiveTasksResponse{TaskStatesMap: taskStates}, nil
}

// GetPendingTasks returns the pending tasks from a resource pool in the
// order in which they were added up to a max limit number of gangs.
// Eg specifying a limit of 10 would return pending tasks from the first 10
// gangs in the queue.
// The tasks are grouped according to their gang membership since one gang
// can contain multiple tasks.
func (h *ServiceHandler) GetPendingTasks(
	ctx context.Context,
	req *resmgrsvc.GetPendingTasksRequest,
) (*resmgrsvc.GetPendingTasksResponse, error) {

	respoolID := req.GetRespoolID()
	limit := req.GetLimit()

	log.WithField("respool_id", respoolID).
		WithField("limit", limit).
		Info("GetPendingTasks called")

	if respoolID == nil {
		return &resmgrsvc.GetPendingTasksResponse{},
			status.Errorf(codes.InvalidArgument,
				"Resource pool ID can't be nil")
	}

	node, err := h.resPoolTree.Get(&peloton.ResourcePoolID{
		Value: respoolID.GetValue()})
	if err != nil {
		return &resmgrsvc.GetPendingTasksResponse{},
			status.Errorf(codes.NotFound,
				"Resource pool ID not found:%s", respoolID)
	}

	if !node.IsLeaf() {
		return &resmgrsvc.GetPendingTasksResponse{},
			status.Errorf(codes.InvalidArgument,
				"Resource pool:%s is not a leaf node", respoolID)
	}

	tasks, err := h.getPendingTasks(node, limit)
	if err != nil {
		return &resmgrsvc.GetPendingTasksResponse{},
			status.Errorf(codes.Internal,
				"Failed to return pending tasks, err:%s", err.Error())
	}

	log.WithField("respool_id", respoolID).
		WithField("limit", limit).
		Debug("GetPendingTasks returned")

	return &resmgrsvc.GetPendingTasksResponse{
		Tasks: tasks,
	}, nil
}

func (h *ServiceHandler) getPendingTasks(node respool.ResPool,
	limit uint32) ([]*resmgrsvc.GetPendingTasksResponse_TaskList,
	error) {
	var tasks []*resmgrsvc.GetPendingTasksResponse_TaskList

	gangs, err := node.PeekPendingGangs(limit)
	if err != nil {
		return tasks, errors.Wrap(err, "failed to peek pending gangs")
	}

	for _, gang := range gangs {
		var taskIDs []string
		for _, task := range gang.GetTasks() {
			taskIDs = append(taskIDs, task.GetId().GetValue())
		}
		tasks = append(tasks, &resmgrsvc.GetPendingTasksResponse_TaskList{
			TaskID: taskIDs,
		})
	}
	return tasks, nil
}

// KillTasks kills the task
func (h *ServiceHandler) KillTasks(
	ctx context.Context,
	req *resmgrsvc.KillTasksRequest,
) (*resmgrsvc.KillTasksResponse, error) {
	listTasks := req.GetTasks()
	if len(listTasks) == 0 {
		return &resmgrsvc.KillTasksResponse{
			Error: []*resmgrsvc.KillTasksResponse_Error{
				{
					NotFound: &resmgrsvc.TasksNotFound{
						Message: "Kill tasks called with no tasks",
					},
				},
			},
		}, nil
	}

	log.WithField("tasks", listTasks).Info("tasks to be killed")

	var tasksNotFound []*peloton.TaskID
	var tasksNotKilled []*peloton.TaskID
	for _, taskTobeKilled := range listTasks {
		killedRmTask := h.rmTracker.GetTask(taskTobeKilled)

		if killedRmTask == nil {
			tasksNotFound = append(tasksNotFound, taskTobeKilled)
			continue
		}

		err := h.rmTracker.MarkItInvalid(taskTobeKilled)
		if err != nil {
			tasksNotKilled = append(tasksNotKilled, taskTobeKilled)
			continue
		}
		log.WithFields(log.Fields{
			"task_ID":       taskTobeKilled.Value,
			"current_state": killedRmTask.GetCurrentState().String(),
		}).Info("Task is Killed and removed from tracker")
		h.rmTracker.UpdateCounters(
			killedRmTask.GetCurrentState().String(),
			t.TaskState_KILLED.String(),
		)
	}
	if len(tasksNotKilled) == 0 && len(tasksNotFound) == 0 {
		return &resmgrsvc.KillTasksResponse{}, nil
	}

	var killResponseErr []*resmgrsvc.KillTasksResponse_Error

	if len(tasksNotFound) != 0 {
		log.WithField("tasks", tasksNotFound).Error("tasks can't be found")
		for _, t := range tasksNotFound {
			killResponseErr = append(killResponseErr,
				&resmgrsvc.KillTasksResponse_Error{
					NotFound: &resmgrsvc.TasksNotFound{
						Message: "Tasks Not Found",
						Task:    t,
					},
				})
		}
	} else {
		log.WithField("tasks", tasksNotKilled).Error("tasks can't be killed")
		for _, t := range tasksNotKilled {
			killResponseErr = append(killResponseErr,
				&resmgrsvc.KillTasksResponse_Error{
					KillError: &resmgrsvc.KillTasksError{
						Message: "Tasks can't be killed",
						Task:    t,
					},
				})
		}
	}

	return &resmgrsvc.KillTasksResponse{
		Error: killResponseErr,
	}, nil
}

// GetPreemptibleTasks returns tasks which need to be preempted from the resource pool
func (h *ServiceHandler) GetPreemptibleTasks(
	ctx context.Context,
	req *resmgrsvc.GetPreemptibleTasksRequest) (*resmgrsvc.GetPreemptibleTasksResponse, error) {

	log.WithField("request", req).Debug("GetPreemptibleTasks called.")
	h.metrics.APIGetPreemptibleTasks.Inc(1)

	limit := req.GetLimit()
	timeout := time.Duration(req.GetTimeout())
	var tasks []*resmgr.Task
	for i := 0; i < int(limit); i++ {
		task, err := h.preemptor.DequeueTask(timeout * time.Millisecond)
		if err != nil {
			// no more tasks
			h.metrics.GetPreemptibleTasksTimeout.Inc(1)
			break
		}

		// Transit task state machine to PREEMPTING
		if rmTask := h.rmTracker.GetTask(task.Id); rmTask != nil {
			err = rmTask.TransitTo(
				t.TaskState_PREEMPTING.String())
			if err != nil {
				// the task could have moved from RUNNING state
				log.WithError(err).
					WithField("task_ID", task.Id.Value).
					Error("failed to transit state for task")
				continue
			}
		} else {
			log.WithError(err).
				WithField("task_ID", task.Id.Value).
				Error("failed to find task in the tracker")
			continue
		}
		tasks = append(tasks, task)
	}

	log.WithField("preemptible_tasks", tasks).
		Info("GetPreemptibleTasks returned")
	h.metrics.GetPreemptibleTasksSuccess.Inc(1)
	return &resmgrsvc.GetPreemptibleTasksResponse{
		Tasks: tasks,
	}, nil
}

// MarkTasksLaunched is used to notify the resource manager that tasks have been launched
func (h *ServiceHandler) MarkTasksLaunched(
	ctx context.Context,
	req *resmgrsvc.MarkTasksLaunchedRequest) (*resmgrsvc.MarkTasksLaunchedResponse, error) {

	launchedTasks := req.GetTasks()
	if len(launchedTasks) == 0 {
		return &resmgrsvc.MarkTasksLaunchedResponse{}, nil
	}

	log.WithField("launched_tasks", launchedTasks).Debug("tasks launched called")
	h.metrics.APILaunchedTasks.Inc(1)

	for _, taskID := range launchedTasks {
		task := h.rmTracker.GetTask(taskID)
		if task == nil {
			continue
		}

		err := task.TransitTo(t.TaskState_LAUNCHED.String())
		if err != nil {
			log.WithError(err).
				WithField("task_id", taskID).
				Info("failed to transit to launched state")
			continue
		}
	}
	return &resmgrsvc.MarkTasksLaunchedResponse{}, nil
}
