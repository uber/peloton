// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resmgr

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	t "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pb_eventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	"github.com/uber/peloton/pkg/resmgr/hostmover"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/eventstream"
	"github.com/uber/peloton/pkg/common/queue"
	"github.com/uber/peloton/pkg/common/statemachine"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/resmgr/preemption"
	r_queue "github.com/uber/peloton/pkg/resmgr/queue"
	"github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/scalar"
	rmtask "github.com/uber/peloton/pkg/resmgr/task"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	errFailingGangMemberTask = errors.New("task fail because other gang member failed")
	errSameTaskPresent       = errors.New("same task present in tracker, Ignoring new task")
	errGangNotEnqueued       = errors.New("could not enqueue gang to ready after retry")
	errEnqueuedAgain         = errors.New("enqueued again after retry")
	errRequeueTaskFailed     = errors.New("requeue existing task to resmgr failed")
)

const (
	_reasonPlacementReceived = "placement received"
	_reasonDequeuedForLaunch = "placement dequeued, waiting for launch"
)

const _eventStreamBufferSize = 1000

// ServiceHandler implements peloton.private.resmgr.ResourceManagerService
type ServiceHandler struct {
	// the handler config
	config Config

	// metrics and scope
	scope   tally.Scope
	metrics *Metrics

	// queues for preempting and placing tasks
	preemptionQueue preemption.Queue
	placements      queue.Queue

	// handler for host manager event stream
	maxOffset          *uint64
	eventStreamHandler *eventstream.Handler

	// rmtasks tracker
	rmTracker rmtask.Tracker

	// batch host scorer
	batchScorer hostmover.Scorer

	// in-memory resource pool tree
	resPoolTree respool.Tree

	hostmgrClient hostsvc.InternalHostServiceYARPCClient
}

// NewServiceHandler initializes the handler for ResourceManagerService
func NewServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	rmTracker rmtask.Tracker,
	batchScorer hostmover.Scorer,
	tree respool.Tree,
	preemptionQueue preemption.Queue,
	hostmgrClient hostsvc.InternalHostServiceYARPCClient,
	conf Config) *ServiceHandler {

	var maxOffset uint64
	handler := &ServiceHandler{
		metrics:     NewMetrics(parent.SubScope("resmgr")),
		resPoolTree: tree,
		placements: queue.NewQueue(
			"placement-queue",
			reflect.TypeOf(resmgr.Placement{}),
			maxPlacementQueueSize,
		),
		rmTracker:       rmTracker,
		batchScorer:     batchScorer,
		preemptionQueue: preemptionQueue,
		maxOffset:       &maxOffset,
		config:          conf,
		scope:           parent,
		eventStreamHandler: initEventStreamHandler(
			d,
			_eventStreamBufferSize,
			parent.SubScope("resmgr")),
		hostmgrClient: hostmgrClient,
	}

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

// EnqueueGangs implements ResourceManagerService.EnqueueGangs
func (h *ServiceHandler) EnqueueGangs(
	ctx context.Context,
	req *resmgrsvc.EnqueueGangsRequest,
) (*resmgrsvc.EnqueueGangsResponse, error) {

	log.WithField("request", req).Info("EnqueueGangs called.")
	h.metrics.APIEnqueueGangs.Inc(1)

	var err error
	var resourcePool respool.ResPool
	respoolID := req.GetResPool()

	if respoolID == nil {
		return &resmgrsvc.EnqueueGangsResponse{
			Error: &resmgrsvc.EnqueueGangsResponse_Error{
				NotFound: &resmgrsvc.ResourcePoolNotFound{
					Id:      respoolID,
					Message: "resource pool ID can't be nil",
				},
			},
		}, nil
	}

	// Lookup respool from the resource pool tree
	resourcePool, err = h.resPoolTree.Get(respoolID)
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

	var failedGangs []*resmgrsvc.EnqueueGangsFailure_FailedTask
	// Enqueue the gangs sent in an API call to the pending queue of the respool.
	// For each gang, add its tasks to the state machine, enqueue the gang, and
	// return per-task success/failure.
	for _, gang := range req.GetGangs() {
		failedGang, err := h.enqueueGang(gang, resourcePool)
		if err != nil {
			failedGangs = append(failedGangs, failedGang...)
			h.metrics.EnqueueGangFail.Inc(1)
			continue
		}
		h.metrics.EnqueueGangSuccess.Inc(1)
	}

	// Even if one gang fails we return as error.
	if len(failedGangs) > 0 {
		return &resmgrsvc.EnqueueGangsResponse{
			Error: &resmgrsvc.EnqueueGangsResponse_Error{
				Failure: &resmgrsvc.EnqueueGangsFailure{
					Failed: failedGangs,
				},
			},
		}, nil
	}

	log.Debug("Enqueue Returned")
	return &resmgrsvc.EnqueueGangsResponse{}, nil
}

// enqueueGang adds the new gangs to pending queue or
// requeue the gang if the tasks have different mesos
// taskid.
func (h *ServiceHandler) enqueueGang(
	gang *resmgrsvc.Gang,
	respool respool.ResPool) (
	[]*resmgrsvc.EnqueueGangsFailure_FailedTask,
	error) {
	var failed []*resmgrsvc.EnqueueGangsFailure_FailedTask
	var failedTask *resmgrsvc.EnqueueGangsFailure_FailedTask
	var err error
	failedTasks := make(map[string]bool)
	for _, task := range gang.GetTasks() {
		if !(h.isTaskPresent(task)) {
			// If the task is not present in the tracker
			// this means its a new task and needs to be
			// added to tracker
			failedTask, err = h.addTask(task, respool)
		} else {
			// This is the already present task,
			// We need to check if it has same mesos
			// id or different mesos task id.
			failedTask, err = h.requeueTask(task, respool)
		}

		// If there is any failure we need to add those tasks to
		// failed list of task by that we can remove the gang later
		if err != nil {
			failed = append(failed, failedTask)
			failedTasks[task.Id.Value] = true
		}
	}

	if len(failed) == 0 {
		err = h.addingGangToPendingQueue(gang, respool)
		// if there is error , we need to mark all tasks in gang failed.
		if err != nil {
			failed = append(failed, h.markingTasksFailInGang(gang, failedTasks, errFailingGangMemberTask)...)
			err = errGangNotEnqueued
		}
		return failed, err
	}
	// we need to fail the other tasks which are not failed
	// as we have to enqueue whole gang or not
	// here we are assuming that all the tasks in gang whether
	// be enqueued or requeued.
	failed = append(failed, h.markingTasksFailInGang(gang, failedTasks, errFailingGangMemberTask)...)
	err = errGangNotEnqueued

	return failed, err
}

// isTaskPresent checks if the task is present in the tracker, Returns
// True if present otherwise False
func (h *ServiceHandler) isTaskPresent(requeuedTask *resmgr.Task) bool {
	return h.rmTracker.GetTask(requeuedTask.Id) != nil
}

// removeGangFromTracker removes the  task from the tracker
func (h *ServiceHandler) removeGangFromTracker(gang *resmgrsvc.Gang) {
	for _, task := range gang.Tasks {
		h.rmTracker.DeleteTask(task.Id)
	}
}

// addingGangToPendingQueue transit all tasks of gang to PENDING state
// and add them to pending queue by that they can be scheduled for
// next scheduling cycle
func (h *ServiceHandler) addingGangToPendingQueue(
	gang *resmgrsvc.Gang,
	respool respool.ResPool) error {
	for _, task := range gang.GetTasks() {
		if h.rmTracker.GetTask(task.Id) != nil {
			// transiting the task from INITIALIZED State to PENDING State
			err := h.rmTracker.GetTask(task.Id).TransitTo(
				t.TaskState_PENDING.String(), statemachine.WithReason("enqueue gangs called"),
				statemachine.WithInfo(mesosTaskID, *task.GetTaskId().Value))
			if err != nil {
				log.WithError(err).WithField("task", task.Id.Value).
					Error("not able to transit task to PENDING")

				// Removing the gang from the tracker if some tasks fail to transit
				h.removeGangFromTracker(gang)
				return errGangNotEnqueued
			}
		}
	}

	// Adding gang to pending queue
	if err := respool.EnqueueGang(gang); err != nil {
		// We need to remove gang tasks from tracker
		h.removeGangFromTracker(gang)
		return errGangNotEnqueued
	}

	return nil
}

// markingTasksFailInGang marks all other tasks as failed which are not
// part of failedTasks map and returns the failed list.
func (h *ServiceHandler) markingTasksFailInGang(gang *resmgrsvc.Gang,
	failedTasks map[string]bool,
	err error,
) []*resmgrsvc.EnqueueGangsFailure_FailedTask {
	var failed []*resmgrsvc.EnqueueGangsFailure_FailedTask
	for _, task := range gang.GetTasks() {
		if _, ok := failedTasks[task.Id.Value]; !ok {
			failed = append(failed,
				&resmgrsvc.EnqueueGangsFailure_FailedTask{
					Task:      task,
					Message:   err.Error(),
					Errorcode: resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_FAILED_DUE_TO_GANG_FAILED,
				})
		}
	}
	return failed
}

// addTask adds the task to RMTracker based on the respool
func (h *ServiceHandler) addTask(newTask *resmgr.Task, respool respool.ResPool,
) (*resmgrsvc.EnqueueGangsFailure_FailedTask, error) {
	// Adding task to state machine
	err := h.rmTracker.AddTask(
		newTask,
		h.eventStreamHandler,
		respool,
		h.config.RmTaskConfig,
	)
	if err != nil {
		return &resmgrsvc.EnqueueGangsFailure_FailedTask{
			Task:      newTask,
			Message:   err.Error(),
			Errorcode: resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_INTERNAL,
		}, err
	}
	return nil, nil
}

// requeueTask validates the enqueued task has the same mesos task id or not
// If task has same mesos task id => return error
// If task has different mesos task id then check state and based on the state
// act accordingly
func (h *ServiceHandler) requeueTask(
	requeuedTask *resmgr.Task,
	respool respool.ResPool,
) (*resmgrsvc.EnqueueGangsFailure_FailedTask, error) {
	rmTask := h.rmTracker.GetTask(requeuedTask.GetId())
	if rmTask == nil {
		return &resmgrsvc.EnqueueGangsFailure_FailedTask{
			Task:      requeuedTask,
			Message:   errRequeueTaskFailed.Error(),
			Errorcode: resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_INTERNAL,
		}, errRequeueTaskFailed
	}
	if *requeuedTask.TaskId.Value == *rmTask.Task().TaskId.Value {
		return &resmgrsvc.EnqueueGangsFailure_FailedTask{
			Task:      requeuedTask,
			Message:   errSameTaskPresent.Error(),
			Errorcode: resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_ALREADY_EXIST,
		}, errSameTaskPresent
	}
	currentTaskState := rmTask.GetCurrentState().State

	// If state is Launched or Running
	// replace the task in the tracker and requeue
	if h.isTaskInTransitRunning(currentTaskState) {
		return h.addTask(requeuedTask, respool)
	}

	// TASK should not be in any other state other than RUNNING or LAUNCHED
	log.WithFields(log.Fields{
		"task":              rmTask.Task().Id.Value,
		"current_state":     currentTaskState.String(),
		"old_mesos_task_id": *rmTask.Task().TaskId.Value,
		"new_mesos_task_id": *requeuedTask.TaskId.Value,
	}).Error("task should not be requeued with different mesos taskid at this state")

	return &resmgrsvc.EnqueueGangsFailure_FailedTask{
		Task:      requeuedTask,
		Message:   errSameTaskPresent.Error(),
		Errorcode: resmgrsvc.EnqueueGangsFailure_ENQUEUE_GANGS_FAILURE_ERROR_CODE_INTERNAL,
	}, errRequeueTaskFailed

}

// isTaskInTransitRunning return TRUE if the task state is in
// RUNNING or LAUNCHED state else it returns FALSE
func (h *ServiceHandler) isTaskInTransitRunning(state t.TaskState) bool {
	if state == t.TaskState_LAUNCHED ||
		state == t.TaskState_RUNNING {
		return true
	}
	return false
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
			log.WithField("task_type", req.Type).
				Debug("Timeout to dequeue gang from ready queue")
			h.metrics.DequeueGangTimeout.Inc(1)
			break
		}
		tasksToRemove := make(map[string]*resmgr.Task)
		for _, task := range gang.GetTasks() {
			h.metrics.DequeueGangSuccess.Inc(1)

			// Moving task to Placing state or Reserved state
			if h.rmTracker.GetTask(task.Id) != nil {
				if task.ReadyForHostReservation {
					err = h.rmTracker.GetTask(task.Id).TransitTo(
						t.TaskState_RESERVED.String())
				} else {
					// Checking if placement backoff is enabled if yes add the
					// backoff otherwise just do the transition
					if h.config.RmTaskConfig.EnablePlacementBackoff {
						//Adding backoff
						h.rmTracker.GetTask(task.Id).AddBackoff()
					}
					err = h.rmTracker.GetTask(task.Id).TransitTo(
						t.TaskState_PLACING.String())
				}

				if err != nil {
					log.WithError(err).WithField(
						"task_id", task.Id.Value).
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

	// first go through all the successful placements
	for _, placement := range req.GetPlacements() {
		newPlacement := h.transitTasksInPlacement(
			placement,
			[]t.TaskState{
				t.TaskState_PLACING,
				t.TaskState_RESERVED,
			},
			t.TaskState_PLACED,
			_reasonPlacementReceived)

		h.rmTracker.SetPlacement(newPlacement)

		err := h.placements.Enqueue(newPlacement)
		if err == nil {
			h.metrics.SetPlacementSuccess.Inc(1)
			continue
		}

		// lets log the error and add the failed placement
		log.WithField("placement", newPlacement).
			WithError(err).
			Error("Failed to enqueue placement")
		failed = append(
			failed,
			&resmgrsvc.SetPlacementsFailure_FailedPlacement{
				Placement: newPlacement,
				Message:   err.Error(),
			},
		)
		h.metrics.SetPlacementFail.Inc(1)
	}

	// now we go through all the unsuccessful placements
	for _, failedPlacement := range req.GetFailedPlacements() {
		err := h.returnFailedPlacement(
			failedPlacement.GetGang(),
			failedPlacement.GetReason(),
		)
		if err != nil {
			log.WithField("placement", failedPlacement).
				WithError(err).
				Error("Failed to enqueue failed placements")
			h.metrics.SetPlacementFail.Inc(1)
		}
	}

	// if there are any failures
	if len(failed) > 0 {
		return &resmgrsvc.SetPlacementsResponse{
			Error: &resmgrsvc.SetPlacementsResponse_Error{
				Failure: &resmgrsvc.SetPlacementsFailure{
					Failed: failed,
				},
			},
		}, nil
	}

	h.metrics.PlacementQueueLen.Update(float64(h.placements.Length()))
	log.Debug("Set Placement Returned")
	return &resmgrsvc.SetPlacementsResponse{}, nil
}

// returnFailedPlacement returns a failed placement gang to the resource manager.
// The failed gangs will be tried again to be placed at a later time.
// The two possible paths from here is
// 1. Put this task again to Ready queue
// 2. Put this to Pending queue
// Paths will be decided based on how many attempts have already been made for placement
func (h *ServiceHandler) returnFailedPlacement(
	failedGang *resmgrsvc.Gang, reason string) error {
	errs := new(multierror.Error)
	for _, task := range failedGang.GetTasks() {
		rmTask := h.rmTracker.GetTask(task.Id)
		if rmTask == nil {
			// task could have been deleted
			continue
		}
		if err := rmTask.RequeueUnPlaced(reason); err != nil {
			errs = multierror.Append(errs, err)
		}
		h.metrics.PlacementFailed.Inc(1)
	}
	return errs.ErrorOrNil()
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
	taskSet map[string]struct{},
) *resmgr.Placement {
	if len(taskSet) == 0 {
		return placement
	}
	var newTaskIDs []*resmgr.Placement_Task

	log.WithFields(log.Fields{
		"tasks_to_remove": taskSet,
		"orig_tasks":      placement.GetTaskIDs(),
	}).Debug("Removing Tasks")

	for _, pt := range placement.GetTaskIDs() {
		if _, ok := taskSet[pt.GetMesosTaskID().GetValue()]; !ok {
			newTaskIDs = append(newTaskIDs, pt)
		}
	}
	placement.TaskIDs = newTaskIDs
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
			[]t.TaskState{
				t.TaskState_PLACED,
			},
			t.TaskState_LAUNCHING,
			_reasonDequeuedForLaunch)
		placements = append(placements, newPlacement)
		h.metrics.GetPlacementSuccess.Inc(1)
	}

	response := resmgrsvc.GetPlacementsResponse{Placements: placements}
	h.metrics.PlacementQueueLen.Update(float64(h.placements.Length()))
	log.Debug("Get Placement Returned")

	return &response, nil
}

// transitTasksInPlacement transitions tasks to new state if the current state
// matches the expected states. Those tasks which couldn't be transitioned are
// removed from the placement. The will tried to place again in the next cycle.
func (h *ServiceHandler) transitTasksInPlacement(
	placement *resmgr.Placement,
	expectedStates []t.TaskState,
	newState t.TaskState,
	reason string) *resmgr.Placement {
	invalidTaskSet := make(map[string]struct{})
	for _, task := range placement.GetTaskIDs() {
		rmTask := h.rmTracker.GetTask(task.GetPelotonTaskID())
		if rmTask == nil ||
			task.GetMesosTaskID().GetValue() != rmTask.Task().GetTaskId().GetValue() {
			invalidTaskSet[task.GetMesosTaskID().GetValue()] = struct{}{}
			continue
		}
		state := rmTask.GetCurrentState().State
		if !util.ContainsTaskState(expectedStates, state) {
			log.WithFields(log.Fields{
				"task_id":        task.GetPelotonTaskID().GetValue(),
				"expected_state": expectedStates,
				"actual_state":   state.String(),
				"new_state":      newState.String(),
			}).Error("Failed to transit tasks in placement: " +
				"task is not in expected state")
			invalidTaskSet[task.GetMesosTaskID().GetValue()] = struct{}{}
		} else {
			err := rmTask.TransitTo(
				newState.String(),
				statemachine.WithReason(reason),
			)
			if err != nil {
				log.WithError(err).
					WithField("task_id", task.GetPelotonTaskID().GetValue()).
					Info("Failed to transit tasks in placement")
				invalidTaskSet[task.GetMesosTaskID().GetValue()] = struct{}{}
			}
		}
	}
	return h.removeTasksFromPlacements(placement, invalidTaskSet)
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

	for _, e := range req.Events {
		h.handleEvent(e)
	}
	response.PurgeOffset = atomic.LoadUint64(h.maxOffset)
	return &response, nil
}

func (h *ServiceHandler) handleEvent(event *pb_eventstream.Event) {
	defer h.acknowledgeEvent(event.Offset)

	if event.GetType() != pb_eventstream.Event_MESOS_TASK_STATUS {
		return
	}

	taskState := util.MesosStateToPelotonState(
		event.MesosTaskStatus.GetState())
	if taskState != t.TaskState_RUNNING &&
		!util.IsPelotonStateTerminal(taskState) {
		return
	}

	mesosTask := event.GetMesosTaskStatus().GetTaskId().GetValue()
	ptID, err := util.ParseTaskIDFromMesosTaskID(
		mesosTask,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"event":         event,
			"mesos_task_id": mesosTask,
		}).Error("Could not parse mesos task ID")
		return
	}

	taskID := &peloton.TaskID{
		Value: ptID,
	}
	rmTask := h.rmTracker.GetTask(taskID)

	if rmTask == nil ||
		(rmTask.Task().GetTaskId().GetValue() != mesosTask) {
		// It might be an orphan task event
		rmTask = h.rmTracker.GetOrphanTask(mesosTask)
	}

	if rmTask == nil {
		return
	}

	if taskState == t.TaskState_RUNNING {
		err = rmTask.TransitTo(taskState.String(), statemachine.WithReason("task running"))
		if err != nil {
			log.WithError(errors.WithStack(err)).
				WithField("task_id", ptID).
				Info("Not able to transition to RUNNING for task")
		}
		return
	}

	// TODO: We probably want to terminate all the tasks in gang
	err = h.rmTracker.MarkItDone(event.GetMesosTaskStatus().GetTaskId().GetValue())
	if err != nil {
		log.WithField("event", event).WithError(err).Error(
			"Could not be updated")
		return
	}

	log.WithFields(log.Fields{
		"task_id":       ptID,
		"current_state": taskState.String(),
		"mesos_task_id": rmTask.Task().TaskId.Value,
	}).Info("Task is completed and removed from tracker")

	// publish metrics
	rmtask.GetTracker().UpdateMetrics(
		t.TaskState_RUNNING,
		taskState,
		scalar.ConvertToResmgrResource(rmTask.Task().GetResource()),
	)
}

func (h *ServiceHandler) acknowledgeEvent(offset uint64) {
	log.WithField("offset", offset).
		Debug("Event received by resource manager")
	if offset > atomic.LoadUint64(h.maxOffset) {
		atomic.StoreUint64(h.maxOffset, offset)
	}
}

func (h *ServiceHandler) fillTaskEntry(task *rmtask.RMTask,
) *resmgrsvc.GetActiveTasksResponse_TaskEntry {
	rmTaskState := task.GetCurrentState()
	taskEntry := &resmgrsvc.GetActiveTasksResponse_TaskEntry{
		TaskID:         task.Task().GetTaskId().GetValue(),
		TaskState:      rmTaskState.State.String(),
		Reason:         rmTaskState.Reason,
		LastUpdateTime: rmTaskState.LastUpdateTime.String(),
		Hostname:       task.Task().GetHostname(),
	}
	return taskEntry
}

// GetActiveTasks returns the active tasks in the scheduler based on the filters
// The filters can be particular task states, job ID or resource pool ID.
func (h *ServiceHandler) GetActiveTasks(
	ctx context.Context,
	req *resmgrsvc.GetActiveTasksRequest,
) (*resmgrsvc.GetActiveTasksResponse, error) {
	var taskStates = map[string]*resmgrsvc.GetActiveTasksResponse_TaskEntries{}
	log.WithField("req", req).Info("GetActiveTasks called")

	taskStateMap := h.rmTracker.GetActiveTasks(
		req.GetJobID(),
		req.GetRespoolID(),
		req.GetStates(),
	)
	for state, tasks := range taskStateMap {
		for _, task := range tasks {
			taskEntry := h.fillTaskEntry(task)
			if _, ok := taskStates[state]; !ok {
				var taskList resmgrsvc.GetActiveTasksResponse_TaskEntries
				taskStates[state] = &taskList
			}
			taskStates[state].TaskEntry = append(
				taskStates[state].GetTaskEntry(),
				taskEntry,
			)
		}
	}

	log.Info("GetActiveTasks returned")
	return &resmgrsvc.GetActiveTasksResponse{
		TasksByState: taskStates,
	}, nil
}

// GetPendingTasks returns the pending tasks from a resource pool in the
// order in which they were added up to a max limit number of gangs.
// Eg specifying a limit of 10 would return pending tasks from the first 10
// gangs in the queue.
// The tasks are grouped according to their gang membership since a gang is the
// unit of admission.
func (h *ServiceHandler) GetPendingTasks(
	ctx context.Context,
	req *resmgrsvc.GetPendingTasksRequest,
) (*resmgrsvc.GetPendingTasksResponse, error) {

	respoolID := req.GetRespoolID()
	limit := req.GetLimit()

	log.WithFields(log.Fields{
		"respool_id": respoolID,
		"limit":      limit,
	}).Info("GetPendingTasks called")

	if respoolID == nil {
		return &resmgrsvc.GetPendingTasksResponse{},
			status.Errorf(codes.InvalidArgument,
				"resource pool ID can't be nil")
	}

	node, err := h.resPoolTree.Get(&peloton.ResourcePoolID{
		Value: respoolID.GetValue()})
	if err != nil {
		return &resmgrsvc.GetPendingTasksResponse{},
			status.Errorf(codes.NotFound,
				"resource pool ID not found:%s", respoolID)
	}

	if !node.IsLeaf() {
		return &resmgrsvc.GetPendingTasksResponse{},
			status.Errorf(codes.InvalidArgument,
				"resource pool:%s is not a leaf node", respoolID)
	}

	// returns a list of pending resmgr.gangs for each queue
	gangsInQueue, err := h.getPendingGangs(node, limit)
	if err != nil {
		return &resmgrsvc.GetPendingTasksResponse{},
			status.Errorf(codes.Internal,
				"failed to return pending tasks, err:%s", err.Error())
	}

	// marshall the response since we only care about task ID's
	pendingGangs := make(map[string]*resmgrsvc.GetPendingTasksResponse_PendingGangs)
	for q, gangs := range gangsInQueue {
		var pendingGang []*resmgrsvc.GetPendingTasksResponse_PendingGang
		for _, gang := range gangs {
			var taskIDs []string
			for _, task := range gang.GetTasks() {
				taskIDs = append(taskIDs, task.GetId().GetValue())
			}
			pendingGang = append(pendingGang,
				&resmgrsvc.GetPendingTasksResponse_PendingGang{
					TaskIDs: taskIDs})
		}
		pendingGangs[q.String()] = &resmgrsvc.GetPendingTasksResponse_PendingGangs{
			PendingGangs: pendingGang,
		}
	}

	log.WithFields(log.Fields{
		"respool_id":    respoolID,
		"limit":         limit,
		"pending_gangs": pendingGangs,
	}).Debug("GetPendingTasks returned")

	return &resmgrsvc.GetPendingTasksResponse{
		PendingGangsByQueue: pendingGangs,
	}, nil
}

func (h *ServiceHandler) getPendingGangs(node respool.ResPool,
	limit uint32) (map[respool.QueueType][]*resmgrsvc.Gang,
	error) {

	var gangs []*resmgrsvc.Gang
	var err error

	gangsInQueue := make(map[respool.QueueType][]*resmgrsvc.Gang)

	for _, q := range []respool.QueueType{
		respool.PendingQueue,
		respool.NonPreemptibleQueue,
		respool.ControllerQueue,
		respool.RevocableQueue} {
		gangs, err = node.PeekGangs(q, limit)

		if err != nil {
			if _, ok := err.(r_queue.ErrorQueueEmpty); ok {
				// queue is empty, move to the next one
				continue
			}
			return gangsInQueue, errors.Wrap(err, "failed to peek pending gangs")
		}

		gangsInQueue[q] = gangs
	}

	return gangsInQueue, nil
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
		rmTaskToKill := h.rmTracker.GetTask(taskTobeKilled)

		if rmTaskToKill == nil {
			tasksNotFound = append(tasksNotFound, taskTobeKilled)
			continue
		}

		err := h.rmTracker.MarkItInvalid(rmTaskToKill.Task().GetTaskId().GetValue())
		if err != nil {
			tasksNotKilled = append(tasksNotKilled, taskTobeKilled)
			continue
		}

		log.WithFields(log.Fields{
			"task_id":       taskTobeKilled.Value,
			"current_state": rmTaskToKill.GetCurrentState().State.String(),
		}).Info("Task is Killed and removed from tracker")

		h.rmTracker.UpdateMetrics(
			rmTaskToKill.GetCurrentState().State,
			t.TaskState_KILLED,
			scalar.ConvertToResmgrResource(rmTaskToKill.Task().GetResource()),
		)
	}

	// release the hosts held for the tasks killed
	resp, err := h.hostmgrClient.ReleaseHostsHeldForTasks(ctx, &hostsvc.ReleaseHostsHeldForTasksRequest{
		Ids: listTasks,
	})
	if err != nil || resp.GetError() != nil {
		// ignore resp.Error to avoid potential infinite retry.
		// rely on hostmgr periodical clean up if fails to release.
		// may revisit this decision later
		log.WithFields(log.Fields{
			"task_ids": listTasks,
			"err":      err.Error(),
			"resp_err": resp.GetError(),
		}).Warn("Fail to release hosts held for tasks upon kill")
	}

	if err != nil {
		return &resmgrsvc.KillTasksResponse{}, err
	}

	if len(tasksNotKilled) == 0 && len(tasksNotFound) == 0 {
		return &resmgrsvc.KillTasksResponse{}, nil
	}

	var killResponseErr []*resmgrsvc.KillTasksResponse_Error
	for _, task := range tasksNotFound {
		killResponseErr = append(killResponseErr,
			&resmgrsvc.KillTasksResponse_Error{
				NotFound: &resmgrsvc.TasksNotFound{
					Message: "Task Not Found",
					Task:    task,
				},
			})
	}
	for _, task := range tasksNotKilled {
		killResponseErr = append(killResponseErr,
			&resmgrsvc.KillTasksResponse_Error{
				KillError: &resmgrsvc.KillTasksError{
					Message: "Task can't be killed",
					Task:    task,
				},
			})
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
	var preemptionCandidates []*resmgr.PreemptionCandidate
	for i := 0; i < int(limit); i++ {
		preemptionCandidate, err := h.preemptionQueue.DequeueTask(timeout * time.Millisecond)
		if err != nil {
			// no more tasks
			h.metrics.GetPreemptibleTasksTimeout.Inc(1)
			break
		}

		if preemptionCandidate.GetReason() == resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES {
			// Transit task state machine to PREEMPTING
			if rmTask := h.rmTracker.GetTask(preemptionCandidate.Id); rmTask != nil {
				err = rmTask.TransitTo(
					t.TaskState_PREEMPTING.String(), statemachine.WithReason("preemption triggered"))
				if err != nil {
					// the task could have moved from RUNNING state
					log.WithError(err).
						WithField("task_id", preemptionCandidate.Id.Value).
						Error("failed to transit state for task")
					continue
				}
			} else {
				log.WithError(err).
					WithField("task_id", preemptionCandidate.Id.Value).
					Error("failed to find task in the tracker")
				continue
			}
		}
		preemptionCandidates = append(preemptionCandidates, preemptionCandidate)
	}

	log.WithField("preemptible_tasks", preemptionCandidates).
		Info("GetPreemptibleTasks returned")
	h.metrics.GetPreemptibleTasksSuccess.Inc(1)
	return &resmgrsvc.GetPreemptibleTasksResponse{
		PreemptionCandidates: preemptionCandidates,
	}, nil
}

// UpdateTasksState will be called to notify the resource manager about the tasks
// which have been moved to cooresponding state , by that resource manager
// can take appropriate actions for those tasks. As an example if the tasks been
// launched then job manager will call resource manager to notify it is launched
// by that resource manager can stop timer for launching state. Similarly if
// task is been failed to be launched in host manager due to valid failure then
// job manager will tell resource manager about the task to be killed by that
// resource manager can remove the task from the tracker and relevant
// resource accounting can be done.
func (h *ServiceHandler) UpdateTasksState(
	ctx context.Context,
	req *resmgrsvc.UpdateTasksStateRequest) (*resmgrsvc.UpdateTasksStateResponse, error) {

	taskStateList := req.GetTaskStates()
	if len(taskStateList) == 0 {
		return &resmgrsvc.UpdateTasksStateResponse{}, nil
	}

	log.WithField("task_state_list", taskStateList).
		Debug("tasks called with states")
	h.metrics.APILaunchedTasks.Inc(1)

	for _, updateEntry := range taskStateList {
		id := updateEntry.GetTask()
		// Checking if the task is present in tracker, if not
		// drop that task to be updated
		task := h.rmTracker.GetTask(id)
		if task == nil {
			continue
		}

		// Checking if the state for the task is in terminal state
		if util.IsPelotonStateTerminal(updateEntry.GetState()) {
			err := h.rmTracker.MarkItDone(updateEntry.GetMesosTaskId().GetValue())
			if err != nil {
				log.WithError(err).WithFields(log.Fields{
					"task_id":      id,
					"update_entry": updateEntry,
				}).Error("could not update task")
			}
			continue
		}

		// Checking if the mesos task is same
		// otherwise drop the task
		if *task.Task().TaskId.Value != *updateEntry.GetMesosTaskId().Value {
			continue
		}
		err := task.TransitTo(updateEntry.GetState().String(),
			statemachine.WithReason(
				fmt.Sprintf("task moved to %s",
					updateEntry.GetState().String())))
		if err != nil {
			log.WithError(err).
				WithFields(log.Fields{
					"task_id":  id,
					"to_state": updateEntry.GetState().String(),
				}).Info("failed to transit")
			continue
		}
	}
	return &resmgrsvc.UpdateTasksStateResponse{}, nil
}

// GetOrphanTasks returns the list of orphan tasks
func (h *ServiceHandler) GetOrphanTasks(
	ctx context.Context,
	req *resmgrsvc.GetOrphanTasksRequest,
) (*resmgrsvc.GetOrphanTasksResponse, error) {
	var orphanTasks []*resmgr.Task
	for _, rmTask := range h.rmTracker.GetOrphanTasks(req.GetRespoolID()) {
		orphanTasks = append(orphanTasks, rmTask.Task())
	}

	return &resmgrsvc.GetOrphanTasksResponse{
		OrphanTasks: orphanTasks,
	}, nil
}

// GetHostsByScores returns a list of batch hosts with lowest host scores
func (h *ServiceHandler) GetHostsByScores(
	ctx context.Context,
	req *resmgrsvc.GetHostsByScoresRequest,
) (*resmgrsvc.GetHostsByScoresResponse, error) {
	return &resmgrsvc.GetHostsByScoresResponse{
		Hosts: h.batchScorer.GetHostsByScores(req.Limit),
	}, nil

}

// NewTestServiceHandler returns an empty new ServiceHandler ptr for testing.
func NewTestServiceHandler() *ServiceHandler {
	return &ServiceHandler{}
}
