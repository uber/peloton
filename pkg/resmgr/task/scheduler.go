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

package task

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pt "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	"github.com/uber/peloton/pkg/common/statemachine"
	"github.com/uber/peloton/pkg/resmgr/common"
	"github.com/uber/peloton/pkg/resmgr/queue"
	"github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/scalar"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

var (
	errEnqueueEmptySchedUnit    = errors.New("empty gang to to ready queue enqueue")
	errReadyQueueDequeueFailed  = errors.New("dequeue gang from ready queue failed")
	errReadyQueueDequeueTimeout = errors.New("dequeue gang from ready queue timed out")
	errTaskIsNotPresent         = errors.New("task is not present in tracker")
)

// Scheduler defines the interface of task scheduler which schedules
// tasks from the pending queues of resource pools to a ready queue
// using different scheduling policies.
type Scheduler interface {
	// Start starts the task scheduler goroutines
	Start() error
	// Stop stops the task scheduler goroutines
	Stop() error
	// Enqueues gang (task list) into resource pool ready queue
	EnqueueGang(gang *resmgrsvc.Gang) error
	// Dequeues gang (task list) from the resource pool ready queue
	DequeueGang(maxWaitTime time.Duration, taskType resmgr.TaskType) (*resmgrsvc.Gang, error)
	// Adds an invalid task so that it can be removed from the ready queue later.
	AddInvalidTask(task *peloton.TaskID)
}

// scheduler implements the TaskScheduler interface
type scheduler struct {
	lock         sync.Mutex
	condition    *sync.Cond
	runningState int32

	// the resource pool hierarchy.
	resPoolTree respool.Tree
	// queue of READY gangs which have passed admission control.
	queue queue.MultiLevelList
	// task tracker for rmtasks.
	rmTaskTracker Tracker
	// set of invalid tasks which will be discarded during dequeue.
	invalidTasks sync.Map

	schedulingPeriod time.Duration
	metrics          *Metrics
	random           *rand.Rand

	stopChan chan struct{}
}

var sched *scheduler

// InitScheduler initializes a Task Scheduler
func InitScheduler(
	parent tally.Scope,
	tree respool.Tree,
	taskSchedulingPeriod time.Duration,
	rmTaskTracker Tracker) {

	if sched != nil {
		log.Warning("Task scheduler has already been initialized")
		return
	}
	sched = &scheduler{
		condition:        sync.NewCond(&sync.Mutex{}),
		runningState:     common.RunningStateNotStarted,
		queue:            queue.NewMultiLevelList("ready-queue", maxReadyQueueSize),
		rmTaskTracker:    rmTaskTracker,
		resPoolTree:      tree,
		schedulingPeriod: taskSchedulingPeriod,
		metrics:          NewMetrics(parent.SubScope("task_scheduler")),
		random:           rand.New(rand.NewSource(time.Now().UnixNano())),
		stopChan:         make(chan struct{}, 1),
	}
	log.Info("Task scheduler is initialized")
}

// GetScheduler returns the task scheduler instance
func GetScheduler() Scheduler {
	if sched == nil {
		log.Fatalf("Task scheduler is not initialized")
	}
	return sched
}

// Start starts the Task Scheduler in a goroutine
func (s *scheduler) Start() error {
	defer s.lock.Unlock()
	s.lock.Lock()

	if s.runningState == common.RunningStateRunning {
		log.Warn("Task Scheduler is already running, no action will be performed")
		return nil
	}

	started := make(chan int, 1)
	go func() {
		defer atomic.StoreInt32(&s.runningState, common.RunningStateNotStarted)
		atomic.StoreInt32(&s.runningState, common.RunningStateRunning)
		ticker := time.NewTicker(s.schedulingPeriod)
		defer ticker.Stop()

		log.Info("Starting Task Scheduler")
		close(started)

		for {
			// TODO: we need to remove ticker and use chanel for signaling
			// For three cases
			// 1. When there is new Item in empty list
			// 2. When there is new Entitlement calculation
			// 3. When there is change in resources in resource pool

			select {
			case <-s.stopChan:
				log.Info("Exiting Task Scheduler")
				return
			case <-ticker.C:
				s.scheduleTasks()
			}
		}
	}()
	// Wait until go routine is started
	<-started
	return nil
}

// scheduleTasks moves gang tasks to ready queue in every scheduling cycle
func (s *scheduler) scheduleTasks() {
	// We need to iterate for all the leaf nodes in the list
	nodes := s.resPoolTree.GetAllNodes(true)
	for e := nodes.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)
		// DequeueGangs checks the entitlement for the
		// resource pool and takes the decision if we can
		// dequeue gang or not based on resource availability.
		gangList, err := n.DequeueGangs(dequeueGangLimit)
		if err != nil {
			log.WithError(err).
				WithField("respool_id", n.ID()).
				Error("Failed to dequeue from resource pool")
			continue
		}
		var invalidGangs []*resmgrsvc.Gang
		for _, gang := range gangList {
			invalidTasks, err := s.transitGang(
				gang,
				pt.TaskState_PENDING,
				pt.TaskState_READY,
				"gang admitted")
			if err != nil {
				newGang := s.processGangFailure(n, gang, invalidTasks)
				if newGang != nil {
					// All the tasks are not deleted from the gang,
					// Adding the new gang to invalid gang by that
					// it can be requeued again
					invalidGangs = append(invalidGangs, newGang)
				}
				continue
			}

			// Once the transition is done to ready state for all
			// the tasks in the gang , we are enqueuing the gang to
			// ready queue.
			if err = s.EnqueueGang(gang); err != nil {
				invalidGangs = append(invalidGangs, gang)
				continue
			}
		}

		// For invalid gangs, which are unable to be admitted to ready queue
		// at scheduling phase for placement
		//
		// 1. remove the allocation for invalid gangs
		// 2. transit those gangs from ready -> pending state
		// 3. enqueue those invalid gangs back to pending queue at respool
		for _, invalidGang := range invalidGangs {
			if err = n.SubtractFromAllocation(
				scalar.GetGangAllocation(invalidGang)); err != nil {
				log.WithField("gang", invalidGang).
					WithError(err).
					Error("unable to remove allocation for invalid gang from respool at scheduler")
			}

			if _, err := s.transitGang(
				invalidGang,
				pt.TaskState_READY,
				pt.TaskState_PENDING,
				"gang admission failure"); err != nil {
				log.WithError(err).
					Error("not able to transit from READY -> PENDING")
			}

			if err = n.EnqueueGang(invalidGang); err != nil {
				log.WithError(err).
					Error("not able to enqueue gang back to pending queue at resource pool")
			}
		}
	}
}

// processGangFailure removes the deleted tasks from the gang
// and return the gang if there are valid tasks remaining
func (s *scheduler) processGangFailure(
	n respool.ResPool,
	gang *resmgrsvc.Gang,
	invalidTasks map[string]error) *resmgrsvc.Gang {
	var newTasks []*resmgr.Task
	var deletedTasks []*resmgr.Task

	for _, t := range gang.Tasks {
		if err, ok := invalidTasks[t.Id.Value]; ok {
			if err != errTaskIsNotPresent {
				newTasks = append(newTasks, t)
				continue
			}
			deletedTasks = append(deletedTasks, t)
		}
	}

	if len(deletedTasks) > 0 {
		// we can not dequeue this gang Dropping them
		// As they can block the whole resource pool.
		// It could happen that the tasks have been killed
		// (Since the time they have been dequeued) and
		// removed from the tracker, so we need to drop
		// them from scheduling and remove their resources
		// from the resource pool allocation.
		log.WithError(errTaskIsNotPresent).WithFields(log.Fields{
			"deleted_tasks": deletedTasks,
			"from":          pt.TaskState_PENDING.String(),
			"to":            pt.TaskState_READY.String(),
		}).Info("Removing deleted tasks")
		// We need to remove the allocation from the resource pool
		// We are deleting allocation only for deleted tasks
		if err := n.SubtractFromAllocation(
			scalar.GetGangAllocation(
				&resmgrsvc.Gang{
					Tasks: deletedTasks,
				})); err != nil {
			log.WithField("gang", gang).
				WithError(err).
				Error("not able to remove allocation from respool")
		}
	}

	// All the tasks are deleted from the gang, returning nil
	if len(newTasks) == 0 {
		return nil
	}

	// Creating new gang with remaining tasks
	return &resmgrsvc.Gang{
		Tasks: newTasks,
	}
}

// transitGang tries to transit to "TO" state for all the tasks
// in the gang and if anyone fails sends error
func (s *scheduler) transitGang(gang *resmgrsvc.Gang, fromState pt.TaskState, toState pt.TaskState,
	reason string) (map[string]error, error) {
	isInvalidTaskInGang := false
	invalidTasks := make(map[string]error)
	for _, task := range gang.GetTasks() {
		rmTask := s.rmTaskTracker.GetTask(task.Id)
		if rmTask == nil {
			isInvalidTaskInGang = true
			invalidTasks[task.GetId().GetValue()] = errTaskIsNotPresent
			continue
		}

		if err := rmTask.TransitFromTo(
			fromState.String(),
			toState.String(),
			statemachine.WithReason(reason)); err != nil {
			isInvalidTaskInGang = true
			invalidTasks[task.GetId().GetValue()] = err
		}
	}

	if isInvalidTaskInGang {
		return invalidTasks, errors.Errorf("invalid Tasks in gang %s", gang)
	}
	return nil, nil
}

// Stop stops Task Scheduler process
func (s *scheduler) Stop() error {
	defer s.lock.Unlock()
	s.lock.Lock()

	if s.runningState == common.RunningStateNotStarted {
		log.Warn("Task Scheduler is already stopped, no action will be performed")
		return nil
	}

	log.Info("Stopping Task Scheduler")
	s.stopChan <- struct{}{}

	// Wait for task scheduler to be stopped
	for {
		runningState := atomic.LoadInt32(&s.runningState)
		if runningState == common.RunningStateRunning {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	log.Info("Task Scheduler Stopped")
	return nil
}

// EnqueueGang inserts a gang, which is a task list of 1 or more (same priority)
// tasks, into the ready queue.
func (s *scheduler) EnqueueGang(gang *resmgrsvc.Gang) error {
	if (gang == nil) || (len(gang.Tasks) == 0) {
		return errEnqueueEmptySchedUnit
	}
	level := gang.Tasks[0].Type
	err := s.queue.Push(int(level), gang)
	if err != nil {
		log.WithError(err).Error("error in EnqueueGang")
	}
	s.metrics.ReadyQueueLen.Update(float64(s.queue.Size()))
	if err == nil {
		s.condition.Broadcast()
	}
	return err
}

// AddInvalidTask adds an invalid task so that it can remove them from the
// ready queue later
func (s *scheduler) AddInvalidTask(id *peloton.TaskID) {
	s.invalidTasks.Store(id.GetValue(), true)
}

// DequeueGang dequeues a gang, which is a task list of 1 or more (same priority)
// tasks of type task type, from the ready queue. If task type is UNKNOWN then
// gangs with tasks of any task type will be returned.
func (s *scheduler) DequeueGang(
	maxWaitTime time.Duration,
	taskType resmgr.TaskType) (*resmgrsvc.Gang, error) {

	level := int(taskType)
	if taskType == resmgr.TaskType_UNKNOWN {
		levels := s.queue.Levels()
		if len(levels) > 0 {
			level = levels[s.getRandLevel(len(levels))]
		}
	}

	gang, err := s.dequeueWithTimeout(maxWaitTime, level)
	if err != nil {
		return gang, err
	}

	// Make sure all the tasks in the gang are valid.
	// A task can be invalid if it was deleted. To reduce the performance
	// penalty of removing the task from the ready queue we remove the task
	// during dequeue.
	var validTasks []*resmgr.Task
	for _, t := range gang.GetTasks() {
		if _, ok := s.invalidTasks.Load(t.GetId().GetValue()); ok {
			s.invalidTasks.Delete(t.GetId().GetValue())
			continue
		}
		validTasks = append(validTasks, t)
	}

	return &resmgrsvc.Gang{Tasks: validTasks}, nil
}

// thread safe way to get random level
func (s *scheduler) getRandLevel(n int) int {
	s.lock.Lock()
	l := s.random.Intn(n)
	s.lock.Unlock()
	return l
}

func (s *scheduler) dequeueWithTimeout(
	maxWaitTime time.Duration,
	level int) (*resmgrsvc.Gang, error) {
	s.condition.L.Lock()
	defer s.condition.L.Unlock()
	pastDeadline := uint32(0)
	timer := time.NewTimer(maxWaitTime)
	defer timer.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-ctx.Done():
		case <-timer.C:
			atomic.StoreUint32(&pastDeadline, 1)
			s.condition.Broadcast()
		}
	}()
	var item interface{}
	var err error
	for {
		item, err = s.queue.Pop(level)
		_, isEmpty := err.(queue.ErrorQueueEmpty)
		if atomic.LoadUint32(&pastDeadline) == 1 || err == nil || !isEmpty {
			break
		}
		s.condition.Wait()
	}
	if item == nil {
		if atomic.LoadUint32(&pastDeadline) == 1 {
			return nil, errReadyQueueDequeueTimeout
		}
		return nil, errReadyQueueDequeueFailed
	}
	res := item.(*resmgrsvc.Gang)
	s.metrics.ReadyQueueLen.Update(float64(s.queue.Size()))
	return res, nil
}
