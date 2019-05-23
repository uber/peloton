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

package goalstate

import (
	"context"
	"sync"
	"time"

	"github.com/uber/peloton/pkg/common/async"
	queue "github.com/uber/peloton/pkg/common/deadline_queue"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// asyncWorkerQueueItem implements the async.Job interface while
// storing the metadata for async.Queue which includes information
// to be provided to the deadline queue on Enqueue
type asyncWorkerQueueItem struct {
	item     queue.QueueItem // the queue item
	deadline time.Time       // deadline to be set for the queue item in enqueue
	engine   *engine         // backpointer to goal state engine
}

func (w *asyncWorkerQueueItem) Run(ctx context.Context) {
	w.engine.processEntityAfterDequeue(w.item.(*queue.Item))
}

// asyncWorkerQueue is a wrapper around deadline queue which
// implements async.Queue
type asyncWorkerQueue struct {
	queue  queue.DeadlineQueue // goal state engine's deadline queue
	engine *engine             // backpointer to goal state engine

	// jobChan is used to sync between asyncWorkerQueue.Dequeue,
	// because queue.DeadlineQueue is not concurrency safe.
	// asyncWorkerQueue.Run would continue to enqueue into the
	// channel, and asyncWorkerQueue.Dequeue would read from the
	// channel.
	jobChan chan queue.QueueItem
}

func newAsyncWorkerQueue(
	ddlQueue queue.DeadlineQueue,
	engine *engine,
) *asyncWorkerQueue {

	return &asyncWorkerQueue{
		queue:  ddlQueue,
		engine: engine,
	}
}

func (q *asyncWorkerQueue) Run(stopChan chan struct{}) {
	q.jobChan = make(chan queue.QueueItem)

	go func() {
		for {
			queueItem := q.queue.Dequeue(stopChan)
			if queueItem == nil {
				q.jobChan <- nil
				return
			}
			select {
			case q.jobChan <- queueItem:
				continue
			case <-stopChan:
				close(q.jobChan)
				return
			}
		}
	}()
}

func (q *asyncWorkerQueue) Enqueue(job async.Job) {
	asyncQueueItem := job.(*asyncWorkerQueueItem)
	q.queue.Enqueue(asyncQueueItem.item, asyncQueueItem.deadline)
	return
}

func (q *asyncWorkerQueue) Dequeue() async.Job {
	queueItem := <-q.jobChan
	if queueItem == nil {
		return nil
	}

	return &asyncWorkerQueueItem{
		item:   queueItem,
		engine: q.engine,
	}
}

// Engine defines the goal state engine interface.
type Engine interface {
	// Start starts the goal state engine processing.
	Start()
	// Enqueue is used to enqueue an entity into the goal state
	// engine for evaluation. The paramater deadline specifies when
	// the entity should be evaluated.
	// Enqueue creates state in the goal state engine which will persist
	// till the caller calls an explicit delete to clean up this state.
	Enqueue(entity Entity, deadline time.Time)
	// IsScheduled is used to determine if a given entity is queued in
	// the deadline queue for evaluation
	IsScheduled(entity Entity) bool
	// Delete is used clean up the state created in the goal state
	// engine for the entity. It is the caller's responsibility to
	// explicitly call delete when an entity is being removed from the system.
	// If Delete is not called, the state in goal state engine will persis forever.
	Delete(entity Entity)
	// Stops stops the goal state engine processing.
	Stop()
}

// NewEngine returns a new goal state engine object.
func NewEngine(
	numWorkerThreads int,
	failureRetryDelay time.Duration,
	maxRetryDelay time.Duration,
	parentScope tally.Scope) Engine {
	e := &engine{
		entityMap:         make(map[string]*entityMapItem),
		failureRetryDelay: failureRetryDelay,
		maxRetryDelay:     maxRetryDelay,
		mtx:               NewMetrics(parentScope),
	}

	asyncQueue := newAsyncWorkerQueue(queue.NewDeadlineQueue(queue.NewQueueMetrics(parentScope)), e)

	pool := async.NewPool(
		async.PoolOptions{MaxWorkers: numWorkerThreads},
		asyncQueue,
	)
	e.pool = pool

	return e
}

// entityMapItem stores the entity state in goal state engine.
type entityMapItem struct {
	sync.RWMutex // the mutex to synchronize access to this object

	entity    Entity      // the entity object
	queueItem *queue.Item // the correspoing queue item in the deadline queue
	// delay is used by goal state to track expoenential backoff of scheduling
	// duration in case entity actions keep returning an error.
	delay time.Duration
}

// engine implements the goal state engine interface
type engine struct {
	sync.RWMutex // the mutex to synchronize access to this object

	entityMap map[string]*entityMapItem // map to store the entity items
	stopChan  chan struct{}             // channel to indicate to deadline queue to stop processing

	pool *async.Pool // worker pool to process queue items after dequeue

	// Global configuration for the delay for each retry on error.
	failureRetryDelay time.Duration
	// Global configuration for the absolute maximum duration between
	// retries. Exponential backoff will be capped at this value.
	maxRetryDelay time.Duration

	mtx *Metrics // goal state engine metrics
}

// addItemToEntityMap stores an entity object in the entity map.
// When a new enqueue request comes in, instead of doing a get and add
// without a lock, this API should be used so that both get and add is
// done while holding the lock. This ensures that concurrent enqueue
// requests for the same entity get synchronized correctly.
func (e *engine) addItemToEntityMap(id string, entity Entity) *queue.Item {
	e.Lock()
	defer e.Unlock()

	var entityItem *entityMapItem

	entityItem, ok := e.entityMap[id]
	if !ok {
		queueItem := queue.NewItem(id)
		entityItem = &entityMapItem{
			entity:    entity,
			queueItem: queueItem,
		}
		e.entityMap[id] = entityItem
		// Only update for adds for now. This is to prevent having to compute
		// the length on every delete as well.
		e.mtx.totalItems.Update(float64(len(e.entityMap)))
	}
	return entityItem.queueItem
}

// getItemFromEntityMap fetches an entity object from the entity map.
func (e *engine) getItemFromEntityMap(id string) *entityMapItem {
	e.RLock()
	defer e.RUnlock()

	item, ok := e.entityMap[id]
	if !ok {
		return nil
	}
	return item
}

// deleteItemFromEntityMap deletes the entity object from the entity map.
func (e *engine) deleteItemFromEntityMap(id string) {
	e.Lock()
	defer e.Unlock()

	delete(e.entityMap, id)
}

// getFailureRetryDelay fetches the failureRetryDelay global configuration
func (e *engine) getFailureRetryDelay() time.Duration {
	e.RLock()
	defer e.RUnlock()

	return e.failureRetryDelay
}

// getMaxRetryDelay fetches the maxRetryDelay global configuration
func (e *engine) getMaxRetryDelay() time.Duration {
	e.RLock()
	defer e.RUnlock()

	return e.maxRetryDelay
}

func (e *engine) Enqueue(entity Entity, deadline time.Time) {
	id := entity.GetID()
	asyncQueueItem := &asyncWorkerQueueItem{
		item:     e.addItemToEntityMap(id, entity),
		deadline: deadline,
	}
	e.pool.Enqueue(asyncQueueItem)
}

func (e *engine) IsScheduled(entity Entity) bool {
	id := entity.GetID()
	entityItem := e.getItemFromEntityMap(id)
	if entityItem == nil {
		return false
	}

	entityItem.RLock()
	defer entityItem.RUnlock()

	return entityItem.queueItem.IsScheduled()
}

func (e *engine) Delete(entity Entity) {
	id := entity.GetID()
	e.deleteItemFromEntityMap(id)
}

// calculateDelay is a helper function to calculate the backoff delay
// in case of error.
func (e *engine) calculateDelay(entityItem *entityMapItem) {
	entityItem.delay = entityItem.delay + e.getFailureRetryDelay()
	if entityItem.delay > e.getMaxRetryDelay() {
		entityItem.delay = e.getMaxRetryDelay()
	}
}

// runActions fetches the action list for an entity and then executes each action.
// Return value reschedule indicates whether the entity needs to be rescheduled
// in the deadline queue, while the return value delay indicates the deadline
// from time.Now() when the entity needs to be evaluated again.
// // Enqueue should always happen outside entityItem lock, hence enqueue is not done here.
func (e *engine) runActions(entityItem *entityMapItem) (reschedule bool, delay time.Duration) {
	entityItem.Lock()
	defer entityItem.Unlock()

	// Get the actions based on state and goal state of entity.
	state := entityItem.entity.GetState()
	goalState := entityItem.entity.GetGoalState()
	ctx, cancel, actions := entityItem.entity.GetActionList(state, goalState)
	if cancel != nil {
		defer cancel()
	}

	if len(actions) == 0 {
		return false, 0
	}

	// Execute each action.
	for _, action := range actions {
		tStart := time.Now()
		err := action.Execute(ctx, entityItem.entity)
		e.mtx.scope.Tagged(map[string]string{"action": action.Name}).
			Timer("run_duration").Record(time.Since(tStart))
		if err != nil {
			log.WithError(err).
				WithFields(log.Fields{
					"entity_id":   entityItem.entity.GetID(),
					"action_name": action.Name,
				}).
				Info("goal state action failed to execute")
			// Backoff and reevaluate the entity again.
			e.calculateDelay(entityItem)
			return true, entityItem.delay
		}
		// set delay to 0
		entityItem.delay = 0
	}
	return false, 0
}

// processEntityAfterDequeue is a helper function to evaluate
// an entity dequeued from the deadline queue, and execute the
// corresponding actions.
func (e *engine) processEntityAfterDequeue(queueItem *queue.Item) {
	entityItem := e.getItemFromEntityMap(queueItem.GetString())
	if entityItem == nil {
		// If an object is deleted from the entity map, it may
		// still exist in the deadline queue. An example in Peloton is
		// task items which are untracked when a job gets untracked.
		// Since untracking of job happens in job goal state, it may get
		// untracked while the task has not been dequeued yet in the deadline queue
		// which will hit this if check.
		log.WithField("goal_state_id", queueItem.GetString()).
			Debug("did not find the identifier in the entity map")
		e.mtx.missingItems.Inc(1)
		return
	}

	reschedule, delay := e.runActions(entityItem)
	if reschedule == true {
		asyncQueueItem := &asyncWorkerQueueItem{
			item:     queueItem,
			deadline: time.Now().Add(delay),
		}
		e.pool.Enqueue(asyncQueueItem)
	}
}

func (e *engine) Start() {
	e.Lock()
	defer e.Unlock()

	e.pool.Start()
	log.Info("goalstate.Engine started")
}

func (e *engine) Stop() {
	e.Lock()
	defer e.Unlock()

	e.pool.Stop()
	log.Info("goalstate.Engine stopped")
}
