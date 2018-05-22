package goalstate

import (
	"context"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/common/async"
	queue "code.uber.internal/infra/peloton/common/deadline_queue"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

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
	return &engine{
		queue:             queue.NewDeadlineQueue(queue.NewQueueMetrics(parentScope)),
		entityMap:         make(map[string]*entityMapItem),
		pool:              async.NewPool(async.PoolOptions{MaxWorkers: numWorkerThreads}),
		failureRetryDelay: failureRetryDelay,
		maxRetryDelay:     maxRetryDelay,
		mtx:               NewMetrics(parentScope),
	}
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

	queue     queue.DeadlineQueue       // goal state engine's deadline queue
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
	log.WithField("goalstate_id", id).
		WithField("goalstate_scheduled", e.IsScheduled(entity)).
		Debug("enqueue happening in goal state engine")
	e.queue.Enqueue(e.addItemToEntityMap(id, entity), deadline)
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
		err := action(ctx, entityItem.entity)
		if err != nil {
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
		// should never be hit
		log.WithField("goal_state_id", queueItem.GetString()).
			Error("did not find the identifier in the entity map")
		e.mtx.missingItems.Inc(1)
		return
	}

	reschedule, delay := e.runActions(entityItem)
	if reschedule == true {
		e.queue.Enqueue(queueItem, time.Now().Add(delay))
	}
}

// processItems dequeues the items from the deadline queue.
// stopChan is used to indicate to the deadline queue to stop processing.
func (e *engine) processItems(stopChan <-chan struct{}) {
	for {
		queueItem := e.queue.Dequeue(stopChan)
		if queueItem == nil {
			return
		}

		e.pool.Enqueue(async.JobFunc(func(context.Context) {
			e.processEntityAfterDequeue(queueItem.(*queue.Item))
		}))
	}
}

func (e *engine) Start() {
	e.Lock()
	defer e.Unlock()

	if e.stopChan != nil {
		return
	}

	e.stopChan = make(chan struct{})
	go e.processItems(e.stopChan)

	log.Info("goalstate.Engine started")
}

func (e *engine) Stop() {
	e.Lock()
	defer e.Unlock()

	if e.stopChan == nil {
		return
	}

	close(e.stopChan)
	e.stopChan = nil

	log.Info("goalstate.Engine stopped")
}
