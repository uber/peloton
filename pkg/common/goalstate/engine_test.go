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
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/uber/peloton/pkg/common/async"
	queue "github.com/uber/peloton/pkg/common/deadline_queue"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

const (
	stateValue         = "init"
	goalStateValue     = "success"
	stateValueMulti    = "multi"
	goalStateValueFail = "fail"

	numWorkerThreads = 3
)

// Lock to access global variables
var globalLock sync.RWMutex

// Global id list used to test actions being called
var idList []string

// used to simulate failure
var failCount int

// synchronization primitive to indicate when all entity actions are complete
var wg sync.WaitGroup

// Sample test action
func testAction(ctx context.Context, entity Entity) error {
	globalLock.Lock()
	defer globalLock.Unlock()

	idList = append(idList, entity.GetID())
	wg.Done()
	return nil
}

// Sample test action which fails thrice before succeeding
func testActionFailure(ctx context.Context, entity Entity) error {
	globalLock.Lock()
	defer globalLock.Unlock()

	idList = append(idList, entity.GetID())
	if failCount < 3 {
		failCount++
		return fmt.Errorf("fake error")
	}
	failCount = 0
	wg.Done()
	return nil
}

// Test implementation of Entity
type testEntity struct {
	id        string
	state     string
	goalstate string
}

// Returns a new test entity
func newTestEntity(id string, state string, goalstate string) *testEntity {
	return &testEntity{
		id:        id,
		state:     state,
		goalstate: goalstate,
	}
}

func (te *testEntity) GetID() string {
	return te.id
}

func (te *testEntity) GetState() interface{} {
	return te.state
}

func (te *testEntity) GetGoalState() interface{} {
	return te.goalstate
}

func (te *testEntity) GetActionList(state interface{}, goalstate interface{}) (context.Context, context.CancelFunc, []Action) {
	var actions []Action
	actionS := Action{
		Name:    "testAction",
		Execute: testAction,
	}
	actionF := Action{
		Name:    "testActionFailure",
		Execute: testActionFailure,
	}

	if state == stateValue && goalstate == goalStateValue {
		// returns sample test action
		actions = append(actions, actionS)
	} else if state == stateValue && goalstate == goalStateValueFail {
		// returns sample test actions which fails thrice before succeeding
		actions = append(actions, actionF)
	} else if state == stateValueMulti && goalstate == goalStateValue {
		// returns both sample test actions with a context timeout
		actions = append(actions, actionS)
		actions = append(actions, actionF)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		return ctx, cancel, actions
	} else if state == stateValueMulti && goalstate == goalStateValueFail {
		// returns empty action
		wg.Done()
	}
	return context.Background(), nil, actions
}

// TestEngineStartStop tests starting and stopping the goal state engine.
func TestEngineStartStop(t *testing.T) {
	e := &engine{
		entityMap:         make(map[string]*entityMapItem),
		failureRetryDelay: 1 * time.Second,
		maxRetryDelay:     1 * time.Second,
		mtx:               NewMetrics(tally.NoopScope),
	}

	asyncQueue := &asyncWorkerQueue{
		queue:  queue.NewDeadlineQueue(queue.NewQueueMetrics(tally.NoopScope)),
		engine: e,
	}

	pool := async.NewPool(
		async.PoolOptions{MaxWorkers: numWorkerThreads},
		asyncQueue,
	)
	e.pool = pool

	e.Start()
	e.Stop()
}

// TestEngineEnqueueDequeueSuccess tests enqueing a test entity, and then
// dequeuing it to run sample test action.
func TestEngineEnqueueDequeueSuccess(t *testing.T) {
	idList = []string{}
	failCount = 0
	e := &engine{
		entityMap:         make(map[string]*entityMapItem),
		failureRetryDelay: 1 * time.Second,
		maxRetryDelay:     1 * time.Second,
		mtx:               NewMetrics(tally.NoopScope),
	}

	asyncQueue := &asyncWorkerQueue{
		queue:  queue.NewDeadlineQueue(queue.NewQueueMetrics(tally.NoopScope)),
		engine: e,
	}

	pool := async.NewPool(
		async.PoolOptions{MaxWorkers: numWorkerThreads},
		asyncQueue,
	)
	e.pool = pool

	count := 10
	for i := uint32(0); i < uint32(count); i++ {
		ent := newTestEntity(strconv.Itoa(int(i)), stateValue, goalStateValue)
		e.Enqueue(ent, time.Now())
		assert.True(t, e.IsScheduled(ent))
	}
	wg.Add(count)
	e.pool.Start()
	wg.Wait()
	e.pool.Stop()
	assert.Equal(t, count, len(idList))

	assert.Equal(t, count, len(e.entityMap))
	for i := uint32(0); i < uint32(count); i++ {
		ent := e.getItemFromEntityMap(strconv.Itoa(int(i)))
		assert.False(t, e.IsScheduled(ent.entity))
		e.Delete(ent.entity)
	}
	assert.Equal(t, 0, len(e.entityMap))
}

// TestEngineEnqueueDequeueFailure tests enqueing a test entity, and then
// dequeuing it to run sample test action which fails thrice before succeeding.
func TestEngineEnqueueDequeueFailure(t *testing.T) {
	idList = []string{}
	failCount = 0
	e := &engine{
		entityMap:         make(map[string]*entityMapItem),
		failureRetryDelay: 100 * time.Millisecond,
		maxRetryDelay:     200 * time.Millisecond,
		mtx:               NewMetrics(tally.NoopScope),
	}

	asyncQueue := &asyncWorkerQueue{
		queue:  queue.NewDeadlineQueue(queue.NewQueueMetrics(tally.NoopScope)),
		engine: e,
	}

	pool := async.NewPool(
		async.PoolOptions{MaxWorkers: numWorkerThreads},
		asyncQueue,
	)
	e.pool = pool

	count := 3
	for i := uint32(0); i < uint32(count); i++ {
		ent := newTestEntity(strconv.Itoa(int(i)), stateValue, goalStateValueFail)
		e.Enqueue(ent, time.Now())
	}
	wg.Add(count)

	e.pool.Start()
	wg.Wait()
	e.pool.Stop()
	assert.Equal(t, 4*count, len(idList))
	for i := uint32(0); i < uint32(count); i++ {
		ent := e.getItemFromEntityMap(strconv.Itoa(int(i)))
		ent.Lock()
		assert.Equal(t, time.Duration(0), ent.delay)
		ent.Unlock()
		e.Delete(ent.entity)
	}
	assert.Equal(t, 0, len(e.entityMap))
}

// TestEngineMultipleActions tests running multiple actions after
// dequeing a test entity.
func TestEngineMultipleActions(t *testing.T) {
	idList = []string{}
	failCount = 0
	e := &engine{
		entityMap:         make(map[string]*entityMapItem),
		failureRetryDelay: 100 * time.Millisecond,
		maxRetryDelay:     200 * time.Millisecond,
		mtx:               NewMetrics(tally.NoopScope),
	}

	asyncQueue := &asyncWorkerQueue{
		queue:  queue.NewDeadlineQueue(queue.NewQueueMetrics(tally.NoopScope)),
		engine: e,
	}

	pool := async.NewPool(
		async.PoolOptions{MaxWorkers: numWorkerThreads},
		asyncQueue,
	)
	e.pool = pool

	count := 3
	for i := uint32(0); i < uint32(count); i++ {
		ent := newTestEntity(strconv.Itoa(int(i)), stateValueMulti, goalStateValue)
		e.Enqueue(ent, time.Now())
	}
	wg.Add((4 * count) + count)

	e.pool.Start()
	wg.Wait()
	e.pool.Stop()
	assert.Equal(t, 4*2*count, len(idList))
	for i := uint32(0); i < uint32(count); i++ {
		ent := e.getItemFromEntityMap(strconv.Itoa(int(i)))
		e.Delete(ent.entity)
	}
	assert.Equal(t, 0, len(e.entityMap))
}

// TestNoActions tests dequeuing a test entity which runs no actions.
func TestNoActions(t *testing.T) {
	idList = []string{}
	failCount = 0
	e := &engine{
		entityMap:         make(map[string]*entityMapItem),
		failureRetryDelay: 100 * time.Millisecond,
		maxRetryDelay:     200 * time.Millisecond,
		mtx:               NewMetrics(tally.NoopScope),
	}

	asyncQueue := &asyncWorkerQueue{
		queue:  queue.NewDeadlineQueue(queue.NewQueueMetrics(tally.NoopScope)),
		engine: e,
	}

	pool := async.NewPool(
		async.PoolOptions{MaxWorkers: numWorkerThreads},
		asyncQueue,
	)
	e.pool = pool

	count := 10
	for i := uint32(0); i < uint32(count); i++ {
		ent := newTestEntity(strconv.Itoa(int(i)), stateValueMulti, goalStateValueFail)
		e.Enqueue(ent, time.Now())
	}
	wg.Add(count)

	e.pool.Start()
	wg.Wait()
	e.pool.Stop()
	assert.Equal(t, 0, len(idList))
}

// TestMultiRequeue tests re-queuing the same entity multiple times.
func TestMultiRequeue(t *testing.T) {
	idList = []string{}
	failCount = 0
	e := &engine{
		entityMap:         make(map[string]*entityMapItem),
		failureRetryDelay: 1 * time.Second,
		maxRetryDelay:     1 * time.Second,
		mtx:               NewMetrics(tally.NoopScope),
	}

	asyncQueue := &asyncWorkerQueue{
		queue:  queue.NewDeadlineQueue(queue.NewQueueMetrics(tally.NoopScope)),
		engine: e,
	}

	pool := async.NewPool(
		async.PoolOptions{MaxWorkers: numWorkerThreads},
		asyncQueue,
	)
	e.pool = pool

	count := 10
	deadline := 30 * time.Second
	for i := uint32(0); i < uint32(count); i++ {
		ent := newTestEntity(strconv.Itoa(int(i)), stateValue, goalStateValue)
		e.Enqueue(ent, time.Now().Add(deadline))
	}
	wg.Add(count)
	assert.Equal(t, 0, len(idList))

	// Requeue again with a larger deadline
	deadline = 60 * time.Second
	for i := uint32(0); i < uint32(count); i++ {
		ent := newTestEntity(strconv.Itoa(int(i)), stateValue, goalStateValue)
		e.Enqueue(ent, time.Now().Add(deadline))
	}
	assert.Equal(t, 0, len(idList))

	// Requeue again with a smaller deadline
	for i := uint32(0); i < uint32(count); i++ {
		ent := newTestEntity(strconv.Itoa(int(i)), stateValue, goalStateValue)
		e.Enqueue(ent, time.Now())
	}

	e.pool.Start()
	wg.Wait()
	e.pool.Stop()
	assert.Equal(t, count, len(idList))
}
