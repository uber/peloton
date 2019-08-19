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

package placement

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/pkg/common/async"
	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/models"
	offers_mock "github.com/uber/peloton/pkg/placement/offers/mocks"
	"github.com/uber/peloton/pkg/placement/plugins"
	"github.com/uber/peloton/pkg/placement/plugins/batch"
	"github.com/uber/peloton/pkg/placement/plugins/mimir"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/algorithms"
	"github.com/uber/peloton/pkg/placement/plugins/mocks"
	tasks_mock "github.com/uber/peloton/pkg/placement/tasks/mocks"
	"github.com/uber/peloton/pkg/placement/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

const (
	_testReason = "Test Placement Reason"
)

type option func(placementConfig *config.PlacementConfig)

func withTaskType(taskType resmgr.TaskType) func(placementConfig *config.PlacementConfig) {
	return func(placementConfig *config.PlacementConfig) {
		placementConfig.TaskType = taskType
	}
}

func withStrategy(strategy config.PlacementStrategy) func(placementConfig *config.PlacementConfig) {
	return func(placementConfig *config.PlacementConfig) {
		placementConfig.Strategy = strategy
	}
}

func setupEngine(t *testing.T, options ...option) (
	*gomock.Controller,
	*engine, *offers_mock.MockService,
	*tasks_mock.MockService,
	*mocks.MockStrategy,
	tally.TestScope) {
	ctrl := gomock.NewController(t)

	mockOfferService := offers_mock.NewMockService(ctrl)
	mockTaskService := tasks_mock.NewMockService(ctrl)
	mockStrategy := mocks.NewMockStrategy(ctrl)
	config := &config.PlacementConfig{
		TaskDequeueLimit:     10,
		OfferDequeueLimit:    10,
		MaxPlacementDuration: 30 * time.Second,
		TaskDequeueTimeOut:   100,
		TaskType:             resmgr.TaskType_BATCH,
		FetchOfferTasks:      false,
		Strategy:             config.Batch,
		Concurrency:          1,
		MaxRounds: config.MaxRoundsConfig{
			Unknown:   1,
			Batch:     1,
			Stateless: 5,
			Daemon:    5,
			Stateful:  0,
		},
		MaxDurations: config.MaxDurationsConfig{
			Unknown:   5 * time.Second,
			Batch:     5 * time.Second,
			Stateless: 10 * time.Second,
			Daemon:    15 * time.Second,
			Stateful:  25 * time.Second,
		},
	}

	for _, option := range options {
		option(config)
	}

	pool := async.NewPool(async.PoolOptions{}, nil)
	pool.Start()
	scope := tally.NewTestScope("", map[string]string{})
	e := New(
		scope,
		config,
		mockOfferService,
		mockTaskService,
		nil,
		mockStrategy,
		pool,
	)

	return ctrl, e.(*engine), mockOfferService, mockTaskService, mockStrategy, scope
}

// Tests that metric is emitted if number of hosts returned by host manager
// are greater than equal to number of assignments.
func TestEngineTaskAffinityConstraintFailure(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, mockStrategy, scope := setupEngine(t)

	defer ctrl.Finish()
	engine.config.MaxPlacementDuration = 1 * time.Second

	host1 := testutil.SetupHostOffers()
	host2 := testutil.SetupHostOffers()
	offers := []models.Offer{host1, host2}
	assignment := testutil.SetupAssignment(time.Now().Add(1*time.Second), 10)
	assignment.Task.PlacementDeadline = time.Now().Add(-1 * time.Second)
	assignments := []models.Task{assignment}

	mockStrategy.EXPECT().
		GetTaskPlacements(
			gomock.Any(),
			gomock.Any(),
		).
		Return(map[int]int{}).
		AnyTimes()

	mockTaskService.EXPECT().
		SetPlacements(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).MinTimes(1).
		Return().
		AnyTimes()

	mockOfferService.EXPECT().
		Acquire(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).MinTimes(1).
		Return(offers, _testReason)

	mockOfferService.EXPECT().Release(
		gomock.Any(),
		gomock.Any()).
		Return().
		AnyTimes()

	mockStrategy.EXPECT().
		ConcurrencySafe().
		Return(true).
		AnyTimes()

	needs := plugins.PlacementNeeds{
		Revocable: true,
	}
	engine.placeAssignmentGroup(context.Background(), needs, assignments)
	assert.Equal(t, int64(1), scope.Snapshot().Counters()["batch.placement.host_limit+result=fail"].Value())
}

func TestEnginePlaceNoTasksToPlace(t *testing.T) {
	ctrl, engine, _, mockTaskService, _, _ := setupEngine(t)
	defer ctrl.Finish()

	mockTaskService.EXPECT().
		Dequeue(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(
			nil,
		)

	_, delay := engine.Place(context.Background(), nil)
	assert.True(t, delay > time.Duration(0))
}

func TestEnginePlaceMultipleTasks(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, _, _ := setupEngine(t)
	defer ctrl.Finish()
	createTasks := 25
	createHosts := 10

	engine.config.MaxPlacementDuration = time.Second
	deadline := time.Now().Add(time.Second)

	var assignments []models.Task
	for i := 0; i < createTasks; i++ {
		assignment := testutil.SetupAssignment(deadline, 1)
		assignment.GetTask().GetTask().Resource.CpuLimit = 5
		assignments = append(assignments, assignment)
	}

	var hosts []models.Offer
	for i := 0; i < createHosts; i++ {
		hosts = append(hosts, testutil.SetupHostOffers())
	}

	mockOfferService.EXPECT().Acquire(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).Return(hosts, _testReason).MinTimes(1)
	mockOfferService.EXPECT().Release(
		gomock.Any(),
		gomock.Any()).
		Return()

	mockTaskService.EXPECT().
		Dequeue(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Times(1).
		Return(assignments)
	mockTaskService.EXPECT().SetPlacements(
		gomock.Any(),
		gomock.Any(),
		gomock.Any()).
		Return()

	engine.strategy = batch.New(&config.PlacementConfig{})
	engine.Place(context.Background(), nil)
	engine.pool.WaitUntilProcessed()

	var success, failed int
	for _, assignment := range assignments {
		if assignment.GetPlacement() != nil {
			success++
		} else {
			failed++
		}
	}

	assert.Equal(t, createTasks, success)
	assert.Equal(t, 0, failed)
}

func TestEnginePlaceInPlaceUpdateTasks(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, _, _ := setupEngine(
		t,
		withTaskType(resmgr.TaskType_STATELESS),
		withStrategy(config.Mimir),
	)

	defer ctrl.Finish()
	createTasks := 24
	createHosts := 12

	engine.config.MaxPlacementDuration = time.Second
	// set a long enough deadline, so in one run of Place,
	// the assignment would not pass deadline for getting
	// placed on the desired host
	deadline := time.Now().Add(10 * time.Minute)

	var assignments []models.Task
	for i := 0; i < createTasks; i++ {
		assignment := testutil.SetupAssignment(deadline, 2)
		assignment.GetTask().GetTask().Resource.CpuLimit = 1
		// half of the tasks have a desired host that can be placed on,
		// the other half have a desired host that cannot have the task get placed
		if i < createTasks/2 {
			assignment.GetTask().GetTask().DesiredHost = fmt.Sprintf("hostname-%d", i)
		} else {
			assignment.GetTask().GetTask().DesiredHost = fmt.Sprintf("nonexist-%d", i)
		}
		assignments = append(assignments, assignment)
	}

	var hosts []models.Offer
	for i := 0; i < createHosts; i++ {
		host := testutil.SetupHostOffers()
		host.Offer.Hostname = fmt.Sprintf("hostname-%d", i)
		hosts = append(hosts, host)
	}

	mockOfferService.EXPECT().Acquire(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).Return(hosts, _testReason).MinTimes(1)

	mockTaskService.EXPECT().
		Dequeue(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Times(1).
		Return(assignments)
	mockTaskService.EXPECT().SetPlacements(
		gomock.Any(),
		gomock.Any(),
		gomock.Any()).
		Return()

	engine.config.Concurrency = 1
	placer := algorithms.NewPlacer(4, 300)
	engine.strategy = mimir.New(placer, engine.config)
	unfulfilledAssignment, _ := engine.Place(context.Background(), nil)
	engine.pool.WaitUntilProcessed()

	// half of the tasks cannot get fulfilled
	assert.Len(t, unfulfilledAssignment, createTasks/2)
}
func TestEnginePlaceSubsetOfTasksDueToInsufficientResources(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, _, _ := setupEngine(t)
	defer ctrl.Finish()
	createTasks := 25
	createHosts := 10

	engine.config.MaxPlacementDuration = time.Second
	deadline := time.Now().Add(time.Second)
	var assignments []models.Task
	for i := 0; i < createTasks; i++ {
		assignment := testutil.SetupAssignment(deadline, 1)
		assignments = append(assignments, assignment)
	}
	var hosts []models.Offer
	for i := 0; i < createHosts; i++ {
		hosts = append(hosts, testutil.SetupHostOffers())
	}

	gomock.InOrder(
		mockOfferService.EXPECT().Acquire(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(hosts, _testReason).Times(1),
		mockOfferService.EXPECT().Acquire(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(nil, _testReason).AnyTimes(),
	)

	mockTaskService.EXPECT().
		Dequeue(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Times(1).
		Return(assignments)
	mockTaskService.EXPECT().SetPlacements(
		gomock.Any(),
		gomock.Any(),
		gomock.Any()).
		Return().AnyTimes()

	engine.strategy = batch.New(&config.PlacementConfig{})
	engine.Place(context.Background(), nil)
	engine.pool.WaitUntilProcessed()

	var success, failed int
	for _, assignment := range assignments {
		if assignment.GetPlacement() != nil {
			success++
		} else {
			failed++
		}
	}
	assert.Equal(t, 10, success)
	assert.Equal(t, 15, failed)
}

// Test tasks cannot get placed due to no host offer.
func TestEnginePlaceNoHostsMakesTaskExceedDeadline(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, _, _ := setupEngine(t)
	defer ctrl.Finish()
	engine.config.MaxPlacementDuration = time.Millisecond
	assignment := testutil.SetupAssignment(time.Now().Add(time.Millisecond), 1)
	assignments := []models.Task{assignment}

	mockOfferService.EXPECT().
		Acquire(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).MinTimes(1).
		Return(nil, _testReason)

	mockTaskService.EXPECT().
		SetPlacements(
			gomock.Any(),
			nil,
			gomock.Any(),
		).Times(1).
		Return()

	needs := plugins.PlacementNeeds{}
	engine.placeAssignmentGroup(context.Background(), needs, assignments)
}

func TestEnginePlaceTaskExceedMaxRoundsAndGetsPlaced(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, mockStrategy, _ := setupEngine(t)
	defer ctrl.Finish()
	maxRounds := 5
	engine.config.MaxPlacementDuration = 1 * time.Second

	host := testutil.SetupHostOffers()
	offers := []models.Offer{host}
	assignment := testutil.SetupAssignment(time.Now().Add(1*time.Second), maxRounds)
	assignment.SetPlacement(host)
	assignments := []models.Task{assignment}

	mockStrategy.EXPECT().
		GetTaskPlacements(
			gomock.Any(),
			gomock.Any(),
		).
		Times(maxRounds).
		Return(map[int]int{})

	mockStrategy.EXPECT().
		ConcurrencySafe().
		Return(true).
		Times(maxRounds - 1)

	mockTaskService.EXPECT().
		SetPlacements(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).MinTimes(1).
		Return()

	mockOfferService.EXPECT().
		Acquire(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).MinTimes(1).
		Return(offers, _testReason)

	needs := plugins.PlacementNeeds{}
	engine.placeAssignmentGroup(context.Background(), needs, assignments)
}

func TestEnginePlaceTaskExceedMaxPlacementDeadlineGetsPlaced(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, mockStrategy, _ := setupEngine(t)
	defer ctrl.Finish()
	engine.config.MaxPlacementDuration = 1 * time.Second

	host := testutil.SetupHostOffers()
	offers := []models.Offer{host}
	assignment := testutil.SetupAssignment(time.Now().Add(1*time.Second), 10)
	assignment.Task.Task.DesiredHost = "desired-host"
	assignment.Task.PlacementDeadline = time.Now().Add(-1 * time.Second)
	assignment.SetPlacement(host)
	assignments := []models.Task{assignment}

	mockStrategy.EXPECT().
		GetTaskPlacements(
			gomock.Any(),
			gomock.Any(),
		).
		Return(map[int]int{})

	mockTaskService.EXPECT().
		SetPlacements(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).MinTimes(1).
		Return()

	mockOfferService.EXPECT().
		Acquire(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).MinTimes(1).
		Return(offers, _testReason)

	needs := plugins.PlacementNeeds{}
	engine.placeAssignmentGroup(context.Background(), needs, assignments)
}

func TestEnginePlaceCallToStrategy(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, mockStrategy, _ := setupEngine(t)
	defer ctrl.Finish()
	engine.config.MaxPlacementDuration = 100 * time.Millisecond

	host := testutil.SetupHostOffers()
	hosts := []models.Offer{host}
	assignment := testutil.SetupAssignment(time.Now(), 1)
	assignment.SetPlacement(host)
	assignment2 := testutil.SetupAssignment(time.Now(), 1)
	assignment2.SetPlacement(host)
	assignment2.Task.Task.Revocable = true
	assignments := []models.Task{assignment, assignment2}

	mockTaskService.EXPECT().
		Dequeue(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(
			assignments,
		)

	mockOfferService.EXPECT().
		Acquire(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).MinTimes(1).
		Return(
			hosts,
			_testReason,
		)

	mockStrategy.EXPECT().
		GetTaskPlacements(
			gomock.Any(),
			gomock.Any()).
		AnyTimes().
		Return(map[int]int{})

	mockStrategy.EXPECT().
		GroupTasksByPlacementNeeds(
			gomock.Any()).
		Return([]*plugins.TasksByPlacementNeeds{
			{PlacementNeeds: plugins.PlacementNeeds{}, Tasks: []int{0}},
		})

	mockStrategy.EXPECT().
		GroupTasksByPlacementNeeds(
			gomock.Any()).
		Return([]*plugins.TasksByPlacementNeeds{
			{PlacementNeeds: plugins.PlacementNeeds{}, Tasks: []int{0}},
		})

	mockStrategy.EXPECT().
		ConcurrencySafe().
		AnyTimes().
		Return(false)

	mockTaskService.EXPECT().
		SetPlacements(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).AnyTimes().
		Return()

	mockTaskService.EXPECT().
		SetPlacements(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).AnyTimes().
		Return()

	mockOfferService.EXPECT().
		Release(
			gomock.Any(),
			gomock.Any(),
		).AnyTimes().
		Return()

	_, delay := engine.Place(context.Background(), nil)
	assert.Equal(t, time.Duration(0), delay)
}

func TestEnginePlaceReservedTasks(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, _, _ := setupEngine(t)
	defer ctrl.Finish()
	createTasks := 25
	createHosts := 10

	engine.config.MaxPlacementDuration = time.Second
	deadline := time.Now().Add(time.Second)

	var assignments []models.Task
	for i := 0; i < createTasks; i++ {
		assignment := testutil.SetupAssignment(deadline, 1)
		assignment.GetTask().GetTask().Resource.CpuLimit = 5
		if i == 0 || i == 10 {
			assignment.GetTask().Task.ReadyForHostReservation = true
		}
		assignments = append(assignments, assignment)
	}

	var hosts []models.Offer
	for i := 0; i < createHosts; i++ {
		hosts = append(hosts, testutil.SetupHostOffers())
	}

	mockOfferService.EXPECT().Acquire(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).Return(hosts, _testReason).MinTimes(1)
	mockOfferService.EXPECT().Release(
		gomock.Any(),
		gomock.Any()).
		Return()

	mockTaskService.EXPECT().
		Dequeue(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Times(1).
		Return(assignments)
	mockTaskService.EXPECT().SetPlacements(
		gomock.Any(),
		gomock.Any(),
		gomock.Any()).
		Return()

	// Test assignments ready for host reservation
	engine.strategy = batch.New(&config.PlacementConfig{})
	engine.Place(context.Background(), nil)
	engine.pool.WaitUntilProcessed()

	var success, failed int
	for _, assignment := range assignments {
		if !assignment.IsReadyForHostReservation() {
			if assignment.GetPlacement() != nil {
				success++
			} else {
				failed++
			}
		}
	}

	assert.Equal(t, 2, engine.reserver.GetReservationQueue().Length())
	assert.Equal(t, createTasks-2, success)
	assert.Equal(t, 0, failed)
}

func TestEngineFindUsedOffers(t *testing.T) {
	ctrl, engine, _, _, _, _ := setupEngine(t)
	defer ctrl.Finish()

	now := time.Now()
	deadline := now.Add(30 * time.Second)
	assignment := testutil.SetupAssignment(deadline, 1)
	assignments := []models.Task{
		assignment,
	}
	used := engine.findUsedHosts(assignments)
	assert.Equal(t, 0, len(used))

	host := testutil.SetupHostOffers()
	assignment.SetPlacement(host)
	used = engine.findUsedHosts(assignments)
	assert.Equal(t, 1, len(used))
	assert.Equal(t, host, used[0])
}

func TestEngineFilterAssignments(t *testing.T) {
	ctrl, engine, _, _, _, _ := setupEngine(t)
	defer ctrl.Finish()

	deadline1 := time.Now()
	now := deadline1.Add(1 * time.Second)
	deadline2 := now.Add(30 * time.Second)
	host := testutil.SetupHostOffers()

	assignment1 := testutil.SetupAssignment(deadline1, 1) // assigned
	assignment1.SetPlacement(host)

	assignment2 := testutil.SetupAssignment(deadline2, 2) // assigned
	assignment2.SetPlacement(host)

	assignment3 := testutil.SetupAssignment(deadline2, 1) // retryable

	assignment4 := testutil.SetupAssignment(deadline1, 1) // unassigned

	assignment5 := testutil.SetupAssignment(deadline2, 2) // assigned
	assignment5.Task.Task.DesiredHost = host.GetOffer().GetHostname()
	assignment5.SetPlacement(host)

	assignment6 := testutil.SetupAssignment(deadline2, 2) // retryable
	assignment6.Task.Task.DesiredHost = "another-host"
	assignment6.SetPlacement(host)

	assignments := []models.Task{
		assignment1,
		assignment2,
		assignment3,
		assignment4,
		assignment5,
		assignment6,
	}

	assigned, retryable, unassigned := engine.filterAssignments(now, assignments)
	assert.Equal(t, 3, len(assigned))
	assert.Equal(t, []models.Task{assignment1, assignment5, assignment2}, assigned)
	assert.Equal(t, 2, len(retryable))
	assert.Equal(t, []models.Task{assignment3, assignment6}, retryable)
	assert.Equal(t, 1, len(unassigned))
	assert.Equal(t, []models.Task{assignment4}, unassigned)
}

func TestEngineCleanup(t *testing.T) {
	ctrl, engine, _, mockTaskService, _, _ := setupEngine(t)
	defer ctrl.Finish()

	host := testutil.SetupHostOffers()
	hosts := []models.Offer{host}
	assignment := testutil.SetupAssignment(time.Now(), 1)
	assignment.SetPlacement(host)
	assignments := []models.Task{assignment}

	mockTaskService.EXPECT().
		SetPlacements(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return()

	engine.cleanup(context.Background(), assignments, nil, assignments, hosts)
}

func TestEngineFindUnusedOffers(t *testing.T) {
	ctrl, engine, _, _, _, _ := setupEngine(t)
	defer ctrl.Finish()

	deadline := time.Now().Add(30 * time.Second)
	assignment1 := testutil.SetupAssignment(deadline, 1)
	assignment2 := testutil.SetupAssignment(deadline, 1)
	assignment3 := testutil.SetupAssignment(deadline, 1)
	assignment4 := testutil.SetupAssignment(deadline, 1)
	assignments := []models.Task{
		assignment1,
		assignment2,
	}
	retryable := []models.Task{
		assignment3,
		assignment4,
	}
	host1 := testutil.SetupHostOffers()
	host2 := testutil.SetupHostOffers()
	assignment1.SetPlacement(host1)
	assignment2.SetPlacement(host1)
	assignment3.SetPlacement(host1)
	offers := []models.Offer{
		host1,
		host2,
	}

	unused := engine.findUnusedHosts(
		assignments,
		retryable,
		offers,
	)
	assert.Equal(t, 1, len(unused))
	assert.Equal(t, host2, unused[0])
}
