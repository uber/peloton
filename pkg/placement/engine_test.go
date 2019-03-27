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
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/uber/peloton/pkg/common/async"
	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/models"
	offers_mock "github.com/uber/peloton/pkg/placement/offers/mocks"
	"github.com/uber/peloton/pkg/placement/plugins/batch"
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

func setupEngine(t *testing.T) (
	*gomock.Controller,
	*engine, *offers_mock.MockService,
	*tasks_mock.MockService,
	*mocks.MockStrategy) {
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
	pool := async.NewPool(async.PoolOptions{}, nil)
	pool.Start()

	e := New(
		tally.NoopScope,
		config,
		mockOfferService,
		mockTaskService,
		nil,
		mockStrategy,
		pool,
	)

	return ctrl, e.(*engine), mockOfferService, mockTaskService, mockStrategy
}

func TestEnginePlaceNoTasksToPlace(t *testing.T) {
	ctrl, engine, _, mockTaskService, _ := setupEngine(t)
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

	delay := engine.Place(context.Background())
	assert.True(t, delay > time.Duration(0))
}

func TestEnginePlaceMultipleTasks(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, _ := setupEngine(t)
	defer ctrl.Finish()
	createTasks := 25
	createHosts := 10

	engine.config.MaxPlacementDuration = time.Second
	deadline := time.Now().Add(time.Second)

	var assignments []*models.Assignment
	for i := 0; i < createTasks; i++ {
		assignment := testutil.SetupAssignment(deadline, 1)
		assignment.GetTask().GetTask().Resource.CpuLimit = 5
		assignments = append(assignments, assignment)
	}

	var hosts []*models.HostOffers
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

	engine.strategy = batch.New()
	engine.Place(context.Background())
	engine.pool.WaitUntilProcessed()

	var success, failed int
	for _, assignment := range assignments {
		if assignment.GetHost() != nil {
			success++
		} else {
			failed++
		}
	}

	assert.Equal(t, createTasks, success)
	assert.Equal(t, 0, failed)
}

func TestEnginePlaceSubsetOfTasksDueToInsufficientResources(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, _ := setupEngine(t)
	defer ctrl.Finish()
	createTasks := 25
	createHosts := 10

	engine.config.MaxPlacementDuration = time.Second
	deadline := time.Now().Add(time.Second)
	var assignments []*models.Assignment
	for i := 0; i < createTasks; i++ {
		assignment := testutil.SetupAssignment(deadline, 1)
		assignments = append(assignments, assignment)
	}
	var hosts []*models.HostOffers
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

	engine.strategy = batch.New()
	engine.Place(context.Background())
	engine.pool.WaitUntilProcessed()

	var success, failed int
	for _, assignment := range assignments {
		if assignment.GetHost() != nil {
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
	ctrl, engine, mockOfferService, mockTaskService, _ := setupEngine(t)
	defer ctrl.Finish()
	engine.config.MaxPlacementDuration = time.Millisecond
	assignment := testutil.SetupAssignment(time.Now().Add(time.Millisecond), 1)
	assignments := []*models.Assignment{assignment}

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

	filter := &hostsvc.HostFilter{}
	engine.placeAssignmentGroup(context.Background(), filter, assignments)
}

func TestEnginePlaceTaskExceedMaxRoundsAndGetsPlaced(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, mockStrategy := setupEngine(t)
	defer ctrl.Finish()
	engine.config.MaxPlacementDuration = 1 * time.Second

	host := testutil.SetupHostOffers()
	offers := []*models.HostOffers{host}
	assignment := testutil.SetupAssignment(time.Now().Add(1*time.Second), 5)
	assignment.SetHost(host)
	assignments := []*models.Assignment{assignment}

	mockStrategy.EXPECT().
		PlaceOnce(
			gomock.Any(),
			gomock.Any(),
		).
		Times(5).
		Return()

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

	filter := &hostsvc.HostFilter{}
	engine.placeAssignmentGroup(context.Background(), filter, assignments)
}

func TestEnginePlaceTaskExceedMaxPlacementDeadlineGetsPlaced(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, mockStrategy := setupEngine(t)
	defer ctrl.Finish()
	engine.config.MaxPlacementDuration = 1 * time.Second

	host := testutil.SetupHostOffers()
	offers := []*models.HostOffers{host}
	assignment := testutil.SetupAssignment(time.Now().Add(1*time.Second), 10)
	assignment.Task.Task.DesiredHost = "desired-host"
	assignment.Task.PlacementDeadline = time.Now().Add(-1 * time.Second)
	assignment.SetHost(host)
	assignments := []*models.Assignment{assignment}

	mockStrategy.EXPECT().
		PlaceOnce(
			gomock.Any(),
			gomock.Any(),
		).
		Return()

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

	filter := &hostsvc.HostFilter{}
	engine.placeAssignmentGroup(context.Background(), filter, assignments)
}

func TestEnginePlaceCallToStrategy(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, mockStrategy := setupEngine(t)
	defer ctrl.Finish()
	engine.config.MaxPlacementDuration = 100 * time.Millisecond

	host := testutil.SetupHostOffers()
	hosts := []*models.HostOffers{host}
	assignment := testutil.SetupAssignment(time.Now(), 1)
	assignment.SetHost(host)
	assignment2 := testutil.SetupAssignment(time.Now(), 1)
	assignment2.SetHost(host)
	assignment2.Task.Task.Revocable = true
	assignments := []*models.Assignment{assignment, assignment2}

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
		PlaceOnce(
			gomock.Any(),
			gomock.Any()).
		AnyTimes().
		Return()

	mockStrategy.EXPECT().
		Filters(
			gomock.Any()).
		Return(map[*hostsvc.HostFilter][]*models.Assignment{nil: {assignment}})

	mockStrategy.EXPECT().
		Filters(
			gomock.Any()).
		Return(map[*hostsvc.HostFilter][]*models.Assignment{nil: {assignment2}})

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

	delay := engine.Place(context.Background())
	assert.Equal(t, time.Duration(0), delay)
}

func TestEnginePlaceReservedTasks(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, _ := setupEngine(t)
	defer ctrl.Finish()
	createTasks := 25
	createHosts := 10

	engine.config.MaxPlacementDuration = time.Second
	deadline := time.Now().Add(time.Second)

	var assignments []*models.Assignment
	for i := 0; i < createTasks; i++ {
		assignment := testutil.SetupAssignment(deadline, 1)
		assignment.GetTask().GetTask().Resource.CpuLimit = 5
		assignments = append(assignments, assignment)
	}
	assignments[0].GetTask().Task.ReadyForHostReservation = true
	assignments[10].GetTask().Task.ReadyForHostReservation = true

	var hosts []*models.HostOffers
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
	engine.strategy = batch.New()
	engine.Place(context.Background())
	engine.pool.WaitUntilProcessed()

	var success, failed int
	for _, assignment := range assignments {
		if !assignment.GetTask().Task.ReadyForHostReservation {
			if assignment.GetHost() != nil {
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
	ctrl, engine, _, _, _ := setupEngine(t)
	defer ctrl.Finish()

	now := time.Now()
	deadline := now.Add(30 * time.Second)
	assignment := testutil.SetupAssignment(deadline, 1)
	assignments := []*models.Assignment{
		assignment,
	}
	used := engine.findUsedHosts(assignments)
	assert.Equal(t, 0, len(used))

	host := testutil.SetupHostOffers()
	assignment.SetHost(host)
	used = engine.findUsedHosts(assignments)
	assert.Equal(t, 1, len(used))
	assert.Equal(t, host, used[0])
}

func TestEngineFilterAssignments(t *testing.T) {
	ctrl, engine, _, _, _ := setupEngine(t)
	defer ctrl.Finish()

	deadline1 := time.Now()
	now := deadline1.Add(1 * time.Second)
	deadline2 := now.Add(30 * time.Second)
	host := testutil.SetupHostOffers()

	assignment1 := testutil.SetupAssignment(deadline1, 1) // assigned
	assignment1.SetHost(host)

	assignment2 := testutil.SetupAssignment(deadline2, 2) // assigned
	assignment2.SetHost(host)

	assignment3 := testutil.SetupAssignment(deadline2, 1) // retryable

	assignment4 := testutil.SetupAssignment(deadline1, 1) // unassigned

	assignment5 := testutil.SetupAssignment(deadline2, 2) // assigned
	assignment5.Task.Task.DesiredHost = host.GetOffer().GetHostname()
	assignment5.SetHost(host)

	assignment6 := testutil.SetupAssignment(deadline2, 2) // retryable
	assignment6.Task.Task.DesiredHost = "another-host"
	assignment6.SetHost(host)

	assignments := []*models.Assignment{
		assignment1,
		assignment2,
		assignment3,
		assignment4,
		assignment5,
		assignment6,
	}

	assigned, retryable, unassigned := engine.filterAssignments(now, assignments)
	assert.Equal(t, 3, len(assigned))
	assert.Equal(t, []*models.Assignment{assignment1, assignment5, assignment2}, assigned)
	assert.Equal(t, 2, len(retryable))
	assert.Equal(t, []*models.Assignment{assignment3, assignment6}, retryable)
	assert.Equal(t, 1, len(unassigned))
	assert.Equal(t, []*models.Assignment{assignment4}, unassigned)
}

func TestEngineCleanup(t *testing.T) {
	ctrl, engine, _, mockTaskService, _ := setupEngine(t)
	defer ctrl.Finish()

	host := testutil.SetupHostOffers()
	hosts := []*models.HostOffers{host}
	assignment := testutil.SetupAssignment(time.Now(), 1)
	assignment.SetHost(host)
	assignments := []*models.Assignment{assignment}

	mockTaskService.EXPECT().
		SetPlacements(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return()

	engine.cleanup(context.Background(), assignments, nil, assignments, hosts)
}

func TestEngineCreatePlacement(t *testing.T) {
	ctrl, engine, _, _, _ := setupEngine(t)
	defer ctrl.Finish()

	now := time.Now()
	deadline := now.Add(30 * time.Second)
	host := testutil.SetupHostOffers()
	assignment1 := testutil.SetupAssignment(deadline, 1)
	assignment1.SetHost(host)
	assignment2 := testutil.SetupAssignment(deadline, 1)
	assignments := []*models.Assignment{
		assignment1,
		assignment2,
	}

	placements := engine.createPlacement(assignments)
	assert.Equal(t, 1, len(placements))
	assert.Equal(t, host.GetOffer().GetHostname(), placements[0].GetHostname())
	assert.Equal(t, host.GetOffer().GetId(), placements[0].GetHostOfferID())
	assert.Equal(t, host.GetOffer().AgentId, placements[0].GetAgentId())
	assert.Equal(t,
		assignment1.GetTask().GetTask().GetType(),
		placements[0].GetType())
	assert.Equal(t,
		[]*resmgr.Placement_Task{
			{
				PelotonTaskID: assignment1.GetTask().GetTask().GetId(),
				MesosTaskID:   assignment1.GetTask().GetTask().GetTaskId(),
			},
		}, placements[0].GetTaskIDs())
	assert.Equal(t, 3, len(placements[0].GetPorts()))
}

func TestEngineFindUnusedOffers(t *testing.T) {
	ctrl, engine, _, _, _ := setupEngine(t)
	defer ctrl.Finish()

	deadline := time.Now().Add(30 * time.Second)
	assignment1 := testutil.SetupAssignment(deadline, 1)
	assignment2 := testutil.SetupAssignment(deadline, 1)
	assignment3 := testutil.SetupAssignment(deadline, 1)
	assignment4 := testutil.SetupAssignment(deadline, 1)
	assignments := []*models.Assignment{
		assignment1,
		assignment2,
	}
	retryable := []*models.Assignment{
		assignment3,
		assignment4,
	}
	host1 := testutil.SetupHostOffers()
	host2 := testutil.SetupHostOffers()
	assignment1.SetHost(host1)
	assignment2.SetHost(host1)
	assignment3.SetHost(host1)
	offers := []*models.HostOffers{
		host1,
		host2,
	}

	unused := engine.findUnusedHosts(assignments, retryable, offers)
	assert.Equal(t, 1, len(unused))
	assert.Equal(t, host2, unused[0])
}
