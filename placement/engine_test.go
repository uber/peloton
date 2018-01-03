package placement

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/common/async"
	"code.uber.internal/infra/peloton/placement/config"
	"code.uber.internal/infra/peloton/placement/metrics"
	"code.uber.internal/infra/peloton/placement/models"
	offers_mock "code.uber.internal/infra/peloton/placement/offers/mocks"
	"code.uber.internal/infra/peloton/placement/plugins/batch"
	"code.uber.internal/infra/peloton/placement/plugins/mocks"
	tasks_mock "code.uber.internal/infra/peloton/placement/tasks/mocks"
	"code.uber.internal/infra/peloton/placement/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func setupEngine(t *testing.T) (*gomock.Controller, *engine, *offers_mock.MockService,
	*tasks_mock.MockService, *mocks.MockStrategy) {
	ctrl := gomock.NewController(t)

	metrics := metrics.NewMetrics(tally.NoopScope)
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
	e := NewEngine(config, mockOfferService, mockTaskService, mockStrategy, async.NewPool(async.PoolOptions{}), metrics)
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

type mockService struct {
	lockAssignments sync.Mutex
	lockHosts       sync.Mutex
	assignments     []*models.Assignment
	hosts           []*models.Host
}

func (s *mockService) Acquire(ctx context.Context, fetchTasks bool, taskType resmgr.TaskType, filter *hostsvc.HostFilter) (offers []*models.Host) {
	s.lockHosts.Lock()
	defer s.lockHosts.Unlock()
	result := s.hosts
	s.hosts = nil
	return result
}

func (s *mockService) Dequeue(ctx context.Context, taskType resmgr.TaskType, batchSize int, timeout int) (assignments []*models.Assignment) {
	s.lockAssignments.Lock()
	defer s.lockAssignments.Unlock()
	result := s.assignments
	s.assignments = nil
	return result
}

func (s *mockService) Release(ctx context.Context, offers []*models.Host) {}

func (s *mockService) Enqueue(ctx context.Context, assignments []*models.Assignment) {}

func (s *mockService) SetPlacements(ctx context.Context, placements []*resmgr.Placement) {}

func TestEnginePlaceMultipleTasks(t *testing.T) {
	ctrl, engine, _, _, _ := setupEngine(t)
	defer ctrl.Finish()
	createTasks := 25
	createHosts := 10

	engine.config.MaxPlacementDuration = time.Second
	deadline := time.Now().Add(time.Second)
	var assignments []*models.Assignment
	for i := 0; i < createTasks; i++ {
		assignment := testutil.SetupAssignment(deadline, 1)
		assignment.Task().Task().Resource.CpuLimit = 5
		assignments = append(assignments, assignment)
	}
	var hosts []*models.Host
	for i := 0; i < createHosts; i++ {
		hosts = append(hosts, testutil.SetupHost())
	}
	mockService := &mockService{
		assignments: assignments,
		hosts:       hosts,
	}
	engine.taskService = mockService
	engine.offerService = mockService

	engine.strategy = batch.New()

	engine.Place(context.Background())
	engine.pool.WaitUntilProcessed()

	var success, failed int
	for _, assignment := range assignments {
		if assignment.Host() != nil {
			success++
		} else {
			failed++
		}
	}
	assert.Equal(t, createTasks, success)
	assert.Equal(t, 0, failed)
}

func TestEnginePlaceSubsetOfTasksDueToInsufficientResources(t *testing.T) {
	ctrl, engine, _, _, _ := setupEngine(t)
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
	var hosts []*models.Host
	for i := 0; i < createHosts; i++ {
		hosts = append(hosts, testutil.SetupHost())
	}
	mockService := &mockService{
		assignments: assignments,
		hosts:       hosts,
	}
	engine.taskService = mockService
	engine.offerService = mockService

	engine.strategy = batch.New()

	engine.Place(context.Background())
	engine.pool.WaitUntilProcessed()

	var success, failed int
	for _, assignment := range assignments {
		if assignment.Host() != nil {
			success++
		} else {
			failed++
		}
	}
	assert.Equal(t, 10, success)
	assert.Equal(t, 15, failed)
}

func TestEnginePlaceNoHostsMakesTaskExceedDeadline(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, _ := setupEngine(t)
	defer ctrl.Finish()
	engine.config.MaxPlacementDuration = time.Millisecond
	assignment := testutil.SetupAssignment(time.Now().Add(time.Millisecond), 1)
	assignments := []*models.Assignment{assignment}

	mockTaskService.EXPECT().
		Enqueue(
			gomock.Any(),
			gomock.Any(),
		).MinTimes(1).
		Return()

	mockTaskService.EXPECT().
		SetPlacements(
			gomock.Any(),
			gomock.Any(),
		).MaxTimes(0).
		Return()

	mockOfferService.EXPECT().
		Acquire(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).MinTimes(1).
		Return(nil)

	mockOfferService.EXPECT().
		Release(
			gomock.Any(),
			gomock.Any(),
		).
		MaxTimes(0).
		Return()

	filter := &hostsvc.HostFilter{}
	engine.placeAssignmentGroup(context.Background(), filter, assignments)
}

func TestEnginePlaceTaskExceedMaxRoundsAndGetsPlaced(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, mockStrategy := setupEngine(t)
	defer ctrl.Finish()
	engine.config.MaxPlacementDuration = 1 * time.Second

	offer := testutil.SetupHost()
	offers := []*models.Host{offer}
	assignment := testutil.SetupAssignment(time.Now().Add(1*time.Second), 5)
	assignment.SetHost(offer)
	assignments := []*models.Assignment{assignment}

	mockStrategy.EXPECT().
		PlaceOnce(
			gomock.Any(),
			gomock.Any(),
		).
		Times(5).
		Return()

	mockTaskService.EXPECT().
		Enqueue(
			gomock.Any(),
			gomock.Any(),
		).MinTimes(1).
		Return()

	mockTaskService.EXPECT().
		SetPlacements(
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
		Return(offers)

	mockOfferService.EXPECT().
		Release(
			gomock.Any(),
			gomock.Any(),
		).
		MinTimes(1).
		Return()

	filter := &hostsvc.HostFilter{}
	engine.placeAssignmentGroup(context.Background(), filter, assignments)
}

func TestEnginePlaceCallToStrategy(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, mockStrategy := setupEngine(t)
	defer ctrl.Finish()
	engine.config.MaxPlacementDuration = 100 * time.Millisecond

	offer := testutil.SetupHost()
	offers := []*models.Host{offer}
	assignment := testutil.SetupAssignment(time.Now(), 1)
	assignment.SetHost(offer)
	assignments := []*models.Assignment{assignment}

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
			offers,
		)

	mockStrategy.EXPECT().
		PlaceOnce(
			gomock.Any(),
			gomock.Any(),
		).
		Return()

	mockStrategy.EXPECT().
		Filters(
			gomock.Any(),
		).MinTimes(1).
		Return(map[*hostsvc.HostFilter][]*models.Assignment{nil: assignments})

	mockStrategy.EXPECT().
		ConcurrencySafe().
		AnyTimes().
		Return(false)

	mockTaskService.EXPECT().
		Enqueue(
			gomock.Any(),
			gomock.Any(),
		).AnyTimes().
		Return()

	mockTaskService.EXPECT().
		SetPlacements(
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

	offer := testutil.SetupHost()
	assignment.SetHost(offer)
	used = engine.findUsedHosts(assignments)
	assert.Equal(t, 1, len(used))
	assert.Equal(t, offer, used[0])
}

func TestEngineFilterAssignments(t *testing.T) {
	ctrl, engine, _, _, _ := setupEngine(t)
	defer ctrl.Finish()

	deadline1 := time.Now()
	now := deadline1.Add(1 * time.Second)
	deadline2 := now.Add(30 * time.Second)
	offer := testutil.SetupHost()

	assignment1 := testutil.SetupAssignment(deadline1, 1) // assigned
	assignment1.SetHost(offer)

	assignment2 := testutil.SetupAssignment(deadline2, 2) // retryable
	assignment2.SetHost(offer)

	assignment3 := testutil.SetupAssignment(deadline2, 1) // retryable

	assignment4 := testutil.SetupAssignment(deadline1, 1) // unassigned

	assignments := []*models.Assignment{
		assignment1,
		assignment2,
		assignment3,
		assignment4,
	}

	assigned, retryable, unassigned := engine.filterAssignments(now, assignments)
	assert.Equal(t, 1, len(assigned))
	assert.Equal(t, []*models.Assignment{assignment1}, assigned)
	assert.Equal(t, 2, len(retryable))
	assert.Equal(t, []*models.Assignment{assignment2, assignment3}, retryable)
	assert.Equal(t, 1, len(unassigned))
	assert.Equal(t, []*models.Assignment{assignment4}, unassigned)
}

func TestEngineCleanup(t *testing.T) {
	ctrl, engine, mockOfferService, mockTaskService, _ := setupEngine(t)
	defer ctrl.Finish()

	offer := testutil.SetupHost()
	offers := []*models.Host{offer}
	assignment := testutil.SetupAssignment(time.Now(), 1)
	assignment.SetHost(offer)
	assignments := []*models.Assignment{assignment}

	mockTaskService.EXPECT().
		Enqueue(
			gomock.Any(),
			gomock.Any(),
		).
		Return()
	mockTaskService.EXPECT().
		SetPlacements(
			gomock.Any(),
			gomock.Any(),
		).
		Return()

	mockOfferService.EXPECT().
		Release(
			gomock.Any(),
			gomock.Any(),
		).
		Return()

	engine.cleanup(context.Background(), assignments, nil, nil, offers)
}

func TestEngineCreatePlacement(t *testing.T) {
	ctrl, engine, _, _, _ := setupEngine(t)
	defer ctrl.Finish()

	now := time.Now()
	deadline := now.Add(30 * time.Second)
	offer := testutil.SetupHost()
	assignment1 := testutil.SetupAssignment(deadline, 1)
	assignment1.SetHost(offer)
	assignment2 := testutil.SetupAssignment(deadline, 1)
	assignments := []*models.Assignment{
		assignment1,
		assignment2,
	}

	placements := engine.createPlacement(assignments)
	assert.Equal(t, 1, len(placements))
	assert.Equal(t, offer.Offer().Hostname, placements[0].Hostname)
	assert.Equal(t, offer.Offer().AgentId, placements[0].AgentId)
	assert.Equal(t, assignment1.Task().Task().Type, placements[0].Type)
	assert.Equal(t, []*peloton.TaskID{assignment1.Task().Task().Id}, placements[0].Tasks)
	assert.Equal(t, 3, len(placements[0].Ports))
}

func TestEngineAssignPortsAllFromASingleRange(t *testing.T) {
	ctrl, engine, _, _, _ := setupEngine(t)
	defer ctrl.Finish()

	now := time.Now()
	deadline := now.Add(30 * time.Second)
	assignment1 := testutil.SetupAssignment(deadline, 1)
	assignment2 := testutil.SetupAssignment(deadline, 1)
	tasks := []*models.Task{
		assignment1.Task(),
		assignment2.Task(),
	}
	offer := testutil.SetupHost()

	ports := engine.assignPorts(offer, tasks)
	assert.Equal(t, 6, len(ports))
	assert.Equal(t, uint32(31000), ports[0])
	assert.Equal(t, uint32(31001), ports[1])
	assert.Equal(t, uint32(31002), ports[2])
	assert.Equal(t, uint32(31003), ports[3])
	assert.Equal(t, uint32(31004), ports[4])
	assert.Equal(t, uint32(31005), ports[5])
}

func TestEngineAssignPortsFromMultipleRanges(t *testing.T) {
	ctrl, engine, _, _, _ := setupEngine(t)
	defer ctrl.Finish()

	now := time.Now()
	deadline := now.Add(30 * time.Second)
	assignment1 := testutil.SetupAssignment(deadline, 1)
	assignment2 := testutil.SetupAssignment(deadline, 1)
	tasks := []*models.Task{
		assignment1.Task(),
		assignment2.Task(),
	}
	offer := testutil.SetupHost()
	*offer.Offer().Resources[4].Ranges.Range[0].End = uint64(31001)
	begin, end := uint64(31002), uint64(31009)
	offer.Offer().Resources[4].Ranges.Range = append(
		offer.Offer().Resources[4].Ranges.Range, &mesos_v1.Value_Range{
			Begin: &begin,
			End:   &end,
		})

	ports := engine.assignPorts(offer, tasks)
	intPorts := make([]int, len(ports))
	for i := range ports {
		intPorts[i] = int(ports[i])
	}
	sort.Ints(intPorts)
	assert.Equal(t, 6, len(ports))
	assert.Equal(t, 31000, intPorts[0])
	assert.Equal(t, 31001, intPorts[1])
	assert.Equal(t, 31002, intPorts[2])
	assert.Equal(t, 31003, intPorts[3])
	assert.Equal(t, 31004, intPorts[4])
	assert.Equal(t, 31005, intPorts[5])
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
	offer1 := testutil.SetupHost()
	offer2 := testutil.SetupHost()
	assignment1.SetHost(offer1)
	assignment2.SetHost(offer1)
	assignment3.SetHost(offer1)
	offers := []*models.Host{
		offer1,
		offer2,
	}

	unused := engine.findUnusedHosts(assignments, retryable, offers)
	assert.Equal(t, 1, len(unused))
	assert.Equal(t, offer2, unused[0])
}
