package placement

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	"code.uber.internal/infra/peloton/common/async"
	"code.uber.internal/infra/peloton/util"
)

// TODO: load from test configs
const (
	taskIDFmt   = "testjob-%d-abcdef12-abcd-1234-5678-1234567890ab"
	testJobName = "testjob"
	// a fake maxPlacementDuration to ensure that placementLoop doesn't complete immediately
	maxPlacementDuration = time.Duration(10) * time.Second
	taskDequeueTimeout   = 100
)

var (
	defaultResourceConfig = task.ResourceConfig{
		CpuLimit:    10,
		MemLimitMb:  10,
		DiskLimitMb: 10,
		FdLimit:     10,
	}

	// a lock used for synchronizing data access between mock and goroutine
	// being tested.
	lock = sync.RWMutex{}

	andConstraint = task.Constraint{
		Type: task.Constraint_AND_CONSTRAINT,
	}

	orConstraint = task.Constraint{
		Type: task.Constraint_OR_CONSTRAINT,
	}
)

func createTestTask(instanceID int) *resmgr.Task {
	var tid = fmt.Sprintf(taskIDFmt, instanceID)
	resCfg := proto.Clone(&defaultResourceConfig).(*task.ResourceConfig)
	return &resmgr.Task{
		JobId: &peloton.JobID{
			Value: testJobName,
		},
		Id: &peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", testJobName, instanceID),
		},
		Resource: resCfg,
		Priority: uint32(1),
		TaskId: &mesos.TaskID{
			Value: &tid,
		},
		Preemptible: true,
		NumPorts:    uint32(2),
	}
}

func createTestGang(task *resmgr.Task) *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	gang.Tasks = append(gang.Tasks, task)
	return &gang
}

// createPortRanges create Mesos Ranges type from given port set.
func createPortRanges(portSet map[uint32]bool) *mesos.Value_Ranges {
	var sorted []int
	for p, ok := range portSet {
		if ok {
			sorted = append(sorted, int(p))
		}
	}
	sort.Ints(sorted)

	res := mesos.Value_Ranges{
		Range: []*mesos.Value_Range{},
	}
	for _, p := range sorted {
		tmp := uint64(p)
		res.Range = append(
			res.Range,
			&mesos.Value_Range{Begin: &tmp, End: &tmp},
		)
	}
	return &res
}

func createResources(defaultMultiplier float64) []*mesos.Resource {
	values := map[string]float64{
		"cpus": defaultMultiplier * defaultResourceConfig.CpuLimit,
		"mem":  defaultMultiplier * defaultResourceConfig.MemLimitMb,
		"disk": defaultMultiplier * defaultResourceConfig.DiskLimitMb,
		"gpus": defaultMultiplier * defaultResourceConfig.GpuLimit,
	}
	portSet := make(map[uint32]bool)
	for i := 0; i < int(defaultMultiplier); i++ {
		portSet[uint32(1000+i*2)] = true
		portSet[uint32(1001+i*2)] = true
	}
	portResource := util.NewMesosResourceBuilder().
		WithName("ports").
		WithType(mesos.Value_RANGES).
		WithRanges(createPortRanges(portSet)).
		Build()

	result := util.CreateMesosScalarResources(values, "*")
	result = append(result, portResource)
	return result
}

func createHostOffer(hostID int, resources []*mesos.Resource) *hostsvc.HostOffer {
	agentID := fmt.Sprintf("agent-%d", hostID)
	return &hostsvc.HostOffer{
		Hostname: fmt.Sprintf("hostname-%d", hostID),
		AgentId: &mesos.AgentID{
			Value: &agentID,
		},
		Resources: resources,
	}
}

// TODO: Add a test for Start()/Stop() pair.

// This test ensures that empty task returned from resmgr does not trigger hostmgr calls.
func TestEmptyTaskToPlace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRes := res_mocks.NewMockResourceManagerServiceYarpcClient(ctrl)
	mockHostMgr := host_mocks.NewMockInternalHostServiceYarpcClient(ctrl)
	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)

	pe := placementEngine{
		cfg: &Config{
			TaskDequeueLimit:     10,
			OfferDequeueLimit:    10,
			MaxPlacementDuration: maxPlacementDuration,
			TaskDequeueTimeOut:   taskDequeueTimeout,
		},
		resMgrClient:  mockRes,
		hostMgrClient: mockHostMgr,
		rootCtx:       context.Background(),
		metrics:       metrics,
		pool:          async.NewPool(async.PoolOptions{}),
	}

	gomock.InOrder(
		mockRes.EXPECT().
			DequeueGangs(
				gomock.Any(),
				gomock.Eq(&resmgrsvc.DequeueGangsRequest{
					Limit:   uint32(10),
					Timeout: uint32(pe.cfg.TaskDequeueTimeOut),
				})).
			Return(&resmgrsvc.DequeueGangsResponse{}, nil),
	)

	pe.placeRound()
}

// This test ensures that metrics are properly tracked when no host offer is returned from hostmgr.
func TestNoHostOfferReturned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRes := res_mocks.NewMockResourceManagerServiceYarpcClient(ctrl)
	mockHostMgr := host_mocks.NewMockInternalHostServiceYarpcClient(ctrl)
	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)

	// TODO: read from test configs
	pe := placementEngine{
		cfg: &Config{
			TaskDequeueLimit:     10,
			OfferDequeueLimit:    10,
			MaxPlacementDuration: 100 * time.Millisecond,
		},
		resMgrClient:  mockRes,
		hostMgrClient: mockHostMgr,
		rootCtx:       context.Background(),
		metrics:       metrics,
		pool:          async.NewPool(async.PoolOptions{}),
	}

	assert.Equal(
		t,
		int64(0),
		testScope.Snapshot().Counters()["offer_starved+"].Value())

	t1 := createTestTask(0)
	g1 := createTestGang(t1)

	gomock.InOrder(
		// Call to resmgr for getting task.
		mockRes.EXPECT().
			DequeueGangs(
				gomock.Any(),
				gomock.Eq(&resmgrsvc.DequeueGangsRequest{
					Limit:   uint32(10),
					Timeout: uint32(pe.cfg.TaskDequeueTimeOut),
				})).
			Return(&resmgrsvc.DequeueGangsResponse{
				Gangs: []*resmgrsvc.Gang{g1},
				Error: nil,
			}, nil),
		// Mock AcquireHostOffers with empty response.
		mockHostMgr.EXPECT().
			AcquireHostOffers(
				gomock.Any(),
				gomock.Eq(&hostsvc.AcquireHostOffersRequest{
					Filter: &hostsvc.HostFilter{
						Quantity: &hostsvc.QuantityControl{
							// only one task so expect 1 instead of default offer limit 10.
							MaxHosts: uint32(1),
						},
						ResourceConstraint: &hostsvc.ResourceConstraint{
							Minimum:  t1.Resource,
							NumPorts: t1.NumPorts,
						},
					},
				})).
			Return(&hostsvc.AcquireHostOffersResponse{}, nil).
			MinTimes(1),
	)

	pe.placeRound()

	pe.pool.WaitUntilProcessed()

	assert.NotEqual(
		t,
		int64(0),
		testScope.Snapshot().Counters()["offer_starved+"].Value())
}

// This test ensures that multiple tasks returned from resmgr can be properly placed by hostmgr
// as long as we have enough resource, and unused HostOffers are properly returned.
func TestMultipleTasksPlaced(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRes := res_mocks.NewMockResourceManagerServiceYarpcClient(ctrl)
	mockHostMgr := host_mocks.NewMockInternalHostServiceYarpcClient(ctrl)
	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)

	pe := placementEngine{
		cfg: &Config{
			TaskDequeueLimit:     10,
			OfferDequeueLimit:    10,
			MaxPlacementDuration: maxPlacementDuration,
		},
		resMgrClient:  mockRes,
		hostMgrClient: mockHostMgr,
		rootCtx:       context.Background(),
		metrics:       metrics,
		pool:          async.NewPool(async.PoolOptions{}),
	}

	// generate 25 test tasks
	numTasks := 25
	var testTasks []*resmgr.Task
	taskIds := make(map[string]*peloton.TaskID)
	var testGangs []*resmgrsvc.Gang

	for i := 0; i < numTasks; i++ {
		tmp := createTestTask(i)
		testTasks = append(testTasks, tmp)
		taskIds[tmp.Id.Value] = tmp.Id
		tmpGang := createTestGang(tmp)
		testGangs = append(testGangs, tmpGang)
	}

	// generate 5 host offers, each can hold 10 tasks.
	numHostOffers := 5
	rs := createResources(10)
	var hostOffers []*hostsvc.HostOffer
	for i := 0; i < numHostOffers; i++ {
		hostOffers = append(hostOffers, createHostOffer(i, rs))
	}

	// Capture LaunchTasks calls
	hostsLaunchedOn := make(map[string]bool)
	launchedTasks := make(map[string]*peloton.TaskID)

	gomock.InOrder(
		mockRes.EXPECT().
			DequeueGangs(
				gomock.Any(),
				gomock.Eq(&resmgrsvc.DequeueGangsRequest{
					Limit:   uint32(10),
					Timeout: uint32(pe.cfg.TaskDequeueTimeOut),
				})).
			Return(&resmgrsvc.DequeueGangsResponse{
				Gangs: testGangs,
				Error: nil,
			}, nil),
		// Mock AcquireHostOffers call.
		mockHostMgr.EXPECT().
			AcquireHostOffers(
				gomock.Any(),
				gomock.Eq(&hostsvc.AcquireHostOffersRequest{
					Filter: &hostsvc.HostFilter{
						Quantity: &hostsvc.QuantityControl{
							MaxHosts: uint32(10), // OfferDequeueLimit
						},
						ResourceConstraint: &hostsvc.ResourceConstraint{
							Minimum:  testTasks[0].Resource,
							NumPorts: testTasks[0].NumPorts,
						},
					},
				})).
			Return(&hostsvc.AcquireHostOffersResponse{
				HostOffers: hostOffers,
			}, nil),
		// Mock PlaceTasks call.
		mockRes.EXPECT().
			SetPlacements(
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context, reqBody interface{}) {
				// No need to unmarksnal output: empty means success.
				// Capture call since we don't know ordering of tasks.
				lock.Lock()
				defer lock.Unlock()
				req := reqBody.(*resmgrsvc.SetPlacementsRequest)
				assert.Len(t, req.Placements, 3)
				for _, p := range req.Placements {
					hostsLaunchedOn[p.Hostname] = true
					for _, lt := range p.Tasks {
						launchedTasks[lt.Value] = lt
					}
					assert.Len(t, p.GetPorts(), len(p.GetTasks())*2)
				}
			}).
			Return(&resmgrsvc.SetPlacementsResponse{}, nil),
		// Mock ReleaseHostOffers call, which should return the last two host offers back because they are not used.
		mockHostMgr.EXPECT().
			ReleaseHostOffers(
				gomock.Any(),
				gomock.Eq(&hostsvc.ReleaseHostOffersRequest{
					HostOffers: hostOffers[3:],
				})).
			Return(&hostsvc.ReleaseHostOffersResponse{}, nil),
	)

	pe.placeRound()

	pe.pool.WaitUntilProcessed()

	expectedLaunchedHosts := map[string]bool{
		"hostname-0": true,
		"hostname-1": true,
		"hostname-2": true,
	}
	lock.Lock()
	defer lock.Unlock()
	assert.Equal(t, expectedLaunchedHosts, hostsLaunchedOn)
	assert.Equal(t, taskIds, launchedTasks)
}

// This test ensures that only subset of tasks returned from resmgr can be
// properly placed by hostmgr due to insufficient port resources.
func TestSubsetTasksPlacedDueToInsufficientPorts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRes := res_mocks.NewMockResourceManagerServiceYarpcClient(ctrl)
	mockHostMgr := host_mocks.NewMockInternalHostServiceYarpcClient(ctrl)
	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)

	pe := placementEngine{
		cfg: &Config{
			TaskDequeueLimit:     10,
			OfferDequeueLimit:    10,
			MaxPlacementDuration: 1 * time.Microsecond,
		},
		resMgrClient:  mockRes,
		hostMgrClient: mockHostMgr,
		rootCtx:       context.Background(),
		metrics:       metrics,
		pool:          async.NewPool(async.PoolOptions{}),
	}

	// generate 25 test tasks
	numTasks := 25
	var testTasks []*resmgr.Task
	taskIds := make(map[string]*peloton.TaskID)
	var testGangs []*resmgrsvc.Gang

	for i := 0; i < numTasks; i++ {
		tmp := createTestTask(i)
		tmp.NumPorts = uint32(10)
		testTasks = append(testTasks, tmp)
		taskIds[tmp.Id.Value] = tmp.Id
		tmpGang := createTestGang(tmp)
		testGangs = append(testGangs, tmpGang)
	}

	// generate 5 host offers, each can only hold 2 tasks as each task
	// requires 10 ports.
	numHostOffers := 5
	rs := createResources(10)
	var hostOffers []*hostsvc.HostOffer
	for i := 0; i < numHostOffers; i++ {
		hostOffers = append(hostOffers, createHostOffer(i, rs))
	}

	// Capture LaunchTasks calls
	hostsLaunchedOn := make(map[string]bool)
	launchedTasks := make(map[string]*peloton.TaskID)

	gomock.InOrder(
		mockRes.EXPECT().
			DequeueGangs(
				gomock.Any(),
				gomock.Eq(&resmgrsvc.DequeueGangsRequest{
					Limit:   uint32(10),
					Timeout: uint32(pe.cfg.TaskDequeueTimeOut),
				})).
			Return(&resmgrsvc.DequeueGangsResponse{
				Gangs: testGangs,
				Error: nil,
			}, nil),
		// Mock AcquireHostOffers call.
		mockHostMgr.EXPECT().
			AcquireHostOffers(
				gomock.Any(),
				gomock.Eq(&hostsvc.AcquireHostOffersRequest{
					Filter: &hostsvc.HostFilter{
						Quantity: &hostsvc.QuantityControl{
							MaxHosts: uint32(10), // OfferDequeueLimit
						},
						ResourceConstraint: &hostsvc.ResourceConstraint{
							Minimum:  testTasks[0].Resource,
							NumPorts: testTasks[0].NumPorts,
						},
					},
				})).
			Return(&hostsvc.AcquireHostOffersResponse{
				HostOffers: hostOffers,
			}, nil),
		// Mock PlaceTasks call.
		mockRes.EXPECT().
			SetPlacements(
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context, reqBody interface{}) {
				// No need to unmarksnal output: empty means success.
				// Capture call since we don't know ordering of tasks.
				lock.Lock()
				defer lock.Unlock()
				req := reqBody.(*resmgrsvc.SetPlacementsRequest)
				for _, p := range req.Placements {
					hostsLaunchedOn[p.Hostname] = true
					for _, lt := range p.Tasks {
						launchedTasks[lt.Value] = lt
					}
					// Verify allocated 20 ports for 2 tasks
					// in the placement.
					assert.Len(t, p.GetPorts(), 20)
				}
			}).
			Return(&resmgrsvc.SetPlacementsResponse{}, nil),
	)

	pe.placeRound()

	pe.pool.WaitUntilProcessed()

	lock.Lock()
	defer lock.Unlock()
	assert.Equal(t, len(hostsLaunchedOn), 5)
	assert.Equal(t, len(launchedTasks), 10)
}

func TestGroupTasks(t *testing.T) {
	t1 := createTestTask(0)
	t2 := createTestTask(1)
	result := groupTasks([]*resmgr.Task{t1, t2})
	assert.Equal(t, 1, len(result))
	tmp1 := getHostFilter(t1)
	key1 := tmp1.String()
	assert.Contains(t, result, key1)
	group1 := result[key1]
	assert.NotNil(t, group1)
	assert.Equal(t, 2, len(group1.tasks))
	assert.Equal(t, t1.GetResource(), group1.getResourceConfig())

	// t3 has a different resource config
	t3 := createTestTask(2)
	t3.Resource.CpuLimit += float64(1)

	// t4 has a nil resource config
	t4 := createTestTask(3)
	t4.Resource = nil

	// t5 and t6 have a non-nil scheduling constraint.
	t5 := createTestTask(4)
	t5.Constraint = &andConstraint

	t6 := createTestTask(6)
	t6.Constraint = &orConstraint

	result = groupTasks([]*resmgr.Task{t1, t2, t3, t4, t5, t6})
	assert.Equal(t, 5, len(result))

	tmp1 = getHostFilter(t1)
	key1 = tmp1.String()
	assert.Contains(t, result, key1)
	group1 = result[key1]
	assert.NotNil(t, group1)
	assert.Equal(t, 2, len(group1.tasks))
	assert.Equal(t, t1.GetResource(), group1.getResourceConfig())

	tmp3 := getHostFilter(t3)
	key3 := tmp3.String()
	assert.Contains(t, result, key3)
	group3 := result[key3]
	assert.NotNil(t, group3)
	assert.Equal(t, 1, len(group3.tasks))
	assert.Equal(t, t3.GetResource(), group3.getResourceConfig())

	tmp4 := getHostFilter(t4)
	key4 := tmp4.String()
	assert.Contains(t, result, key4)
	group4 := result[key4]
	assert.NotNil(t, group4)
	assert.Equal(t, 1, len(group4.tasks))
	assert.Nil(t, group4.getResourceConfig())

	tmp5 := getHostFilter(t5)
	key5 := tmp5.String()
	assert.Contains(t, result, key5)
	group5 := result[key5]
	assert.NotNil(t, group5)
	assert.Equal(t, 1, len(group5.tasks))
	assert.Equal(t, t5.GetResource(), group5.getResourceConfig())

	tmp6 := getHostFilter(t6)
	key6 := tmp6.String()
	assert.Contains(t, result, key6)
	group6 := result[key6]
	assert.NotNil(t, group6)
	assert.Equal(t, 1, len(group6.tasks))
	assert.Equal(t, t6.GetResource(), group6.getResourceConfig())
}

func TestGroupTasksWithPorts(t *testing.T) {
	t1 := createTestTask(1)
	t1.Constraint = &orConstraint

	t2 := createTestTask(2)
	t2.Constraint = &orConstraint
	t2.NumPorts = uint32(3)

	t3 := createTestTask(3)
	t3.Constraint = &orConstraint
	t3.NumPorts = uint32(3)

	t4 := createTestTask(4)
	t4.Constraint = &orConstraint
	t4.NumPorts = uint32(4)

	t5 := createTestTask(5)
	t5.NumPorts = uint32(5)

	t6 := createTestTask(6)
	t6.NumPorts = uint32(5)

	result := groupTasks([]*resmgr.Task{t1, t2, t3, t4, t5, t6})
	assert.Equal(t, 4, len(result))

	tmp1 := getHostFilter(t1)
	key1 := tmp1.String()
	assert.Contains(t, result, key1)
	group1 := result[key1]
	assert.NotNil(t, group1)
	assert.Equal(t, 1, len(group1.tasks))

	tmp3 := getHostFilter(t3)
	key3 := tmp3.String()
	assert.Contains(t, result, key3)
	group3 := result[key3]
	assert.NotNil(t, group3)
	assert.Equal(t, 2, len(group3.tasks))

	tmp4 := getHostFilter(t4)
	key4 := tmp4.String()
	assert.Contains(t, result, key4)
	group4 := result[key4]
	assert.NotNil(t, group4)
	assert.Equal(t, 1, len(group4.tasks))

	tmp5 := getHostFilter(t5)
	key5 := tmp5.String()
	assert.Contains(t, result, key5)
	group5 := result[key5]
	assert.NotNil(t, group5)
	assert.Equal(t, 2, len(group5.tasks))
}
