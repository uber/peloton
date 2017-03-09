package placement

import (
	"fmt"
	"testing"
	"time"

	"go.uber.org/yarpc"

	"golang.org/x/net/context"

	mesos "mesos/v1"

	"peloton/api/task"
	"peloton/api/task/config"
	"peloton/private/hostmgr/hostsvc"
	"peloton/private/resmgr/taskqueue"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/util"

	yarpc_mocks "code.uber.internal/infra/peloton/yarpc/encoding/mocks"
)

// TODO: load from test configs
const (
	taskIDFmt   = "testjob-%d-abcdefgh-abcd-1234-5678-1234567890"
	testJobName = "testjob"
	// a fake maxPlacementDuration to ensure that placementLoop doesn't complete immediately
	maxPlacementDuration = time.Duration(10) * time.Second
)

var (
	defaultResourceConfig = config.ResourceConfig{
		CpuLimit:    10,
		MemLimitMb:  10,
		DiskLimitMb: 10,
		FdLimit:     10,
	}
)

func createTestTask(instanceID int) *task.TaskInfo {
	var tid = fmt.Sprintf(taskIDFmt, instanceID)

	return &task.TaskInfo{
		InstanceId: uint32(instanceID),
		Config: &config.TaskConfig{
			Name:     testJobName,
			Resource: &defaultResourceConfig,
		},
		Runtime: &task.RuntimeInfo{
			TaskId: &mesos.TaskID{
				Value: &tid,
			},
		},
	}
}

func createTestTasks(instanceIds []int) []*task.TaskInfo {
	var tasks []*task.TaskInfo
	for _, instanceID := range instanceIds {
		tasks = append(tasks, createTestTask(instanceID))
	}
	return tasks
}

func createResources(defaultMultiplier float64) []*mesos.Resource {
	values := map[string]float64{
		"cpus": defaultMultiplier * defaultResourceConfig.CpuLimit,
		"mem":  defaultMultiplier * defaultResourceConfig.MemLimitMb,
		"disk": defaultMultiplier * defaultResourceConfig.DiskLimitMb,
		"gpus": defaultMultiplier * defaultResourceConfig.GpuLimit,
	}
	return util.CreateMesosScalarResources(values, "*")
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

// createLaunchTasksRequest generates hostsvc.LaunchTasksRequest from tasks and hostname
func createLaunchTasksRequest(
	tasks []*task.TaskInfo,
	hostname string,
	agentID *mesos.AgentID) *hostsvc.LaunchTasksRequest {
	return &hostsvc.LaunchTasksRequest{
		Hostname: hostname,
		Tasks:    createLaunchableTasks(tasks),
		AgentId:  agentID,
	}
}

// TODO: Add a test for Start()/Stop() pair.

// This test ensures that empty task returned from resmgr does not trigger hostmgr calls.
func TestEmptyTaskToPlace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRes := yarpc_mocks.NewMockClient(ctrl)
	mockHostMgr := yarpc_mocks.NewMockClient(ctrl)
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
	}

	gomock.InOrder(
		mockRes.EXPECT().
			Call(
				gomock.Any(),
				gomock.Eq(yarpc.NewReqMeta().Procedure("TaskQueue.Dequeue")),
				gomock.Eq(&taskqueue.DequeueRequest{
					Limit: uint32(10),
				}),
				gomock.Any()).
			Do(func(_ context.Context, _ yarpc.CallReqMeta, _ interface{}, resBodyOut interface{}) {
				var empty taskqueue.DequeueResponse
				o := resBodyOut.(*taskqueue.DequeueResponse)
				*o = empty
			}).
			Return(nil, nil),
	)

	pe.placeRound()
}

// This test ensures that metrics are properly tracked when no host offer is returned from hostmgr.
func TestNoHostOfferReturned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRes := yarpc_mocks.NewMockClient(ctrl)
	mockHostMgr := yarpc_mocks.NewMockClient(ctrl)
	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)

	// TODO: read from test configs
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
	}

	assert.Equal(
		t,
		int64(0),
		testScope.Snapshot().Counters()["offer_starved"].Value())

	t1 := createTestTask(0)

	gomock.InOrder(
		// Call to resmgr for getting task.
		mockRes.EXPECT().
			Call(
				gomock.Any(),
				gomock.Eq(yarpc.NewReqMeta().Procedure("TaskQueue.Dequeue")),
				gomock.Eq(&taskqueue.DequeueRequest{
					Limit: uint32(10),
				}),
				gomock.Any()).
			Do(func(_ context.Context, _ yarpc.CallReqMeta, _ interface{}, resBodyOut interface{}) {
				o := resBodyOut.(*taskqueue.DequeueResponse)
				*o = taskqueue.DequeueResponse{
					Tasks: []*task.TaskInfo{t1},
				}
			}).
			Return(nil, nil),
		// Mock AcquireHostOffers with empty response.
		mockHostMgr.EXPECT().
			Call(
				gomock.Any(),
				gomock.Eq(yarpc.NewReqMeta().Procedure("InternalHostService.AcquireHostOffers")),
				gomock.Eq(&hostsvc.AcquireHostOffersRequest{
					Constraints: []*hostsvc.Constraint{
						{
							Limit: uint32(1), // only one task
							ResourceConstraint: &hostsvc.ResourceConstraint{
								Minimum: t1.Config.Resource,
							},
						},
					},
				}),
				gomock.Any()).
			Return(nil, nil).
			MinTimes(1),
	)

	pe.placeRound()

	assert.NotEqual(
		t,
		int64(0),
		testScope.Snapshot().Counters()["offer_starved"].Value())
}

// This test ensures that multiple tasks returned from resmgr can be properly placed by hostmgr
// as long as we have enough resource, and unused HostOffers are properly returned.
func TestMultipleTasksPlaced(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRes := yarpc_mocks.NewMockClient(ctrl)
	mockHostMgr := yarpc_mocks.NewMockClient(ctrl)
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
	}

	// generate 25 test tasks
	numTasks := 25
	var testTasks []*task.TaskInfo
	taskConfigs := make(map[string]*config.TaskConfig)
	for i := 0; i < numTasks; i++ {
		tmp := createTestTask(i)
		testTasks = append(testTasks, tmp)
		taskConfigs[tmp.GetRuntime().GetTaskId().GetValue()] = tmp.Config
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
	launchedTasks := make(map[string]*config.TaskConfig)

	gomock.InOrder(
		mockRes.EXPECT().
			Call(
				gomock.Any(),
				gomock.Eq(yarpc.NewReqMeta().Procedure("TaskQueue.Dequeue")),
				gomock.Eq(&taskqueue.DequeueRequest{
					Limit: uint32(10),
				}),
				gomock.Any()).
			Do(func(_ context.Context, _ yarpc.CallReqMeta, _ interface{}, resBodyOut interface{}) {
				o := resBodyOut.(*taskqueue.DequeueResponse)
				*o = taskqueue.DequeueResponse{
					Tasks: testTasks,
				}
			}).
			Return(nil, nil),
		// Mock AcquireHostOffers call.
		mockHostMgr.EXPECT().
			Call(
				gomock.Any(),
				gomock.Eq(yarpc.NewReqMeta().Procedure("InternalHostService.AcquireHostOffers")),
				gomock.Eq(&hostsvc.AcquireHostOffersRequest{
					Constraints: []*hostsvc.Constraint{
						{
							Limit: uint32(10), // OfferDequeueLimit
							ResourceConstraint: &hostsvc.ResourceConstraint{
								Minimum: testTasks[0].Config.Resource,
							},
						},
					},
				}),
				gomock.Any()).
			Do(func(_ context.Context, _ yarpc.CallReqMeta, _ interface{}, resBodyOut interface{}) {
				o := resBodyOut.(*hostsvc.AcquireHostOffersResponse)
				*o = hostsvc.AcquireHostOffersResponse{
					HostOffers: hostOffers,
				}
			}).
			Return(nil, nil),
		// Mock LaunchTasks call.
		mockHostMgr.EXPECT().
			Call(
				gomock.Any(),
				gomock.Eq(yarpc.NewReqMeta().Procedure("InternalHostService.LaunchTasks")),
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context, _ yarpc.CallReqMeta, reqBody interface{}, _ interface{}) {
				// No need to unmarksnal output: empty means success.
				// Capture call since we don't know ordering of tasks.
				req := reqBody.(*hostsvc.LaunchTasksRequest)
				hostsLaunchedOn[req.Hostname] = true
				for _, lt := range req.Tasks {
					launchedTasks[lt.TaskId.GetValue()] = lt.Config
				}
			}).
			Return(nil, nil).
			Times(3),
		// Mock ReleaseHostOffers call, which should return the last two host offers back because they are not used.
		mockHostMgr.EXPECT().
			Call(
				gomock.Any(),
				gomock.Eq(yarpc.NewReqMeta().Procedure("InternalHostService.ReleaseHostOffers")),
				gomock.Eq(&hostsvc.ReleaseHostOffersRequest{
					HostOffers: hostOffers[3:],
				}),
				gomock.Any()).
			Return(nil, nil),
	)

	pe.placeRound()

	expectedLaunchedHosts := map[string]bool{
		"hostname-0": true,
		"hostname-1": true,
		"hostname-2": true,
	}
	assert.Equal(t, expectedLaunchedHosts, hostsLaunchedOn)
	assert.Equal(t, taskConfigs, launchedTasks)
}

func TestCreateLaunchableTasks(t *testing.T) {
	var hostname = "testhost"
	var tmp = "agentid"
	var agentID = &mesos.AgentID{
		Value: &tmp,
	}

	tasks := createTestTasks([]int{0, 1})
	ltr := createLaunchTasksRequest(tasks, hostname, agentID)
	assert.Equal(t, hostname, ltr.Hostname)
	assert.Equal(t, agentID, ltr.AgentId)
	assert.Equal(t, 2, len(ltr.Tasks))
}

func TestGroupTasksByResource(t *testing.T) {
	t1 := createTestTask(0)
	t2 := createTestTask(1)
	result := groupTasksByResource([]*task.TaskInfo{t1, t2})
	assert.Equal(t, 1, len(result))
	key1 := t1.GetConfig().GetResource().String()
	assert.Contains(t, result, key1)
	group1 := result[key1]
	assert.NotNil(t, group1)
	assert.Equal(t, 2, len(group1.tasks))
	assert.Equal(t, t1.GetConfig().GetResource(), group1.resourceConfig)

	// t3 has a different resource config
	t3 := createTestTask(2)
	t3.Config.Resource = &config.ResourceConfig{
		CpuLimit:    10,
		MemLimitMb:  20,
		DiskLimitMb: 20,
		FdLimit:     20,
	}

	// t4 has a nil resource config
	t4 := createTestTask(3)
	t4.Config.Resource = nil

	result = groupTasksByResource([]*task.TaskInfo{t1, t2, t3, t4})
	assert.Equal(t, 3, len(result))

	key1 = t1.GetConfig().GetResource().String()
	assert.Contains(t, result, key1)
	group1 = result[key1]
	assert.NotNil(t, group1)
	assert.Equal(t, 2, len(group1.tasks))
	assert.Equal(t, t1.GetConfig().GetResource(), group1.resourceConfig)

	key3 := t3.GetConfig().GetResource().String()
	assert.Contains(t, result, key3)
	group3 := result[key3]
	assert.NotNil(t, group3)
	assert.Equal(t, 1, len(group3.tasks))
	assert.Equal(t, t3.GetConfig().GetResource(), group3.resourceConfig)

	key4 := t4.GetConfig().GetResource().String()
	assert.Contains(t, result, key4)
	group4 := result[key4]
	assert.NotNil(t, group4)
	assert.Equal(t, 1, len(group4.tasks))
	assert.Nil(t, group4.resourceConfig)
}
