package launcher

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"
	yarpc_mocks "code.uber.internal/infra/peloton/vendor_mocks/go.uber.org/yarpc/encoding/json/mocks"
)

const (
	taskIDFmt   = "testjob-%d-abcdefgh-abcd-1234-5678-1234567890ab"
	testJobName = "testjob"
	testPort    = uint32(100)
)

var (
	defaultResourceConfig = task.ResourceConfig{
		CpuLimit:    10,
		MemLimitMb:  10,
		DiskLimitMb: 10,
		FdLimit:     10,
	}
	lock = sync.RWMutex{}
)

func createTestTask(instanceID int) *task.TaskInfo {
	var tid = fmt.Sprintf(taskIDFmt, instanceID)

	return &task.TaskInfo{
		JobId: &peloton.JobID{
			Value: testJobName,
		},
		InstanceId: uint32(instanceID),
		Config: &task.TaskConfig{
			Name:     testJobName,
			Resource: &defaultResourceConfig,
			Ports: []*task.PortConfig{
				{
					Name:    "port",
					EnvName: "PORT",
				},
			},
		},
		Runtime: &task.RuntimeInfo{
			TaskId: &mesos.TaskID{
				Value: &tid,
			},
		},
	}
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

// This test ensures that multiple placements returned from resmgr can be properly placed by hostmgr
func TestMultipleTasksPlaced(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRes := yarpc_mocks.NewMockClient(ctrl)
	mockHostMgr := yarpc_mocks.NewMockClient(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)
	taskLauncher := launcher{
		config: &Config{
			PlacementDequeueLimit: 100,
		},
		resMgrClient:  mockRes,
		hostMgrClient: mockHostMgr,
		taskStore:     mockTaskStore,
		rootCtx:       context.Background(),
		metrics:       metrics,
	}

	// generate 25 test tasks
	numTasks := 1
	testTasks := make([]*task.TaskInfo, numTasks)
	taskIDs := make([]*peloton.TaskID, numTasks)
	placements := make([]*resmgr.Placement, numTasks)
	taskConfigs := make(map[string]*task.TaskConfig)
	taskIds := make(map[string]*peloton.TaskID)
	for i := 0; i < numTasks; i++ {
		tmp := createTestTask(i)
		taskID := &peloton.TaskID{
			Value: tmp.JobId.Value + "-" + fmt.Sprint(tmp.InstanceId),
		}
		taskIDs[i] = taskID
		testTasks[i] = tmp
		taskConfigs[tmp.GetRuntime().GetTaskId().GetValue()] = tmp.Config
		taskIds[taskID.Value] = taskID
	}

	// generate 1 host offer, each can hold 1 tasks.
	numHostOffers := 1
	rs := createResources(1)
	var hostOffers []*hostsvc.HostOffer
	for i := 0; i < numHostOffers; i++ {
		hostOffers = append(hostOffers, createHostOffer(i, rs))
	}

	// Generate Placements per host offer
	for i := 0; i < numHostOffers; i++ {
		p := createPlacements(testTasks[i], hostOffers[i])
		placements[i] = p
	}

	// Capture LaunchTasks calls
	hostsLaunchedOn := make(map[string]bool)
	launchedTasks := make(map[string]*task.TaskConfig)

	updatedTaskInfo := proto.Clone(testTasks[0]).(*task.TaskInfo)
	updatedTaskInfo.Runtime.Host = hostOffers[0].Hostname
	updatedTaskInfo.Runtime.Ports = make(map[string]uint32)
	updatedTaskInfo.Runtime.Ports["port"] = testPort
	updatedTaskInfo.Runtime.State = task.TaskState_LAUNCHING

	gomock.InOrder(
		mockRes.EXPECT().
			Call(
				gomock.Any(),
				gomock.Eq(yarpc.NewReqMeta().Procedure("ResourceManagerService.GetPlacements")),
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context, _ yarpc.CallReqMeta, _ interface{}, resBodyOut interface{}) {
				o := resBodyOut.(*resmgrsvc.GetPlacementsResponse)
				*o = resmgrsvc.GetPlacementsResponse{
					Placements: placements,
				}
			}).
			Return(nil, nil),

		// Mock Task Store GetTaskByID
		mockTaskStore.EXPECT().GetTaskByID(taskIDs[0].Value).Return(testTasks[0], nil),

		// Mock Task Store UpdateTask
		mockTaskStore.EXPECT().UpdateTask(updatedTaskInfo).Return(nil),

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
				lock.Lock()
				defer lock.Unlock()
				req := reqBody.(*hostsvc.LaunchTasksRequest)
				hostsLaunchedOn[req.Hostname] = true
				for _, lt := range req.Tasks {
					launchedTasks[lt.TaskId.GetValue()] = lt.Config
				}
			}).
			Return(nil, nil).
			Times(1),
	)

	placements, err := taskLauncher.getPlacements()

	if err != nil {
		assert.Error(t, err)
	}
	taskLauncher.processPlacements(placements)

	time.Sleep(1 * time.Second)
	expectedLaunchedHosts := map[string]bool{
		"hostname-0": true,
	}
	lock.Lock()
	defer lock.Unlock()
	assert.Equal(t, expectedLaunchedHosts, hostsLaunchedOn)
	assert.Equal(t, taskConfigs, launchedTasks)
}

// createPlacements creates the placement
func createPlacements(t *task.TaskInfo,
	hostOffer *hostsvc.HostOffer) *resmgr.Placement {
	TasksIds := make([]*peloton.TaskID, 1)

	taskID := &peloton.TaskID{
		Value: t.JobId.Value + "-" + fmt.Sprint(t.InstanceId),
	}
	TasksIds[0] = taskID
	placement := &resmgr.Placement{
		AgentId:  hostOffer.AgentId,
		Hostname: hostOffer.Hostname,
		Tasks:    TasksIds,
		Ports:    []uint32{testPort},
	}

	return placement
}
