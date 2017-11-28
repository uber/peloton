package placement

import (
	"fmt"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"
)

const (
	taskIDFmt   = "testjob-%d-%s"
	testJobName = "testjob"
	testPort    = uint32(100)
)

var (
	_defaultResourceConfig = task.ResourceConfig{
		CpuLimit:    10,
		MemLimitMb:  10,
		DiskLimitMb: 10,
		FdLimit:     10,
	}
	_jobID = uuid.NewUUID().String()
	_sla   = &job.SlaConfig{
		Preemptible: false,
	}
	_jobConfig = &job.JobConfig{
		Name:          _jobID,
		Sla:           _sla,
		InstanceCount: 1,
	}
	lock = sync.RWMutex{}
)

func createTestTask(instanceID int) *task.TaskInfo {
	var tid = fmt.Sprintf(taskIDFmt, instanceID, uuid.NewUUID().String())

	return &task.TaskInfo{
		JobId: &peloton.JobID{
			Value: testJobName,
		},
		InstanceId: uint32(instanceID),
		Config: &task.TaskConfig{
			Name:     testJobName,
			Resource: &_defaultResourceConfig,
			Ports: []*task.PortConfig{
				{
					Name:    "port",
					EnvName: "PORT",
				},
			},
		},
		Runtime: &task.RuntimeInfo{
			MesosTaskId: &mesos.TaskID{
				Value: &tid,
			},
		},
	}
}

func createResources(defaultMultiplier float64) []*mesos.Resource {
	values := map[string]float64{
		"cpus": defaultMultiplier * _defaultResourceConfig.CpuLimit,
		"mem":  defaultMultiplier * _defaultResourceConfig.MemLimitMb,
		"disk": defaultMultiplier * _defaultResourceConfig.DiskLimitMb,
		"gpus": defaultMultiplier * _defaultResourceConfig.GpuLimit,
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
func TestMultipleTasksPlacements(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRes := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)
	pp := processor{
		config: &Config{
			PlacementDequeueLimit: 100,
		},
		resMgrClient: mockRes,
		taskStore:    mockTaskStore,
		metrics:      metrics,
	}

	// generate 25 test tasks
	numTasks := 25
	testTasks := make([]*task.TaskInfo, numTasks)
	placements := make([]*resmgr.Placement, numTasks)
	for i := 0; i < numTasks; i++ {
		tmp := createTestTask(i)
		testTasks[i] = tmp
	}

	// generate 25 host offer, each can hold 1 tasks.
	numHostOffers := numTasks
	rs := createResources(float64(numHostOffers))
	var hostOffers []*hostsvc.HostOffer
	for i := 0; i < numHostOffers; i++ {
		hostOffers = append(hostOffers, createHostOffer(i, rs))
	}

	// Generate Placements per host offer
	for i := 0; i < numHostOffers; i++ {
		p := createPlacements(testTasks[i], hostOffers[i])
		placements[i] = p
	}

	gomock.InOrder(
		mockRes.EXPECT().
			GetPlacements(
				gomock.Any(),
				gomock.Any()).
			Return(&resmgrsvc.GetPlacementsResponse{Placements: placements}, nil),

		mockTaskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(testTasks[0].Runtime, nil).
			AnyTimes(),
	)

	gPlacements, err := pp.getPlacements()

	if err != nil {
		assert.Error(t, err)
	}
	assert.Equal(t, placements, gPlacements)
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
