package resmgr

import (
	"container/list"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/common/queue"
	rp "code.uber.internal/infra/peloton/resmgr/respool"
	rm_task "code.uber.internal/infra/peloton/resmgr/task"
	"code.uber.internal/infra/peloton/storage/cassandra"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type recoveryTestSuite struct {
	suite.Suite
	resourceTree  rp.Tree
	store         *cassandra.Store
	ctrl          *gomock.Controller
	rmTaskTracker rm_task.Tracker
	handler       *ServiceHandler
	taskScheduler rm_task.Scheduler
	recovery      recoveryHandler
}

func (suite *recoveryTestSuite) SetupSuite() {
	suite.ctrl = gomock.NewController(suite.T())
	mockResPoolStore := store_mocks.NewMockResourcePoolStore(suite.ctrl)
	mockResPoolStore.EXPECT().GetAllResourcePools(context.Background()).
		Return(suite.getResPools(), nil).AnyTimes()
	conf := cassandra.MigrateForTest()
	store, err := cassandra.NewStore(conf, tally.NoopScope)
	suite.NoError(err)
	suite.store = store

	rp.InitTree(tally.NoopScope, mockResPoolStore, suite.store, suite.store)
	suite.resourceTree = rp.GetTree()
	// Initializing the resmgr state machine
	rm_task.InitTaskTracker()
	suite.rmTaskTracker = rm_task.GetTracker()
	rm_task.InitScheduler(100*time.Second, suite.rmTaskTracker)
	suite.taskScheduler = rm_task.GetScheduler()

	suite.handler = &ServiceHandler{
		metrics:     NewMetrics(tally.NoopScope),
		resPoolTree: suite.resourceTree,
		placements: queue.NewQueue(
			"placement-queue",
			reflect.TypeOf(&resmgr.Placement{}),
			maxPlacementQueueSize,
		),
		rmTracker: suite.rmTaskTracker,
	}
	suite.handler.eventStreamHandler = eventstream.NewEventStreamHandler(
		1000,
		[]string{
			common.PelotonJobManager,
			common.PelotonResourceManager,
		},
		nil,
		tally.Scope(tally.NoopScope))
}

func (suite *recoveryTestSuite) SetupTest() {
	err := suite.resourceTree.Start()
	suite.NoError(err)

	err = suite.taskScheduler.Start()
	suite.NoError(err)

}

// Returns resource pools
func (suite *recoveryTestSuite) getResPools() map[string]*respool.ResourcePoolConfig {
	rootID := respool.ResourcePoolID{Value: rp.RootResPoolID}
	policy := respool.SchedulingPolicy_PriorityFIFO

	return map[string]*respool.ResourcePoolConfig{
		// NB: root resource pool node is not stored in the database
		"respool1": {
			Name:      "respool1",
			Parent:    &rootID,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool2": {
			Name:      "respool2",
			Parent:    &rootID,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool3": {
			Name:      "respool3",
			Parent:    &rootID,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool11": {
			Name:      "respool11",
			Parent:    &respool.ResourcePoolID{Value: "respool1"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool12": {
			Name:      "respool12",
			Parent:    &respool.ResourcePoolID{Value: "respool1"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool21": {
			Name:      "respool21",
			Parent:    &respool.ResourcePoolID{Value: "respool2"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool22": {
			Name:      "respool22",
			Parent:    &respool.ResourcePoolID{Value: "respool2"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool23": {
			Name:   "respool23",
			Parent: &respool.ResourcePoolID{Value: "respool22"},
			Resources: []*respool.ResourceConfig{
				{
					Kind:        "cpu",
					Reservation: 50,
					Limit:       100,
					Share:       1,
				},
			},
			Policy: policy,
		},
		"respool99": {
			Name:   "respool99",
			Parent: &respool.ResourcePoolID{Value: "respool21"},
			Resources: []*respool.ResourceConfig{
				{
					Kind:        "cpu",
					Reservation: 50,
					Limit:       100,
					Share:       1,
				},
			},
			Policy: policy,
		},
	}
}

// Returns resource configs
func (suite *recoveryTestSuite) getResourceConfig() []*respool.ResourceConfig {

	resConfigs := []*respool.ResourceConfig{
		{
			Share:       1,
			Kind:        "cpu",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "memory",
			Reservation: 1000,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "disk",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "gpu",
			Reservation: 2,
			Limit:       4,
		},
	}
	return resConfigs
}

func (suite *recoveryTestSuite) TearDownSuite() {
	suite.resourceTree.Stop()
	suite.ctrl.Finish()
}

func (s *recoveryTestSuite) getEntitlement() map[string]float64 {
	mapEntitlement := make(map[string]float64)
	mapEntitlement[common.CPU] = float64(100)
	mapEntitlement[common.MEMORY] = float64(1000)
	mapEntitlement[common.DISK] = float64(100)
	mapEntitlement[common.GPU] = float64(2)
	return mapEntitlement
}

func (suite *recoveryTestSuite) getQueueContent(
	respoolID respool.ResourcePoolID) map[string]map[string]bool {

	var result = make(map[string]map[string]bool)
	for {
		node, err := suite.resourceTree.Get(&respoolID)
		suite.NoError(err)
		node.SetEntitlement(suite.getEntitlement())
		tlist, err := node.DequeueGangList(1)
		if err != nil {
			fmt.Printf("Failed to dequeue item: %v", err)
			break
		}
		if tlist.Len() != 1 {
			assert.Fail(suite.T(), "Dequeue should return single task scheduling unit")
		}
		rmTask := tlist.Front().Value.(*list.List)
		if rmTask != nil {
			jobID := rmTask.Front().Value.(*resmgr.Task).JobId.Value
			taskID := rmTask.Front().Value.(*resmgr.Task).Id.Value
			_, ok := result[jobID]
			if !ok {
				result[jobID] = make(map[string]bool)
			}
			result[jobID][taskID] = true
		} else {
			break
		}
	}
	return result
}

func (suite *recoveryTestSuite) createJob(
	jobID *peloton.JobID,
	numTasks uint32,
	instanceCount uint32,
	minInstances uint32,
	taskState task.TaskState,
	jobState job.JobState) {

	sla := job.SlaConfig{
		Preemptible:             false,
		Priority:                1,
		MinimumRunningInstances: minInstances,
	}
	var resPoolID respool.ResourcePoolID
	resPoolID.Value = "respool21"

	var jobConfig = job.JobConfig{
		Name:          jobID.Value,
		Sla:           &sla,
		InstanceCount: instanceCount,
		RespoolID:     &resPoolID,
	}

	var err = suite.store.CreateJob(context.Background(), jobID, &jobConfig, "uber")
	suite.NoError(err)
	for i := uint32(0); i < numTasks; i++ {
		var taskID = fmt.Sprintf("%s-%d", jobID.Value, i)
		taskConf := task.TaskConfig{
			Name: fmt.Sprintf("%s-%d", jobID.Value, i),
			Resource: &task.ResourceConfig{
				CpuLimit:   1,
				MemLimitMb: 20,
			},
		}
		var taskInfo = task.TaskInfo{
			Runtime: &task.RuntimeInfo{
				TaskId: &mesos.TaskID{Value: &taskID},
				State:  taskState,
			},
			Config:     &taskConf,
			InstanceId: i,
			JobId:      jobID,
		}
		err = suite.store.CreateTask(context.Background(), jobID, i, &taskInfo, "test")
		suite.NoError(err)
	}

	runtime, err := suite.store.GetJobRuntime(context.Background(), jobID)
	suite.NoError(err)
	runtime.State = jobState
	err = suite.store.UpdateJobRuntime(context.Background(), jobID, runtime)
	suite.NoError(err)
}

func (suite *recoveryTestSuite) TestRefillTaskQueue() {
	// Create jobs. each with different number of tasks
	jobs := [4]peloton.JobID{}
	for i := 0; i < 4; i++ {
		jobs[i] = peloton.JobID{Value: fmt.Sprintf("TestJob_%d", i)}
	}

	// create 4 jobs ( 2 JobState_SUCCEEDED, 1 JobState_INITIALIZED, 1 JobState_PENDING)
	suite.createJob(&jobs[0], 10, 10, 10,
		task.TaskState_SUCCEEDED, job.JobState_SUCCEEDED)
	suite.createJob(&jobs[1], 10, 10, 1,
		task.TaskState_SUCCEEDED, job.JobState_SUCCEEDED)
	suite.createJob(&jobs[2], 10, 10, 1,
		task.TaskState_INITIALIZED, job.JobState_INITIALIZED)
	suite.createJob(&jobs[3], 10, 10, 1,
		task.TaskState_PENDING, job.JobState_PENDING)

	// Perform recovery
	InitRecovery(suite.store, suite.store, suite.handler)
	GetRecoveryHandler().Start()

	// 1. All jobs should have 10 tasks in DB
	tasks, err := suite.store.GetTasksForJob(context.Background(), &jobs[0])
	suite.NoError(err)
	suite.Equal(len(tasks), 10)
	tasks, err = suite.store.GetTasksForJob(context.Background(), &jobs[1])
	suite.NoError(err)
	suite.Equal(len(tasks), 10)
	tasks, err = suite.store.GetTasksForJob(context.Background(), &jobs[2])
	suite.NoError(err)
	suite.Equal(len(tasks), 10)
	tasks, err = suite.store.GetTasksForJob(context.Background(), &jobs[3])
	suite.NoError(err)
	suite.Equal(len(tasks), 10)

	// 2. check the queue content
	var resPoolID respool.ResourcePoolID
	resPoolID.Value = "respool21"
	contentSummary := suite.getQueueContent(resPoolID)

	// Job2 and Job3 should be recovered and should have 10 tasks in the pending queue
	suite.Equal(len(contentSummary), 2)

	suite.Equal(len(contentSummary["TestJob_0"]), 0)
	suite.Equal(len(contentSummary["TestJob_1"]), 0)
	suite.Equal(len(contentSummary["TestJob_2"]), 10)
	suite.Equal(len(contentSummary["TestJob_3"]), 10)
}

func TestResmgrRecovery(t *testing.T) {
	suite.Run(t, new(recoveryTestSuite))
}
