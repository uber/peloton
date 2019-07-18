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

package resmgr

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	host_mocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/eventstream"
	"github.com/uber/peloton/pkg/common/queue"
	rc "github.com/uber/peloton/pkg/resmgr/common"
	rp "github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/scalar"
	rm_task "github.com/uber/peloton/pkg/resmgr/task"
	task_mocks "github.com/uber/peloton/pkg/resmgr/task/mocks"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type recoveryTestSuite struct {
	suite.Suite
	resourceTree      rp.Tree
	rmTaskTracker     rm_task.Tracker
	handler           *ServiceHandler
	taskScheduler     rm_task.Scheduler
	recovery          *RecoveryHandler
	mockCtrl          *gomock.Controller
	mockJobStore      *store_mocks.MockJobStore
	mockTaskStore     *store_mocks.MockTaskStore
	activeJobsOps     *objectmocks.MockActiveJobsOps
	jobConfigOps      *objectmocks.MockJobConfigOps
	jobRuntimeOps     *objectmocks.MockJobRuntimeOps
	mockResPoolOps    *objectmocks.MockResPoolOps
	mockHostmgrClient *host_mocks.MockInternalHostServiceYARPCClient
}

func (suite *recoveryTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockResPoolOps = objectmocks.NewMockResPoolOps(suite.mockCtrl)
	suite.mockResPoolOps.EXPECT().GetAll(context.Background()).
		Return(suite.getResPools(), nil).AnyTimes()
	suite.mockJobStore = store_mocks.NewMockJobStore(suite.mockCtrl)
	suite.mockTaskStore = store_mocks.NewMockTaskStore(suite.mockCtrl)
	suite.activeJobsOps = objectmocks.NewMockActiveJobsOps(suite.mockCtrl)
	suite.jobConfigOps = objectmocks.NewMockJobConfigOps(suite.mockCtrl)
	suite.jobRuntimeOps = objectmocks.NewMockJobRuntimeOps(suite.mockCtrl)
	suite.mockHostmgrClient = host_mocks.NewMockInternalHostServiceYARPCClient(suite.mockCtrl)

	suite.resourceTree = rp.NewTree(tally.NoopScope, suite.mockResPoolOps, suite.mockJobStore,
		suite.mockTaskStore,
		rc.PreemptionConfig{
			Enabled: false,
		})
	// Initializing the resmgr state machine
	rm_task.InitTaskTracker(tally.NoopScope, &rm_task.Config{
		EnablePlacementBackoff: true,
	})

	suite.rmTaskTracker = rm_task.GetTracker()
	rm_task.InitScheduler(
		tally.NoopScope,
		suite.resourceTree,
		100*time.Second,
		suite.rmTaskTracker,
	)
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
		config: Config{
			RmTaskConfig: &rm_task.Config{
				LaunchingTimeout: 1 * time.Minute,
				PlacingTimeout:   1 * time.Minute,
				PolicyName:       rm_task.ExponentialBackOffPolicy,
			},
		},
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
	suite.recovery = NewRecovery(
		tally.NoopScope,
		suite.mockTaskStore,
		suite.activeJobsOps,
		suite.jobConfigOps,
		suite.jobRuntimeOps,
		suite.handler,
		suite.resourceTree,
		Config{
			RmTaskConfig: &rm_task.Config{
				LaunchingTimeout: 1 * time.Minute,
				PlacingTimeout:   1 * time.Minute,
				PolicyName:       rm_task.ExponentialBackOffPolicy,
			},
		}, suite.mockHostmgrClient)

	suite.NoError(suite.resourceTree.Start())
	suite.NoError(suite.taskScheduler.Start())
}

func (suite *recoveryTestSuite) TearDownTest() {
	suite.NoError(suite.resourceTree.Stop())
	suite.NoError(suite.taskScheduler.Stop())
}

// Returns resource pools
func (suite *recoveryTestSuite) getResPools() map[string]*respool.ResourcePoolConfig {
	rootID := peloton.ResourcePoolID{Value: common.RootResPoolID}
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
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool12": {
			Name:      "respool12",
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool21": {
			Name:      "respool21",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool22": {
			Name:      "respool22",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool23": {
			Name:   "respool23",
			Parent: &peloton.ResourcePoolID{Value: "respool22"},
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
	suite.rmTaskTracker.Clear()
	suite.mockCtrl.Finish()
}

func (suite *recoveryTestSuite) getEntitlement() *scalar.Resources {
	return &scalar.Resources{
		CPU:    100,
		MEMORY: 1000,
		DISK:   100,
		GPU:    2,
	}
}

// returns a map of JobID -> array of gangs
func (suite *recoveryTestSuite) getQueueContent(
	respoolID peloton.ResourcePoolID) map[string][]*resmgrsvc.Gang {

	var result = make(map[string][]*resmgrsvc.Gang)
	for {
		node, err := suite.resourceTree.Get(&respoolID)
		suite.NoError(err)
		node.SetNonSlackEntitlement(suite.getEntitlement())
		dequeuedGangs, err := node.DequeueGangs(1)
		suite.NoError(err)

		if len(dequeuedGangs) == 0 {
			break
		}
		gang := dequeuedGangs[0]
		if gang != nil {
			jobID := gang.Tasks[0].JobId.Value
			_, ok := result[jobID]
			if !ok {
				var gang []*resmgrsvc.Gang
				result[jobID] = gang
			}
			result[jobID] = append(result[jobID], gang)
		}
	}
	return result
}

func (suite *recoveryTestSuite) createJob(jobID *peloton.JobID, instanceCount uint32, minInstances uint32) *job.JobConfig {
	return &job.JobConfig{
		Name: jobID.Value,
		SLA: &job.SlaConfig{
			Preemptible:             true,
			Priority:                1,
			MinimumRunningInstances: minInstances,
		},
		InstanceCount: instanceCount,
		RespoolID: &peloton.ResourcePoolID{
			Value: "respool21",
		},
	}
}

func (suite *recoveryTestSuite) createTasks(jobID *peloton.JobID, numTasks uint32, taskState task.TaskState) map[uint32]*task.TaskInfo {
	tasks := map[uint32]*task.TaskInfo{}
	var i uint32
	for i = uint32(0); i < numTasks; i++ {
		var taskID = fmt.Sprintf("%s-%d", jobID.Value, i)
		taskConf := task.TaskConfig{
			Name: fmt.Sprintf("%s-%d", jobID.Value, i),
			Resource: &task.ResourceConfig{
				CpuLimit:   1,
				MemLimitMb: 20,
			},
		}
		tasks[i] = &task.TaskInfo{
			Runtime: &task.RuntimeInfo{
				MesosTaskId:   &mesos.TaskID{Value: &taskID},
				State:         taskState,
				ConfigVersion: uint64(0),
			},
			Config:     &taskConf,
			InstanceId: i,
			JobId:      jobID,
		}
	}

	// Add 1 tasks in SUCCESS state
	var taskID = fmt.Sprintf("%s-%d", jobID.Value, i+1)
	taskConf := task.TaskConfig{
		Name: fmt.Sprintf("%s-%d", jobID.Value, i+1),
		Resource: &task.ResourceConfig{
			CpuLimit:   1,
			MemLimitMb: 20,
		},
	}
	tasks[i+1] = &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId: &mesos.TaskID{Value: &taskID},
			State:       task.TaskState_SUCCEEDED,
		},
		Config:     &taskConf,
		InstanceId: i,
		JobId:      jobID,
	}
	return tasks
}

func (suite *recoveryTestSuite) TestRefillTaskQueue() {
	// Create jobs. each with different number of tasks
	jobs := make([]*peloton.JobID, 4)
	for i := 0; i < 4; i++ {
		jobs[i] = &peloton.JobID{Value: fmt.Sprintf("TestJob_%d", i)}
	}

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), jobs[0]).
		Return(&job.RuntimeInfo{
			State:     job.JobState_RUNNING,
			GoalState: job.JobState_SUCCEEDED,
		}, nil)

	suite.activeJobsOps.EXPECT().
		GetAll(gomock.Any()).
		Return(jobs, nil)

	suite.jobConfigOps.EXPECT().
		Get(context.Background(), jobs[0], gomock.Any()).
		Return(suite.createJob(jobs[0], 10, 1), &models.ConfigAddOn{}, nil)
	suite.mockTaskStore.EXPECT().
		GetTasksForJobByRange(context.Background(), jobs[0], &task.InstanceRange{
			From: 0,
			To:   10,
		}).
		Return(suite.createTasks(jobs[0], 9, task.TaskState_INITIALIZED), nil)

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), jobs[1]).
		Return(&job.RuntimeInfo{
			State:     job.JobState_RUNNING,
			GoalState: job.JobState_SUCCEEDED,
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(context.Background(), jobs[1], gomock.Any()).
		Return(suite.createJob(jobs[1], 10, 10), &models.ConfigAddOn{}, nil)
	suite.mockTaskStore.EXPECT().
		GetTasksForJobByRange(context.Background(), jobs[1], &task.InstanceRange{
			From: 0,
			To:   10,
		}).
		Return(suite.createTasks(jobs[1], 9, task.TaskState_PENDING), nil)

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), jobs[2]).
		Return(&job.RuntimeInfo{
			State:     job.JobState_RUNNING,
			GoalState: job.JobState_SUCCEEDED,
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(context.Background(), jobs[2], gomock.Any()).
		Return(suite.createJob(jobs[2], 10, 1), &models.ConfigAddOn{}, nil)
	suite.mockTaskStore.EXPECT().
		GetTasksForJobByRange(context.Background(), jobs[2], &task.InstanceRange{
			From: 0,
			To:   10,
		}).
		Return(suite.createTasks(jobs[2], 9, task.TaskState_RUNNING), nil)

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), jobs[3]).
		Return(&job.RuntimeInfo{
			State:     job.JobState_RUNNING,
			GoalState: job.JobState_SUCCEEDED,
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(context.Background(), jobs[3], gomock.Any()).
		Return(suite.createJob(jobs[3], 10, 1), &models.ConfigAddOn{}, nil)
	suite.mockTaskStore.EXPECT().
		GetTasksForJobByRange(context.Background(), jobs[3], &task.InstanceRange{
			From: 0,
			To:   10,
		}).
		Return(suite.createTasks(jobs[3], 9, task.TaskState_LAUNCHED), nil)

	suite.recovery.Start()
	<-suite.recovery.finished
	time.Sleep(2 * time.Duration(hostmgrBackoffRetryInterval))

	// 2. check the queue content
	var resPoolID peloton.ResourcePoolID
	resPoolID.Value = "respool21"
	gangsSummary := suite.getQueueContent(resPoolID)

	suite.Equal(1, len(gangsSummary))

	// Job0 should not be recovered
	gangs := gangsSummary["TestJob_0"]
	suite.Equal(0, len(gangs))

	// Job1 should be recovered and should have 1 gang(9 tasks per gang) in the pending queue
	gangs = gangsSummary["TestJob_1"]
	suite.Equal(1, len(gangs))
	for _, gang := range gangs {
		suite.Equal(9, len(gang.GetTasks()))
	}

	// Checking total number of tasks in the tracker(9*3=27)
	// This includes tasks from TestJob_2 which are not re queued but are inserted in the tracker
	suite.Equal(suite.rmTaskTracker.GetSize(), int64(27))
}

func (suite *recoveryTestSuite) TestNonRunningJobError() {
	// Create jobs. each with different number of tasks
	jobs := make([]*peloton.JobID, 1)
	for i := 0; i < 1; i++ {
		jobs[i] = &peloton.JobID{Value: fmt.Sprintf("TestJob_%d", i)}
	}

	suite.activeJobsOps.EXPECT().GetAll(gomock.Any()).Return(jobs, nil)

	suite.jobRuntimeOps.EXPECT().
		Get(context.Background(), jobs[0]).
		Return(&job.RuntimeInfo{
			State:     job.JobState_PENDING,
			GoalState: job.JobState_SUCCEEDED,
		}, nil)

	jobConfig := suite.createJob(jobs[0], 10, 1)
	// Adding the invalid resource pool ID to simulate failure
	jobConfig.RespoolID = &peloton.ResourcePoolID{Value: "respool10"}

	suite.jobConfigOps.EXPECT().
		Get(context.Background(), jobs[0], gomock.Any()).
		Return(jobConfig, &models.ConfigAddOn{}, nil)

	suite.mockTaskStore.EXPECT().
		GetTasksForJobByRange(context.Background(), jobs[0], &task.InstanceRange{
			From: 0,
			To:   10,
		}).Return(suite.createTasks(jobs[0], 10, task.TaskState_PENDING), nil)

	suite.recovery.Start()
	<-suite.recovery.finished

	time.Sleep(2 * time.Duration(hostmgrBackoffRetryInterval))

	// 2. check the queue content
	var resPoolID peloton.ResourcePoolID
	resPoolID.Value = "respool21"
	gangsSummary := suite.getQueueContent(resPoolID)

	// Job0 should not be recovered
	gangs := gangsSummary["TestJob_0"]
	suite.Equal(0, len(gangs))

	// stopping handler
	suite.Nil(suite.recovery.Stop())
}

func (suite *recoveryTestSuite) TestAddRunningTasksError() {
	// Create jobs. each with different number of tasks
	jobs := make([]*peloton.JobID, 1)
	for i := 0; i < 1; i++ {
		jobs[i] = &peloton.JobID{Value: fmt.Sprintf("TestJob_%d", i)}
	}

	jobConfig := suite.createJob(jobs[0], 10, 1)
	// Adding the invalid resource pool ID to simulate failure
	jobConfig.RespoolID = &peloton.ResourcePoolID{Value: "respool10"}

	tasks := suite.createTasks(jobs[0], 10, task.TaskState_PENDING)
	var taskInfos []*task.TaskInfo

	for _, info := range tasks {
		taskInfos = append(taskInfos, info)
	}

	val, err := suite.recovery.addRunningTasks(taskInfos, jobConfig)
	suite.Error(err)
	suite.EqualError(err, "respool respool10 does not exist")
	suite.Equal(val, 0)
}

func (suite *recoveryTestSuite) TestAddRunningTasks() {
	suite.T().SkipNow()
	// Create jobs. each with different number of tasks
	jobs := make([]*peloton.JobID, 1)
	for i := 0; i < 1; i++ {
		jobs[i] = &peloton.JobID{Value: uuid.New()}
	}

	jobConfig := suite.createJob(jobs[0], 10, 1)

	tasks := suite.createTasks(jobs[0], 10, task.TaskState_RUNNING)
	var taskInfos []*task.TaskInfo

	for _, info := range tasks {
		taskInfos = append(taskInfos, info)
	}

	tracker := task_mocks.NewMockTracker(suite.mockCtrl)
	tracker.EXPECT().AddTask(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any()).
		Return(errors.New("error"))
	suite.recovery.tracker = tracker

	val, err := suite.recovery.addRunningTasks(taskInfos, jobConfig)
	suite.Error(err)
	suite.EqualError(err, "unable to add running task to tracker: error")
	suite.Equal(val, 0)

	tracker.EXPECT().AddTask(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any()).
		Return(nil)
	tracker.EXPECT().AddResources(gomock.Any()).
		Return(errors.New("error")).Times(1)
	val, err = suite.recovery.addRunningTasks(taskInfos, jobConfig)
	suite.Error(err)
	suite.EqualError(err, "could not add resources: error")
	suite.Equal(val, 0)

	node, err := suite.resourceTree.Get(&peloton.ResourcePoolID{Value: "respool21"})
	t := &resmgr.Task{
		Id: &peloton.TaskID{Value: fmt.Sprintf("%s-%d", jobs[0].GetValue(), 1)},
	}
	rmTask, err := rm_task.CreateRMTask(
		tally.NoopScope,
		t,
		nil,
		node,
		&rm_task.Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
			PolicyName:       rm_task.ExponentialBackOffPolicy,
		},
	)
	// Transitioning rmtask to RUNNING , by that it will not move to
	// RUNNING state again in test
	err = rmTask.TransitTo(task.TaskState_RUNNING.String())
	suite.NoError(err)

	tracker.EXPECT().AddTask(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any()).
		Return(nil).AnyTimes()
	tracker.EXPECT().AddResources(gomock.Any()).Return(nil).AnyTimes()
	tracker.EXPECT().GetTask(gomock.Any()).Return(rmTask).AnyTimes()
	val, err = suite.recovery.addRunningTasks(taskInfos, jobConfig)
	suite.Error(err)
	suite.Contains(err.Error(), "transition failed in task state machine")
	suite.Equal(val, 0)
}

// TestLoadTasksInRange tests classifying the tasks of the job as running and non-running
func (suite *recoveryTestSuite) TestLoadTasksInRange() {
	jobID := &peloton.JobID{Value: uuid.New()}

	tasks := make(map[uint32]*task.TaskInfo)
	for s := range task.TaskState_name {
		tasks[uint32(s)] = suite.createTasks(jobID, 1, task.TaskState(s))[0]
	}

	suite.mockTaskStore.EXPECT().
		GetTasksForJobByRange(gomock.Any(), jobID, &task.InstanceRange{
			From: 0,
			To:   uint32(len(tasks)),
		}).Return(tasks, nil)

	nonRunningTasks, runningTasks, err := suite.recovery.loadTasksInRange(
		context.Background(),
		jobID.GetValue(),
		0,
		uint32(len(tasks)),
	)
	suite.NoError(err)
	suite.Len(runningTasks, len(runningTaskStates))
	suite.Len(nonRunningTasks, len(tasks)-len(runningTaskStates)-len(taskStatesToSkip))
}

func (suite *recoveryTestSuite) TestLoadTasksInRangeError() {
	_, _, err := suite.recovery.loadTasksInRange(context.Background(), "", 100, 0)
	suite.Error(err)
	suite.Contains(err.Error(), "invalid job instance range")
	_, _, err = suite.recovery.loadTasksInRange(context.Background(), "", 100, 100)
	suite.NoError(err)
}

func (suite *recoveryTestSuite) TestStartStop() {
	suite.True(suite.recovery.lifecycle.Start())
	// Starting recovery handler should be no-op since
	// lifecycle manager is already started
	suite.Nil(suite.recovery.Start())

	suite.True(suite.recovery.lifecycle.Stop())
	// Stopping recovery handler should be no-op since
	// lifecycle manager is already stopped
	suite.Nil(suite.recovery.Stop())
}

func (suite *recoveryTestSuite) TestRecoverNonRunningTasks_Stop() {
	// Add dummy EnqueueGangsRequest to recovery.nonRunningTasks
	suite.recovery.nonRunningTasks = []*resmgrsvc.EnqueueGangsRequest{
		{},
	}

	// Call to recoverNonRunningTasks should be no-op
	// since lifecycle.StopCh() is closed
	suite.recovery.recoverNonRunningTasks()
	<-suite.recovery.finished
}

func TestResmgrRecovery(t *testing.T) {
	suite.Run(t, new(recoveryTestSuite))
}
