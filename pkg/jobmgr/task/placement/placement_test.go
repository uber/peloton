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
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	resmocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/rpc"
	"github.com/uber/peloton/pkg/common/util"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	goalstatemocks "github.com/uber/peloton/pkg/jobmgr/goalstate/mocks"
	"github.com/uber/peloton/pkg/jobmgr/task/launcher"
	launchermocks "github.com/uber/peloton/pkg/jobmgr/task/launcher/mocks"
)

const (
	_testJobID = "bca875f5-322a-4439-b0c9-63e3cf9f982e"
	taskIDFmt  = _testJobID + "-%d-%s"
	testPort   = uint32(100)
)

var (
	_defaultResourceConfig = task.ResourceConfig{
		CpuLimit:    10,
		MemLimitMb:  10,
		DiskLimitMb: 10,
		FdLimit:     10,
	}
)

func createTestTask(instanceID int) (*task.TaskInfo, jobmgrcommon.RuntimeDiff) {
	var tid = fmt.Sprintf(taskIDFmt, instanceID, uuid.NewUUID().String())
	mesosID := &mesos.TaskID{
		Value: &tid,
	}

	return &task.TaskInfo{
			JobId: &peloton.JobID{
				Value: _testJobID,
			},
			InstanceId: uint32(instanceID),
			Config: &task.TaskConfig{
				Name:     _testJobID,
				Resource: &_defaultResourceConfig,
				Ports: []*task.PortConfig{
					{
						Name:    "port",
						EnvName: "PORT",
					},
				},
			},
			Runtime: &task.RuntimeInfo{
				MesosTaskId: mesosID,
				State:       task.TaskState_PENDING,
				GoalState:   task.TaskState_SUCCEEDED,
			},
		},
		jobmgrcommon.RuntimeDiff{
			jobmgrcommon.MesosTaskIDField: mesosID,
			jobmgrcommon.StateField:       task.TaskState_PENDING,
			jobmgrcommon.GoalStateField:   task.TaskState_SUCCEEDED,
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

type PlacementTestSuite struct {
	suite.Suite

	ctrl            *gomock.Controller
	resMgrClient    *resmocks.MockResourceManagerServiceYARPCClient
	pp              *processor
	taskLauncher    *launchermocks.MockLauncher
	jobFactory      *cachedmocks.MockJobFactory
	goalStateDriver *goalstatemocks.MockDriver
	cachedJob       *cachedmocks.MockJob
	cachedTask      *cachedmocks.MockTask
	config          *Config
	metrics         *Metrics
	scope           tally.Scope
}

func (suite *PlacementTestSuite) SetupTest() {
	suite.scope = tally.NewTestScope("", map[string]string{})
	suite.metrics = NewMetrics(suite.scope)
	suite.ctrl = gomock.NewController(suite.T())
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
	suite.resMgrClient =
		resmocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)
	suite.taskLauncher = launchermocks.NewMockLauncher(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.ctrl)
	suite.config = &Config{
		PlacementDequeueLimit: 100,
	}
	suite.pp = &processor{
		config:          suite.config,
		resMgrClient:    suite.resMgrClient,
		metrics:         suite.metrics,
		taskLauncher:    suite.taskLauncher,
		jobFactory:      suite.jobFactory,
		goalStateDriver: suite.goalStateDriver,
		lifeCycle:       lifecycle.NewLifeCycle(),
	}

}

func (suite *PlacementTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestPlacementTestSuite(t *testing.T) {
	suite.Run(t, new(PlacementTestSuite))
}

// This test ensures that multiple placements returned from resmgr can be properly placed by hostmgr
func (suite *PlacementTestSuite) TestMultipleTasksPlacements() {
	// generate 25 test tasks
	numTasks := 25
	testTasks := make([]*task.TaskInfo, numTasks)
	placements := make([]*resmgr.Placement, numTasks)
	for i := 0; i < numTasks; i++ {
		tmp, _ := createTestTask(i)
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
		suite.resMgrClient.EXPECT().
			GetPlacements(
				gomock.Any(),
				gomock.Any()).
			Return(&resmgrsvc.GetPlacementsResponse{Placements: placements}, nil),
	)

	gPlacements, err := suite.pp.getPlacements()

	if err != nil {
		suite.Error(err)
	}
	suite.Equal(placements, gPlacements)
}

// This test ensures placement engine, one start can dequeue placements, and
// then call launcher to launch the placements.
func (suite *PlacementTestSuite) TestTaskPlacementNoError() {
	testTask, testRuntimeDiff := createTestTask(0) // taskinfo
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements(testTask, hostOffer)

	taskID := &peloton.TaskID{
		Value: testTask.JobId.Value + "-" + fmt.Sprint(testTask.InstanceId),
	}

	gomock.InOrder(
		suite.taskLauncher.EXPECT().
			GetLaunchableTasks(gomock.Any(), p.Tasks, p.Hostname, p.AgentId, p.Ports).
			Return(
				map[string]*launcher.LaunchableTask{
					taskID.Value: {
						RuntimeDiff: testRuntimeDiff,
						Config:      testTask.Config,
					},
				},
				nil,
				nil),
		suite.jobFactory.EXPECT().
			AddJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(0)).
			Return(suite.cachedTask, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any()).Return(nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.taskLauncher.EXPECT().
			CreateLaunchableTasks(gomock.Any(), gomock.Any()).Return(nil, nil),
		suite.taskLauncher.EXPECT().
			ProcessPlacement(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
		suite.goalStateDriver.EXPECT().
			EnqueueTask(testTask.JobId, testTask.InstanceId, gomock.Any()).Return(),
		suite.jobFactory.EXPECT().
			AddJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1*time.Second),
		suite.goalStateDriver.EXPECT().
			EnqueueJob(testTask.JobId, gomock.Any()).Return(),
	)

	suite.pp.processPlacement(context.Background(), p)
}

func (suite *PlacementTestSuite) TestTaskPlacementGetTaskError() {
	testTask, _ := createTestTask(0) // taskinfo
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements(testTask, hostOffer)

	gomock.InOrder(
		suite.taskLauncher.EXPECT().
			GetLaunchableTasks(gomock.Any(), p.Tasks, p.Hostname, p.AgentId, p.Ports).
			Return(nil, nil, fmt.Errorf("fake launch error")),
		suite.taskLauncher.EXPECT().
			TryReturnOffers(gomock.Any(), gomock.Any(), p).Return(nil),
	)

	suite.pp.processPlacement(context.Background(), p)
}

func (suite *PlacementTestSuite) TestTaskPlacementKilledTask() {
	testTask, runtimeDiff := createTestTask(0) // taskinfo
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements(testTask, hostOffer)
	testTask.Runtime.State = task.TaskState_KILLED
	testTask.Runtime.GoalState = task.TaskState_KILLED

	taskID := &peloton.TaskID{
		Value: testTask.JobId.Value + "-" + fmt.Sprint(testTask.InstanceId),
	}

	req := &resmgrsvc.KillTasksRequest{Tasks: []*peloton.TaskID{taskID}}
	resp := &resmgrsvc.KillTasksResponse{}
	gomock.InOrder(
		suite.taskLauncher.EXPECT().
			GetLaunchableTasks(gomock.Any(), p.Tasks, p.Hostname, p.AgentId, p.Ports).
			Return(
				map[string]*launcher.LaunchableTask{
					taskID.Value: {
						RuntimeDiff: runtimeDiff,
						Config:      testTask.Config,
					},
				},
				nil,
				nil),
		suite.jobFactory.EXPECT().
			AddJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(0)).
			Return(suite.cachedTask, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.resMgrClient.EXPECT().
			KillTasks(gomock.Any(), req).
			Return(resp, nil),
	)

	suite.pp.processPlacement(context.Background(), p)
}

func (suite *PlacementTestSuite) TestTaskPlacementKilledJob() {
	testTask, _ := createTestTask(0) // taskinfo
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements(testTask, hostOffer)
	testTask.Runtime.State = task.TaskState_KILLED
	testTask.Runtime.GoalState = task.TaskState_KILLED

	taskID := &peloton.TaskID{
		Value: testTask.JobId.Value + "-" + fmt.Sprint(testTask.InstanceId),
	}

	req := &resmgrsvc.KillTasksRequest{Tasks: []*peloton.TaskID{taskID}}
	resp := &resmgrsvc.KillTasksResponse{
		Error: []*resmgrsvc.KillTasksResponse_Error{
			{
				NotFound: &resmgrsvc.TasksNotFound{
					Message: "Task Not Found",
					Task:    taskID,
				},
			},
		},
	}
	gomock.InOrder(
		suite.taskLauncher.EXPECT().
			GetLaunchableTasks(gomock.Any(), p.Tasks, p.Hostname, p.AgentId, p.Ports).
			Return(
				nil,
				[]*peloton.TaskID{taskID},
				nil),
		suite.resMgrClient.EXPECT().
			KillTasks(gomock.Any(), req).
			Return(resp, nil),
	)

	suite.pp.processPlacement(context.Background(), p)
}

func (suite *PlacementTestSuite) TestTaskPlacementKilledRunningTask() {
	testTask, runtimeDiff := createTestTask(0) // taskinfo
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements(testTask, hostOffer)
	testTask.Runtime.GoalState = task.TaskState_KILLED

	taskID := &peloton.TaskID{
		Value: testTask.JobId.Value + "-" + fmt.Sprint(testTask.InstanceId),
	}

	expectedRuntime := make(map[uint32]*task.RuntimeInfo)
	expectedRuntime[testTask.InstanceId] = testTask.Runtime

	req := &resmgrsvc.KillTasksRequest{Tasks: []*peloton.TaskID{taskID}}
	resp := &resmgrsvc.KillTasksResponse{}
	gomock.InOrder(
		suite.taskLauncher.EXPECT().
			GetLaunchableTasks(gomock.Any(), p.Tasks, p.Hostname, p.AgentId, p.Ports).
			Return(
				map[string]*launcher.LaunchableTask{
					taskID.Value: {
						RuntimeDiff: runtimeDiff,
						Config:      testTask.Config,
					},
				},
				nil,
				nil),
		suite.jobFactory.EXPECT().
			AddJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(0)).
			Return(suite.cachedTask, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.goalStateDriver.EXPECT().
			EnqueueTask(testTask.JobId, gomock.Any(), gomock.Any()).Return(),
		suite.resMgrClient.EXPECT().
			KillTasks(gomock.Any(), req).
			Return(resp, nil),
	)

	suite.pp.processPlacement(context.Background(), p)
}

func (suite *PlacementTestSuite) TestTaskPlacementDBError() {
	testTask, runtimeDiff := createTestTask(0) // taskinfo
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements(testTask, hostOffer)

	taskID := &peloton.TaskID{
		Value: testTask.JobId.Value + "-" + fmt.Sprint(testTask.InstanceId),
	}

	gomock.InOrder(
		suite.taskLauncher.EXPECT().
			GetLaunchableTasks(gomock.Any(), p.Tasks, p.Hostname, p.AgentId, p.Ports).
			Return(
				map[string]*launcher.LaunchableTask{
					taskID.Value: {
						RuntimeDiff: runtimeDiff,
						Config:      testTask.Config,
					},
				},
				nil,
				nil),
		suite.jobFactory.EXPECT().
			AddJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(0)).
			Return(suite.cachedTask, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any()).Return(fmt.Errorf("fake db error")),
	)

	suite.pp.processPlacement(context.Background(), p)
}

func (suite *PlacementTestSuite) TestTaskPlacementError() {
	testTask, runtimeDiff := createTestTask(0) // taskinfo
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements(testTask, hostOffer)

	taskID := &peloton.TaskID{
		Value: testTask.JobId.Value + "-" + fmt.Sprint(testTask.InstanceId),
	}

	gomock.InOrder(
		suite.taskLauncher.EXPECT().
			GetLaunchableTasks(gomock.Any(), p.Tasks, p.Hostname, p.AgentId, p.Ports).
			Return(
				map[string]*launcher.LaunchableTask{
					taskID.Value: {
						RuntimeDiff: runtimeDiff,
						Config:      testTask.Config,
					},
				},
				nil,
				nil),
		suite.jobFactory.EXPECT().
			AddJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(0)).
			Return(suite.cachedTask, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any()).Return(nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.taskLauncher.EXPECT().
			CreateLaunchableTasks(gomock.Any(), gomock.Any()).Return(nil, nil),
		suite.taskLauncher.EXPECT().
			ProcessPlacement(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("fake launch error")),
		suite.jobFactory.EXPECT().
			AddJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any()).Return(nil),
		suite.goalStateDriver.EXPECT().
			EnqueueTask(testTask.JobId, testTask.InstanceId, gomock.Any()).Return(),
		suite.cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1*time.Second),
		suite.goalStateDriver.EXPECT().
			EnqueueJob(testTask.JobId, gomock.Any()).Return(),
	)

	suite.pp.processPlacement(context.Background(), p)
}

func (suite *PlacementTestSuite) TestTaskPlacementPlacementResMgrError() {
	testTask, runtimeDiff := createTestTask(0) // taskinfo
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements(testTask, hostOffer)

	taskID := &peloton.TaskID{
		Value: testTask.JobId.Value + "-" + fmt.Sprint(testTask.InstanceId),
	}

	gomock.InOrder(
		suite.taskLauncher.EXPECT().
			GetLaunchableTasks(gomock.Any(), p.Tasks, p.Hostname, p.AgentId, p.Ports).
			Return(
				map[string]*launcher.LaunchableTask{
					taskID.Value: {
						RuntimeDiff: runtimeDiff,
						Config:      testTask.Config,
					},
				},
				nil,
				nil),
		suite.jobFactory.EXPECT().
			AddJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(0)).Return(suite.cachedTask, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any()).Return(nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.taskLauncher.EXPECT().
			CreateLaunchableTasks(gomock.Any(), gomock.Any()).Return(nil, nil),
		suite.taskLauncher.EXPECT().
			ProcessPlacement(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("fake launch error")),
		suite.jobFactory.EXPECT().
			AddJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any()).Return(fmt.Errorf("fake db error")),
	)

	suite.pp.processPlacement(context.Background(), p)
}

// TestTaskPlacementProcessorStartAndStop tests for normal start and stop
func (suite *PlacementTestSuite) TestTaskPlacementProcessorStartAndStop() {
	suite.resMgrClient.EXPECT().
		GetPlacements(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("test error")).
		AnyTimes()
	suite.NoError(suite.pp.Start())
	suite.NoError(suite.pp.Stop())
}

// TestTaskPlacementProcessorMultipleStart tests multiple starts in a row
func (suite *PlacementTestSuite) TestTaskPlacementProcessorMultipleStart() {
	suite.resMgrClient.EXPECT().
		GetPlacements(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("test error")).
		AnyTimes()
	suite.NoError(suite.pp.Start())
	suite.NoError(suite.pp.Start())
}

// TestTaskPlacementProcessorMultipleStop tests multiple stop in a row
func (suite *PlacementTestSuite) TestTaskPlacementProcessorMultipleStop() {
	suite.NoError(suite.pp.Stop())
	suite.NoError(suite.pp.Stop())
}

// TestTaskPlacementProcessorStopRun tests stop does make run() return
func (suite *PlacementTestSuite) TestTaskPlacementProcessorStopRun() {
	suite.resMgrClient.EXPECT().
		GetPlacements(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("test error")).
		AnyTimes()

	var runwg sync.WaitGroup
	runwg.Add(1)
	go func() {
		suite.pp.run()
		runwg.Done()

	}()

	suite.NoError(suite.pp.Stop())
	runwg.Wait()
}

// TestTaskPlacementProcessorProcessReturnUpOnStop test that process()
// would not process placement if stop is called.
func (suite *PlacementTestSuite) TestTaskPlacementProcessorProcessReturnUpOnStop() {
	numTasks := 1
	testTasks := make([]*task.TaskInfo, numTasks)
	placements := make([]*resmgr.Placement, numTasks)
	for i := 0; i < numTasks; i++ {
		tmp, _ := createTestTask(i)
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

	// start the lifeCycle only, otherwise other methods in
	// pp.Start would also be called and make test hard
	suite.pp.lifeCycle.Start()
	go suite.pp.lifeCycle.StopComplete()
	suite.pp.Stop()
	suite.resMgrClient.EXPECT().
		GetPlacements(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.GetPlacementsResponse{Placements: placements}, nil)
	suite.pp.process()
}

// TestTaskPlacementProcessorProcessNormal tests the normal case of process call
func (suite *PlacementTestSuite) TestTaskPlacementProcessorProcessNormal() {
	numTasks := 1
	testTasks := make([]*task.TaskInfo, numTasks)
	placements := make([]*resmgr.Placement, numTasks)
	for i := 0; i < numTasks; i++ {
		tmp, _ := createTestTask(i)
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

	// start the lifeCycle only, otherwise other methods in
	// pp.Start would also be called and make test hard
	suite.pp.lifeCycle.Start()
	suite.resMgrClient.EXPECT().
		GetPlacements(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.GetPlacementsResponse{Placements: placements}, nil)
	suite.taskLauncher.EXPECT().
		GetLaunchableTasks(gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, fmt.Errorf("test err"))
	suite.taskLauncher.EXPECT().
		TryReturnOffers(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("test err"))
	suite.pp.process()
	// wait for all work to be done
	time.Sleep(time.Second)
}

func (suite *PlacementTestSuite) TestInitPlacementProcessor() {
	t := rpc.NewTransport()
	outbounds := yarpc.Outbounds{
		"testClient": transport.Outbounds{
			Unary: t.NewSingleOutbound("localhost"),
		},
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      "test-service",
		Outbounds: outbounds,
	})

	pp := InitProcessor(
		dispatcher,
		"testClient",
		suite.jobFactory,
		suite.goalStateDriver,
		suite.taskLauncher,
		suite.config,
		suite.scope,
	).(*processor)
	suite.Equal(suite.jobFactory, pp.jobFactory)
	suite.Equal(suite.goalStateDriver, pp.goalStateDriver)
	suite.Equal(suite.taskLauncher, pp.taskLauncher)
	suite.Equal(suite.config, pp.config)
	suite.NotNil(pp.metrics)
	suite.NotNil(pp.lifeCycle)
	suite.NotNil(pp.resMgrClient)
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
