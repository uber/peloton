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
	"encoding/base64"
	"fmt"
	"sync"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	resmocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"
	lmmocks "github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/rpc"
	"github.com/uber/peloton/pkg/common/util"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	goalstatemocks "github.com/uber/peloton/pkg/jobmgr/goalstate/mocks"
	"github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr"
	"github.com/uber/peloton/pkg/storage/objects"
	ormstore "github.com/uber/peloton/pkg/storage/objects"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	_testJobID     = "bca875f5-322a-4439-b0c9-63e3cf9f982e"
	taskIDFmt      = _testJobID + "-%d-%s"
	testPort       = uint32(100)
	testSecretPath = "/tmp/secret"
	testSecretStr  = "test-data"
	testSecretID   = "bca875f5-322a-4439-b0c9-63e3cf9f982f"
)

var (
	_defaultResourceConfig = task.ResourceConfig{
		CpuLimit:    10,
		MemLimitMb:  10,
		DiskLimitMb: 10,
		FdLimit:     10,
	}
)

func createLaunchableTaskInfos(
	numTasks int,
	useSecrets bool,
) map[string]*lifecyclemgr.LaunchableTaskInfo {
	launchableTaskInfos := make(map[string]*lifecyclemgr.LaunchableTaskInfo, numTasks)

	mesosContainerizer := mesos.ContainerInfo_MESOS

	for i := 0; i < numTasks; i++ {
		tmp, _ := createTestTask(i)
		taskID := &peloton.TaskID{
			Value: tmp.JobId.Value + "-" + fmt.Sprint(tmp.InstanceId),
		}

		if useSecrets {
			tmp.GetConfig().Container = &mesos.ContainerInfo{
				Type: &mesosContainerizer,
			}
			tmp.GetConfig().GetContainer().Volumes = []*mesos.Volume{
				util.CreateSecretVolume(testSecretPath, testSecretID)}
		}

		launchableTaskInfos[taskID.GetValue()] = &lifecyclemgr.LaunchableTaskInfo{
			TaskInfo: tmp,
		}
	}
	return launchableTaskInfos
}

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
	jobFactory      *cachedmocks.MockJobFactory
	goalStateDriver *goalstatemocks.MockDriver
	cachedJob       *cachedmocks.MockJob
	cachedTask      *cachedmocks.MockTask
	lmMock          *lmmocks.MockManager
	secretInfoOps   *objectmocks.MockSecretInfoOps
	taskConfigV2Ops *objectmocks.MockTaskConfigV2Ops
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
	suite.lmMock = lmmocks.NewMockManager(suite.ctrl)
	suite.taskConfigV2Ops = objectmocks.NewMockTaskConfigV2Ops(suite.ctrl)
	suite.secretInfoOps = objectmocks.NewMockSecretInfoOps(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.ctrl)
	suite.config = &Config{
		PlacementDequeueLimit: 100,
	}
	suite.pp = &processor{
		config:          suite.config,
		resMgrClient:    suite.resMgrClient,
		metrics:         suite.metrics,
		lm:              suite.lmMock,
		taskConfigV2Ops: suite.taskConfigV2Ops,
		secretInfoOps:   suite.secretInfoOps,
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

	// Generate Placements per host offer.
	for i := 0; i < numHostOffers; i++ {
		p := createPlacements([]*task.TaskInfo{testTasks[i]}, hostOffers[i])
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
	testTask, _ := createTestTask(0) // taskinfo.
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements([]*task.TaskInfo{testTask}, hostOffer)

	gomock.InOrder(
		suite.jobFactory.EXPECT().
			GetJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(0)).
			Return(suite.cachedTask, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), testTask.JobId, uint32(0), gomock.Any()).
			Return(testTask.Config, &models.ConfigAddOn{}, nil),
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any(), false).
			Return(nil, nil, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),

		suite.lmMock.EXPECT().
			Launch(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				nil,
			).Return(
			nil,
		),

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

// TestTaskPlacementKillSkippedTasks tests processPlacement action to simulate
// resmgr kill for skipped tasks.
func (suite *PlacementTestSuite) TestTaskPlacementKillSkippedTasks() {
	testTask, _ := createTestTask(0) // taskinfo
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements([]*task.TaskInfo{testTask}, hostOffer)
	taskID := &peloton.TaskID{
		Value: testTask.JobId.Value + "-" + fmt.Sprint(testTask.InstanceId),
	}

	emptyTaskInfos := make(map[string]*lifecyclemgr.LaunchableTaskInfo, 0)
	gomock.InOrder(
		suite.jobFactory.EXPECT().
			GetJob(testTask.JobId).Return(nil),

		suite.lmMock.EXPECT().
			Launch(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				emptyTaskInfos,
				nil,
			).Return(
			nil,
		),
		suite.resMgrClient.EXPECT().
			KillTasks(gomock.Any(), &resmgrsvc.KillTasksRequest{
				Tasks: []*peloton.TaskID{taskID},
			}).Return(&resmgrsvc.KillTasksResponse{}, nil),
	)
	suite.pp.processPlacement(context.Background(), p)
}

// TestTaskPlacementKilledTask tests launching a task which has goalstate KILLED
// This task should be skipped.
func (suite *PlacementTestSuite) TestTaskPlacementKilledTask() {
	testTask, _ := createTestTask(0) // taskinfo
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements([]*task.TaskInfo{testTask}, hostOffer)
	emptyTaskInfos := make(map[string]*lifecyclemgr.LaunchableTaskInfo, 0)
	testTask.Runtime.State = task.TaskState_KILLED
	testTask.Runtime.GoalState = task.TaskState_KILLED

	taskID := &peloton.TaskID{
		Value: testTask.JobId.Value + "-" + fmt.Sprint(testTask.InstanceId),
	}

	req := &resmgrsvc.KillTasksRequest{Tasks: []*peloton.TaskID{taskID}}
	resp := &resmgrsvc.KillTasksResponse{}
	gomock.InOrder(
		suite.jobFactory.EXPECT().
			GetJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(0)).
			Return(suite.cachedTask, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), testTask.JobId, uint32(0), gomock.Any()).
			Return(testTask.Config, &models.ConfigAddOn{}, nil),
		suite.lmMock.EXPECT().
			Launch(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				emptyTaskInfos,
				nil,
			).Return(
			nil,
		),
		suite.resMgrClient.EXPECT().
			KillTasks(gomock.Any(), req).
			Return(resp, nil),
	)
	suite.pp.processPlacement(context.Background(), p)
}

// TestTaskPlacementKillResmgrTaskError tests launching a task which has
// goalstate KILLED. This task should be skipped. This test simulates failure
// when we try to send a kill request to resmgr for this skipped task.
func (suite *PlacementTestSuite) TestTaskPlacementKillResmgrTaskError() {
	testTask, _ := createTestTask(0) // taskinfo
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements([]*task.TaskInfo{testTask}, hostOffer)
	emptyTaskInfos := make(map[string]*lifecyclemgr.LaunchableTaskInfo, 0)
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
		suite.jobFactory.EXPECT().
			GetJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(0)).
			Return(suite.cachedTask, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), testTask.JobId, uint32(0), gomock.Any()).
			Return(testTask.Config, &models.ConfigAddOn{}, nil),
		suite.lmMock.EXPECT().
			Launch(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				emptyTaskInfos,
				nil,
			).Return(
			nil,
		),
		suite.resMgrClient.EXPECT().
			KillTasks(gomock.Any(), req).
			Return(resp, nil),
	)
	suite.pp.processPlacement(context.Background(), p)
}

// TestTaskPlacementKilledRunningTask tests the case where we want to kill a
// skipped task (which is RUNNING) in resmgr as well as re-enqueue it in the
// jobmgr goalstate engine, because its goalstate is KILLED.
func (suite *PlacementTestSuite) TestTaskPlacementKilledRunningTask() {
	testTask, _ := createTestTask(0) // taskinfo
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements([]*task.TaskInfo{testTask}, hostOffer)
	emptyTaskInfos := make(map[string]*lifecyclemgr.LaunchableTaskInfo, 0)
	testTask.Runtime.GoalState = task.TaskState_KILLED

	taskID := &peloton.TaskID{
		Value: testTask.JobId.Value + "-" + fmt.Sprint(testTask.InstanceId),
	}

	req := &resmgrsvc.KillTasksRequest{Tasks: []*peloton.TaskID{taskID}}
	resp := &resmgrsvc.KillTasksResponse{}
	gomock.InOrder(
		suite.jobFactory.EXPECT().
			GetJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(0)).
			Return(suite.cachedTask, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), testTask.JobId, uint32(0), gomock.Any()).
			Return(testTask.Config, &models.ConfigAddOn{}, nil),
		suite.goalStateDriver.EXPECT().
			EnqueueTask(testTask.JobId, gomock.Any(), gomock.Any()).Return(),
		suite.lmMock.EXPECT().
			Launch(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				emptyTaskInfos,
				nil,
			).Return(
			nil,
		),
		suite.resMgrClient.EXPECT().
			KillTasks(gomock.Any(), req).
			Return(resp, nil),
	)

	suite.pp.processPlacement(context.Background(), p)
}

// TestTaskPlacementDBError tests the case where PatchTasks fails with a DB error.
func (suite *PlacementTestSuite) TestTaskPlacementDBError() {
	testTask, _ := createTestTask(0) // taskinfo
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements([]*task.TaskInfo{testTask}, hostOffer)

	// Introduce non-transient DB error. Task is dropped in this case.
	gomock.InOrder(
		suite.jobFactory.EXPECT().
			GetJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(0)).
			Return(suite.cachedTask, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), testTask.JobId, uint32(0), gomock.Any()).
			Return(testTask.Config, &models.ConfigAddOn{}, nil),
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any(), false).
			Return(nil, nil, fmt.Errorf("fake db error")),
		suite.lmMock.EXPECT().
			Launch(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				nil,
			).Return(
			nil,
		),
	)

	suite.pp.processPlacement(context.Background(), p)

	// Introduce non-transient DB error. Task is dropped in this case.
	gomock.InOrder(
		suite.jobFactory.EXPECT().
			GetJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(0)).
			Return(suite.cachedTask, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), testTask.JobId, uint32(0), gomock.Any()).
			Return(testTask.Config, &models.ConfigAddOn{}, nil),
		// Simulate transient failure where PatchTasks succeeds on second try.
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any(), false).
			Return(nil, nil, yarpcerrors.DeadlineExceededErrorf("transient error")),
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any(), false).
			Return(nil, nil, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.lmMock.EXPECT().
			Launch(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				nil,
			).Return(
			nil,
		),
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

// TestLaunchError tests failure in lifecyclemgr.Launch().
func (suite *PlacementTestSuite) TestLaunchError() {
	testTask, _ := createTestTask(0) // taskinfo
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements([]*task.TaskInfo{testTask}, hostOffer)
	taskID := &peloton.TaskID{
		Value: testTask.JobId.Value + "-" + fmt.Sprint(testTask.InstanceId),
	}

	gomock.InOrder(
		suite.jobFactory.EXPECT().
			GetJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(0)).
			Return(suite.cachedTask, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), testTask.JobId, uint32(0), gomock.Any()).
			Return(testTask.Config, &models.ConfigAddOn{}, nil),
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any(), false).
			Return(nil, nil, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.lmMock.EXPECT().
			Launch(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				nil,
			).Return(
			fmt.Errorf("fake launch error"),
		),
		suite.resMgrClient.EXPECT().
			KillTasks(gomock.Any(), &resmgrsvc.KillTasksRequest{
				Tasks: []*peloton.TaskID{taskID},
			}).
			Return(&resmgrsvc.KillTasksResponse{}, nil),
		suite.jobFactory.EXPECT().
			AddJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any(), false).
			Return(nil, nil, nil),
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

// TestLaunchErrorAndResmgrEnqueueError tests failure in lifecyclemgr.Launch()
// followed by an error enqueuing the tasks to resmgr.
func (suite *PlacementTestSuite) TestLaunchErrorAndResmgrEnqueueError() {
	testTask, _ := createTestTask(0) // taskinfo.
	rs := createResources(float64(1))
	hostOffer := createHostOffer(0, rs)
	p := createPlacements([]*task.TaskInfo{testTask}, hostOffer)
	taskID := &peloton.TaskID{
		Value: testTask.JobId.Value + "-" + fmt.Sprint(testTask.InstanceId),
	}

	gomock.InOrder(
		suite.jobFactory.EXPECT().
			GetJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(0)).
			Return(suite.cachedTask, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), testTask.JobId, uint32(0), gomock.Any()).
			Return(testTask.Config, &models.ConfigAddOn{}, nil),
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any(), false).
			Return(nil, nil, nil),
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(testTask.Runtime, nil),
		suite.lmMock.EXPECT().
			Launch(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
				nil,
			).Return(
			fmt.Errorf("fake launch error"),
		),
		suite.resMgrClient.EXPECT().
			KillTasks(gomock.Any(), &resmgrsvc.KillTasksRequest{
				Tasks: []*peloton.TaskID{taskID},
			}).
			Return(&resmgrsvc.KillTasksResponse{}, nil),
		suite.jobFactory.EXPECT().
			AddJob(testTask.JobId).Return(suite.cachedJob),
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any(), false).
			Return(nil, nil, fmt.Errorf("fake db error")),
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
		p := createPlacements(testTasks, hostOffers[i])
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

// TestTaskPlacementProcessorProcessNormal tests the normal case of process call.
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
		p := createPlacements(testTasks, hostOffers[i])
		// Simulate port mismatch failure which will trigger TerminateLease().
		// This will ensure that the selected ports are fewer than what the task needs for launch.
		p.Ports = []uint32{}
		placements[i] = p
	}

	// start the lifeCycle only, otherwise other methods in
	// pp.Start would also be called and make test hard
	suite.pp.lifeCycle.Start()
	suite.resMgrClient.EXPECT().
		GetPlacements(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.GetPlacementsResponse{Placements: placements}, nil)
	suite.jobFactory.EXPECT().
		GetJob(testTasks[0].JobId).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), uint32(0)).
		Return(suite.cachedTask, nil)
	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(testTasks[0].Runtime, nil)
	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(gomock.Any(), testTasks[0].JobId, uint32(0), gomock.Any()).
		Return(testTasks[0].Config, &models.ConfigAddOn{}, nil)
	suite.lmMock.EXPECT().
		TerminateLease(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("test err"))
	suite.pp.process()
	// wait for all work to be done
	time.Sleep(time.Second)
}

func (suite *PlacementTestSuite) TestInitPlacementProcessor() {
	t := rpc.NewTransport()
	outbound := t.NewOutbound(nil)
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     common.PelotonJobManager,
		Inbounds: nil,
		Outbounds: yarpc.Outbounds{
			common.PelotonHostManager: transport.Outbounds{
				Unary: outbound,
			},
			"testClient": transport.Outbounds{
				Unary: t.NewSingleOutbound("localhost"),
			},
		},
		Metrics: yarpc.MetricsConfig{
			Tally: tally.NoopScope,
		},
	})

	pp := InitProcessor(
		dispatcher,
		"testClient",
		suite.jobFactory,
		suite.goalStateDriver,
		api.V0,
		&ormstore.Store{},
		suite.config,
		suite.scope,
	).(*processor)
	suite.Equal(suite.jobFactory, pp.jobFactory)
	suite.Equal(suite.goalStateDriver, pp.goalStateDriver)
	suite.Equal(suite.config, pp.config)
	suite.NotNil(pp.metrics)
	suite.NotNil(pp.lifeCycle)
	suite.NotNil(pp.resMgrClient)
}

// TestPopulateSecrets tests the populateSecrets function
// to make sure that all the tasks in launchableTasks list
// that contain a volume/secret will be populated with
// the actual secret data fetched from the secret store.
func (suite *PlacementTestSuite) TestPopulateSecrets() {
	// Expected Secret

	secretInfoObject := &objects.SecretInfoObject{
		SecretID:     testSecretID,
		JobID:        _testJobID,
		Version:      0,
		Valid:        true,
		Path:         testSecretPath,
		Data:         base64.StdEncoding.EncodeToString([]byte(testSecretStr)),
		CreationTime: time.Now(),
	}
	suite.secretInfoOps.EXPECT().
		GetSecret(gomock.Any(), testSecretID).
		Return(secretInfoObject, nil).Times(5)
	launchableTasksWithSecrets := createLaunchableTaskInfos(5, true)
	launchableTasks := createLaunchableTaskInfos(5, false)
	for id, task := range launchableTasksWithSecrets {
		launchableTasks[id] = task
	}
	suite.pp.populateSecrets(context.Background(), launchableTasks)

	// Test secret not found error.
	launchableTasks = createLaunchableTaskInfos(1, true)
	for _, t := range launchableTasks {
		suite.jobFactory.EXPECT().GetJob(t.JobId).Return(suite.cachedJob)
		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), gomock.Any(), false).
			Do(func(ctx context.Context, runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff, slaAware bool) {
				suite.Equal(task.TaskState_KILLED, runtimeDiffs[0][jobmgrcommon.GoalStateField])
				suite.Equal("REASON_SECRET_NOT_FOUND", runtimeDiffs[0][jobmgrcommon.ReasonField])
			}).Return(nil, nil, nil)
	}
	suite.secretInfoOps.EXPECT().
		GetSecret(gomock.Any(), testSecretID).
		Return(nil, yarpcerrors.NotFoundErrorf("not found"))
	suite.pp.populateSecrets(context.Background(), launchableTasks)

	// Test secret transient error.
	suite.secretInfoOps.EXPECT().
		GetSecret(gomock.Any(), testSecretID).
		Return(nil, yarpcerrors.DeadlineExceededErrorf("deadline exceeded"))
	skipped := suite.pp.populateSecrets(context.Background(), launchableTasks)
	suite.Equal(skipped, launchableTasks)
}

// createPlacements creates the placement.
func createPlacements(
	tasks []*task.TaskInfo,
	hostOffer *hostsvc.HostOffer,
) *resmgr.Placement {
	placementTasks := make([]*resmgr.Placement_Task, len(tasks))
	for i, t := range tasks {
		taskID := &peloton.TaskID{
			Value: t.JobId.Value + "-" + fmt.Sprint(t.InstanceId),
		}
		placementTasks[i] = &resmgr.Placement_Task{
			PelotonTaskID: taskID,
			MesosTaskID:   t.GetRuntime().GetMesosTaskId(),
		}
	}
	return &resmgr.Placement{
		AgentId:  hostOffer.AgentId,
		Hostname: hostOffer.Hostname,
		TaskIDs:  placementTasks,
		Ports:    []uint32{testPort},
	}
}
