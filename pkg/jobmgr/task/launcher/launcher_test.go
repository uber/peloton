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

package launcher

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/volume"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/pkg/storage/objects"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/uber/peloton/pkg/common/backoff"
	"github.com/uber/peloton/pkg/common/util"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	jobmgrtask "github.com/uber/peloton/pkg/jobmgr/task"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
)

const (
	taskIDFmt            = _testJobID + "-%d-%s"
	testPort             = uint32(100)
	testSecretPath       = "/tmp/secret"
	testSecretStr        = "test-data"
	testTaskConfigData   = "../../../../example/thermos-executor-task-config.bin"
	testAssignedTaskData = "../../../../example/thermos-executor-assigned-task.bin"
)

var (
	_defaultResourceConfig = task.ResourceConfig{
		CpuLimit:    10,
		MemLimitMb:  10,
		DiskLimitMb: 10,
		FdLimit:     10,
	}
	lock = sync.RWMutex{}
)

type LauncherTestSuite struct {
	suite.Suite

	ctrl            *gomock.Controller
	mockHostMgr     *host_mocks.MockInternalHostServiceYARPCClient
	mockTaskStore   *store_mocks.MockTaskStore
	jobFactory      *cachedmocks.MockJobFactory
	cachedJob       *cachedmocks.MockJob
	cachedTask      *cachedmocks.MockTask
	mockVolumeStore *store_mocks.MockPersistentVolumeStore
	secretInfoOps   *objectmocks.MockSecretInfoOps
	testScope       tally.TestScope
	metrics         *Metrics
	taskLauncher    launcher
}

func TestLauncherTestSuite(t *testing.T) {
	suite.Run(t, new(LauncherTestSuite))
}

func (suite *LauncherTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())

	suite.mockHostMgr = host_mocks.NewMockInternalHostServiceYARPCClient(suite.ctrl)
	suite.mockTaskStore = store_mocks.NewMockTaskStore(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
	suite.mockVolumeStore = store_mocks.NewMockPersistentVolumeStore(suite.ctrl)
	suite.secretInfoOps = objectmocks.NewMockSecretInfoOps(suite.ctrl)

	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.metrics = NewMetrics(suite.testScope)
	suite.taskLauncher = launcher{
		hostMgrClient: suite.mockHostMgr,
		jobFactory:    suite.jobFactory,
		volumeStore:   suite.mockVolumeStore,
		taskStore:     suite.mockTaskStore,
		secretInfoOps: suite.secretInfoOps,
		metrics:       suite.metrics,
		retryPolicy:   backoff.NewRetryPolicy(5, 15*time.Millisecond),
	}
}

func (suite *LauncherTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func createTestTask(instanceID int) *LaunchableTaskInfo {
	var tid = fmt.Sprintf(taskIDFmt, instanceID, uuid.NewUUID().String())

	return &LaunchableTaskInfo{
		TaskInfo: &task.TaskInfo{
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
				MesosTaskId: &mesos.TaskID{
					Value: &tid,
				},
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
		Id:        &peloton.HostOfferID{Value: uuid.New()},
	}
}

// getTaskConfigData returns a sample binary-serialized TaskConfig
// thrift struct from file
func (suite *LauncherTestSuite) getTaskConfigData(t *testing.T) []byte {
	data, err := ioutil.ReadFile(testTaskConfigData)
	assert.NoError(t, err)
	return data
}

// getAssignedTaskData returns a sample binary-serialized AssignedTask
// thrift struct from file
func getAssignedTaskData(t *testing.T) []byte {
	data, err := ioutil.ReadFile(testAssignedTaskData)
	assert.NoError(t, err)
	return data
}

func (suite *LauncherTestSuite) TestGetLaunchableTasks() {
	// generate 25 test tasks
	numTasks := 25
	var tasks []*peloton.TaskID
	var selectedPorts []uint32
	taskInfos := make(map[string]*LaunchableTaskInfo)
	for i := 0; i < numTasks; i++ {
		tmp := createTestTask(i)
		taskID := &peloton.TaskID{
			Value: tmp.JobId.Value + "-" + fmt.Sprint(tmp.InstanceId),
		}
		tasks = append(tasks, taskID)
		taskInfos[taskID.Value] = tmp
		selectedPorts = append(selectedPorts, testPort+uint32(i))
	}
	unknownTasks := []*peloton.TaskID{
		{Value: "bcabcabc-bcab-bcab-bcab-bcabcabcabca-0"},
		{Value: "abcabcab-bcab-bcab-bcab-bcabcabcabca-1"},
	}
	rs := createResources(1)
	hostOffer := createHostOffer(0, rs)

	for i := 0; i < numTasks; i++ {
		taskID := tasks[i].GetValue()
		jobID, instanceID, err := util.ParseTaskID(taskID)
		suite.NoError(err)
		suite.jobFactory.EXPECT().
			GetJob(&peloton.JobID{Value: jobID}).Return(suite.cachedJob)
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(instanceID)).
			Return(suite.cachedTask, nil)
		suite.mockTaskStore.EXPECT().
			GetTaskConfig(gomock.Any(), &peloton.JobID{Value: jobID}, uint32(instanceID), gomock.Any()).
			Return(taskInfos[tasks[i].GetValue()].GetConfig(), &models.ConfigAddOn{}, nil)
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(taskInfos[tasks[i].GetValue()].GetRuntime(), nil).AnyTimes()
	}
	for _, taskID := range unknownTasks {
		jobID, _, err := util.ParseTaskID(taskID.GetValue())
		suite.NoError(err)
		suite.jobFactory.EXPECT().
			GetJob(&peloton.JobID{Value: jobID}).Return(nil)
	}

	tasks = append(tasks, unknownTasks...)
	launchableTasks, skippedTasks, err := suite.taskLauncher.GetLaunchableTasks(
		context.Background(), tasks, hostOffer.Hostname,
		hostOffer.AgentId, selectedPorts)
	suite.NoError(err)
	for _, launchableTask := range launchableTasks {
		runtimeDiff := launchableTask.RuntimeDiff
		suite.Equal(task.TaskState_LAUNCHED, runtimeDiff[jobmgrcommon.StateField])
		suite.Equal(hostOffer.Hostname, runtimeDiff[jobmgrcommon.HostField])
		suite.Equal(hostOffer.AgentId, runtimeDiff[jobmgrcommon.AgentIDField])
	}
	suite.EqualValues(unknownTasks, skippedTasks)
}
func (suite *LauncherTestSuite) TestGetLaunchableTasksStateful() {
	unknownTasks := []*peloton.TaskID{
		{Value: "bcabcabc-bcab-bcab-bcab-bcabcabcabca-0"},
		{Value: "abcabcab-bcab-bcab-bcab-bcabcabcabca-1"},
	}

	// generate 25 test tasks
	numTasks := 25
	var tasks []*peloton.TaskID
	var selectedPorts []uint32
	taskInfos := make(map[string]*LaunchableTaskInfo)
	for i := 0; i < numTasks; i++ {
		tmp := createTestTask(i)
		tmp.GetConfig().Volume = &task.PersistentVolumeConfig{
			ContainerPath: "testpath",
			SizeMB:        10,
		}
		taskID := &peloton.TaskID{
			Value: tmp.JobId.Value + "-" + fmt.Sprint(tmp.InstanceId),
		}
		tasks = append(tasks, taskID)
		taskInfos[taskID.Value] = tmp
		selectedPorts = append(selectedPorts, testPort+uint32(i))
	}
	rs := createResources(1)
	hostOffer := createHostOffer(0, rs)

	for i := 0; i < numTasks; i++ {
		taskID := tasks[i].GetValue()
		jobID, instanceID, err := util.ParseTaskID(taskID)
		suite.NoError(err)
		suite.jobFactory.EXPECT().
			GetJob(&peloton.JobID{Value: jobID}).Return(suite.cachedJob)
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(instanceID)).
			Return(suite.cachedTask, nil)
		suite.mockTaskStore.EXPECT().
			GetTaskConfig(gomock.Any(), &peloton.JobID{Value: jobID}, uint32(instanceID), gomock.Any()).
			Return(taskInfos[tasks[i].GetValue()].GetConfig(), &models.ConfigAddOn{}, nil)
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).Return(taskInfos[tasks[i].GetValue()].GetRuntime(), nil).AnyTimes()
	}
	for _, taskID := range unknownTasks {
		jobID, _, err := util.ParseTaskID(taskID.GetValue())
		suite.NoError(err)
		suite.jobFactory.EXPECT().
			GetJob(&peloton.JobID{Value: jobID}).Return(nil)
	}

	tasks = append(tasks, unknownTasks...)
	launchableTasks, skippedTasks, err := suite.taskLauncher.GetLaunchableTasks(
		context.Background(), tasks, hostOffer.Hostname,
		hostOffer.AgentId, selectedPorts)
	suite.NoError(err)
	for _, launchableTask := range launchableTasks {
		runtimeDiff := launchableTask.RuntimeDiff
		suite.Equal(task.TaskState_LAUNCHED, runtimeDiff[jobmgrcommon.StateField])
		suite.Equal(hostOffer.Hostname, runtimeDiff[jobmgrcommon.HostField])
		suite.Equal(hostOffer.AgentId, runtimeDiff[jobmgrcommon.AgentIDField])
		suite.NotNil(runtimeDiff[jobmgrcommon.VolumeIDField], "Volume ID should not be null")
	}
	suite.EqualValues(unknownTasks, skippedTasks)
}

// This test ensures that multiple tasks can be launched in hostmgr
func (suite *LauncherTestSuite) TestMultipleTasksLaunched() {
	// generate 25 test tasks
	numTasks := 25
	var launchableTasks []*hostsvc.LaunchableTask
	taskInfos := make(map[string]*LaunchableTaskInfo)
	taskConfigs := make(map[string]*task.TaskConfig)
	for i := 0; i < numTasks; i++ {
		tmp := createTestTask(i)
		launchableTask := hostsvc.LaunchableTask{
			TaskId: tmp.GetRuntime().GetMesosTaskId(),
			Config: tmp.GetConfig(),
			Ports:  tmp.GetRuntime().GetPorts(),
		}
		launchableTasks = append(launchableTasks, &launchableTask)
		taskID := tmp.JobId.Value + "-" + fmt.Sprint(tmp.InstanceId)
		taskInfos[taskID] = tmp
		taskConfigs[tmp.GetRuntime().GetMesosTaskId().GetValue()] = tmp.Config
	}

	// generate 1 host offer, each can hold many tasks
	rs := createResources(1)
	hostOffer := createHostOffer(0, rs)
	placement := createPlacementMultipleTasks(taskInfos, hostOffer)

	// Capture LaunchTasks calls
	hostsLaunchedOn := make(map[string]bool)
	launchedTasks := make(map[string]*task.TaskConfig)

	gomock.InOrder(
		// Mock LaunchTasks call.
		suite.mockHostMgr.EXPECT().
			LaunchTasks(
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context, reqBody interface{}) {
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
			Return(&hostsvc.LaunchTasksResponse{}, nil).
			Times(1),
	)

	suite.taskLauncher.ProcessPlacement(context.Background(), launchableTasks, placement)

	time.Sleep(1 * time.Second)
	expectedLaunchedHosts := map[string]bool{
		"hostname-0": true,
	}
	lock.Lock()
	defer lock.Unlock()
	suite.Equal(expectedLaunchedHosts, hostsLaunchedOn)
	suite.Equal(taskConfigs, launchedTasks)
}

// This test ensures that tasks got rescheduled when launched got invalid offer resp.
func (suite *LauncherTestSuite) TestLaunchTasksWithInvalidOfferResponse() {
	// generate 1 test task
	numTasks := 1
	var launchableTasks []*hostsvc.LaunchableTask
	taskInfos := make(map[string]*LaunchableTaskInfo)
	taskConfigs := make(map[string]*task.TaskConfig)
	for i := 0; i < numTasks; i++ {
		tmp := createTestTask(i)
		launchableTask := hostsvc.LaunchableTask{
			TaskId: tmp.GetRuntime().GetMesosTaskId(),
			Config: tmp.GetConfig(),
			Ports:  tmp.GetRuntime().GetPorts(),
		}
		launchableTasks = append(launchableTasks, &launchableTask)
		taskID := tmp.JobId.Value + "-" + fmt.Sprint(tmp.InstanceId)
		taskInfos[taskID] = tmp
		taskConfigs[tmp.Runtime.GetMesosTaskId().GetValue()] = tmp.Config
	}

	// generate 1 host offer, each can hold many tasks
	rs := createResources(1)
	hostOffer := createHostOffer(0, rs)
	placement := createPlacementMultipleTasks(taskInfos, hostOffer)

	// Capture LaunchTasks calls
	hostsLaunchedOn := make(map[string]bool)
	launchedTasks := make(map[string]*task.TaskConfig)

	gomock.InOrder(
		// Mock LaunchTasks call.
		suite.mockHostMgr.EXPECT().
			LaunchTasks(
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context, reqBody interface{}) {
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
			Return(&hostsvc.LaunchTasksResponse{
				Error: &hostsvc.LaunchTasksResponse_Error{
					InvalidOffers: &hostsvc.InvalidOffers{
						Message: "invalid offer failure",
					},
				},
			}, nil).
			Times(1),
	)

	err := suite.taskLauncher.ProcessPlacement(context.Background(), launchableTasks, placement)
	suite.Error(err)

	time.Sleep(1 * time.Second)
	expectedLaunchedHosts := map[string]bool{
		"hostname-0": true,
	}
	lock.Lock()
	defer lock.Unlock()
	suite.Equal(expectedLaunchedHosts, hostsLaunchedOn)
	suite.Equal(taskConfigs, launchedTasks)
	suite.Equal(
		int64(0),
		suite.testScope.Snapshot().Counters()["launch_tasks.retry+"].Value())
}

func (suite *LauncherTestSuite) TestLaunchTasksRetryWithError() {
	// generate 1 test task
	numTasks := 1
	var launchableTasks []*hostsvc.LaunchableTask
	taskInfos := make(map[string]*LaunchableTaskInfo)
	taskConfigs := make(map[string]*task.TaskConfig)
	for i := 0; i < numTasks; i++ {
		tmp := createTestTask(i)
		launchableTask := hostsvc.LaunchableTask{
			TaskId: tmp.GetRuntime().GetMesosTaskId(),
			Config: tmp.GetConfig(),
			Ports:  tmp.GetRuntime().GetPorts(),
		}
		launchableTasks = append(launchableTasks, &launchableTask)
		taskID := tmp.JobId.Value + "-" + fmt.Sprint(tmp.InstanceId)
		taskInfos[taskID] = tmp
		taskConfigs[tmp.GetRuntime().GetMesosTaskId().GetValue()] = tmp.Config
	}

	// generate 1 host offer, each can hold many tasks
	rs := createResources(1)
	hostOffer := createHostOffer(0, rs)
	placement := createPlacementMultipleTasks(taskInfos, hostOffer)

	// Capture LaunchTasks calls
	hostsLaunchedOn := make(map[string]bool)
	launchedTasks := make(map[string]*task.TaskConfig)

	gomock.InOrder(
		// Mock LaunchTasks call.
		suite.mockHostMgr.EXPECT().
			LaunchTasks(
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context, reqBody interface{}) {
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
			Return(&hostsvc.LaunchTasksResponse{
				Error: &hostsvc.LaunchTasksResponse_Error{
					InvalidArgument: &hostsvc.InvalidArgument{
						Message: "invalid argument failure",
					},
				},
			}, nil).
			Times(5),

		// Mock ReleaseOffer call.
		suite.mockHostMgr.EXPECT().ReleaseHostOffers(gomock.Any(), gomock.Any()).
			Return(&hostsvc.ReleaseHostOffersResponse{}, nil),
	)

	err := suite.taskLauncher.ProcessPlacement(context.Background(), launchableTasks, placement)
	suite.Error(err)

	time.Sleep(1 * time.Second)
	expectedLaunchedHosts := map[string]bool{
		"hostname-0": true,
	}
	lock.Lock()
	defer lock.Unlock()
	suite.Equal(expectedLaunchedHosts, hostsLaunchedOn)
	suite.Equal(taskConfigs, launchedTasks)
	suite.Equal(
		int64(4),
		suite.testScope.Snapshot().Counters()["launch_tasks.retry+"].Value())
}

func (suite *LauncherTestSuite) TestLaunchStatefulTask() {
	// generate 1 test task
	numTasks := 1
	var launchableTasks []*hostsvc.LaunchableTask
	taskInfos := make(map[string]*LaunchableTaskInfo)
	for i := 0; i < numTasks; i++ {
		tmp := createTestTask(i)
		tmp.GetConfig().Volume = &task.PersistentVolumeConfig{
			ContainerPath: "testpath",
			SizeMB:        10,
		}
		launchableTask := hostsvc.LaunchableTask{
			TaskId: tmp.GetRuntime().GetMesosTaskId(),
			Config: tmp.GetConfig(),
			Ports:  tmp.GetRuntime().GetPorts(),
		}
		launchableTasks = append(launchableTasks, &launchableTask)
		taskID := tmp.JobId.Value + "-" + fmt.Sprint(tmp.InstanceId)
		taskInfos[taskID] = tmp
	}

	// generate 1 host offer, each can hold many tasks
	rs := createResources(1)
	hostOffer := createHostOffer(0, rs)
	placement := createPlacementMultipleTasks(taskInfos, hostOffer)
	placement.Type = resmgr.TaskType_STATEFUL

	// Capture OfferOperation calls
	hostsLaunchedOn := make(map[string]bool)

	volumeInfo := &volume.PersistentVolumeInfo{}
	gomock.InOrder(
		suite.mockVolumeStore.EXPECT().
			GetPersistentVolume(gomock.Any(), gomock.Any()).
			Return(volumeInfo, nil),

		// Mock OfferOperation call.
		suite.mockHostMgr.EXPECT().
			OfferOperations(
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context, reqBody interface{}) {
				// No need to unmarksnal output: empty means success.
				// Capture call since we don't know ordering of tasks.
				lock.Lock()
				defer lock.Unlock()
				req := reqBody.(*hostsvc.OfferOperationsRequest)
				hostsLaunchedOn[req.Hostname] = true
				suite.Equal(3, len(req.Operations))
			}).
			Return(&hostsvc.OfferOperationsResponse{}, nil).
			Times(1),
	)

	suite.taskLauncher.ProcessPlacement(context.Background(), launchableTasks, placement)

	time.Sleep(1 * time.Second)
	expectedLaunchedHosts := map[string]bool{
		"hostname-0": true,
	}
	lock.Lock()
	defer lock.Unlock()
	suite.Equal(expectedLaunchedHosts, hostsLaunchedOn)
}

// TestLaunchStatefulTaskLaunchWithVolume will return persistent volume info to be non-null
func (suite *LauncherTestSuite) TestLaunchStatefulTaskLaunchWithVolume() {
	// generate 1 test task
	numTasks := 1
	var launchableTasks []*hostsvc.LaunchableTask
	taskInfos := make(map[string]*LaunchableTaskInfo)
	taskConfigs := make(map[string]*task.TaskConfig)
	for i := 0; i < numTasks; i++ {
		tmp := createTestTask(i)
		tmp.GetConfig().Volume = &task.PersistentVolumeConfig{
			ContainerPath: "testpath",
			SizeMB:        10,
		}
		launchableTask := hostsvc.LaunchableTask{
			TaskId: tmp.GetRuntime().GetMesosTaskId(),
			Config: tmp.GetConfig(),
			Ports:  tmp.GetRuntime().GetPorts(),
		}
		launchableTasks = append(launchableTasks, &launchableTask)
		taskID := tmp.JobId.Value + "-" + fmt.Sprint(tmp.InstanceId)
		taskInfos[taskID] = tmp
		taskConfigs[tmp.GetRuntime().GetMesosTaskId().GetValue()] = tmp.Config
	}

	// generate 1 host offer, each can hold many tasks
	rs := createResources(1)
	hostOffer := createHostOffer(0, rs)
	placement := createPlacementMultipleTasks(taskInfos, hostOffer)
	placement.Type = resmgr.TaskType_STATEFUL

	// Capture OfferOperation calls
	hostsLaunchedOn := make(map[string]bool)

	volumeInfo := &volume.PersistentVolumeInfo{
		State: volume.VolumeState_CREATED,
	}
	gomock.InOrder(
		suite.mockVolumeStore.EXPECT().
			GetPersistentVolume(gomock.Any(), gomock.Any()).
			Return(volumeInfo, nil),

		// Mock OfferOperation call.
		suite.mockHostMgr.EXPECT().
			OfferOperations(
				gomock.Any(),
				gomock.Any()).
			Do(func(_ context.Context, reqBody interface{}) {
				// No need to unmarksnal output: empty means success.
				// Capture call since we don't know ordering of tasks.
				lock.Lock()
				defer lock.Unlock()
				req := reqBody.(*hostsvc.OfferOperationsRequest)
				hostsLaunchedOn[req.Hostname] = true
				// Since volume info is already present, only 1 operation is requested.
				suite.Equal(1, len(req.Operations))
			}).
			Return(&hostsvc.OfferOperationsResponse{}, nil).
			Times(1),
	)

	suite.taskLauncher.ProcessPlacement(context.Background(), launchableTasks, placement)

	time.Sleep(1 * time.Second)
	expectedLaunchedHosts := map[string]bool{
		"hostname-0": true,
	}
	lock.Lock()
	defer lock.Unlock()
	suite.Equal(expectedLaunchedHosts, hostsLaunchedOn)
}

func (suite *LauncherTestSuite) TestProcessPlacementsWithNoTasksReleasesOffers() {
	// Mock OfferOperation call.
	suite.mockHostMgr.EXPECT().ReleaseHostOffers(gomock.Any(), &hostsvc.ReleaseHostOffersRequest{
		HostOffers: []*hostsvc.HostOffer{{
			Hostname: "hostname-0",
			AgentId:  &mesos.AgentID{},
		}},
	}).
		Return(&hostsvc.ReleaseHostOffersResponse{}, nil)

	suite.taskLauncher.ProcessPlacement(context.Background(), nil, &resmgr.Placement{
		Hostname: "hostname-0",
		AgentId:  &mesos.AgentID{},
	})

	time.Sleep(1 * time.Second)
}

// TestCreateLaunchableTasks tests the CreateLaunchableTasks function
// to make sure that all the tasks in launchableTasks list
// that contain a volume/secret will be populated with
// the actual secret data fetched from the secret store
func (suite *LauncherTestSuite) TestCreateLaunchableTasks() {
	// Expected Secret
	secret := jobmgrtask.CreateSecretProto("", testSecretPath, []byte(testSecretStr))
	mesosContainerizer := mesos.ContainerInfo_MESOS
	secretInfoObject := &objects.SecretInfoObject{
		SecretID:     secret.Id.Value,
		JobID:        _testJobID,
		Version:      0,
		Valid:        true,
		Path:         testSecretPath,
		Data:         base64.StdEncoding.EncodeToString([]byte(testSecretStr)),
		CreationTime: time.Now(),
	}

	// generate 5 test tasks
	numTasks := 5
	taskInfos := make(map[string]*LaunchableTaskInfo)
	var launchableTasks []*hostsvc.LaunchableTask
	for i := 0; i < numTasks; i++ {
		idStr := fmt.Sprintf("secret-id-%d", i)
		tmp := createTestTask(i)
		taskID := &peloton.TaskID{
			Value: tmp.JobId.Value + "-" + fmt.Sprint(tmp.InstanceId),
		}
		// add secrets for even number of tasks, to create a mix
		if i%2 == 0 {
			tmp.GetConfig().Container = &mesos.ContainerInfo{
				Type: &mesosContainerizer,
			}
			tmp.GetConfig().GetContainer().Volumes = []*mesos.Volume{
				util.CreateSecretVolume(testSecretPath, idStr)}
			suite.secretInfoOps.EXPECT().
				GetSecret(gomock.Any(), idStr).
				Return(secretInfoObject, nil)
		}
		taskInfos[taskID.Value] = tmp
	}

	launchableTasks, skippedTaskInfos := suite.taskLauncher.CreateLaunchableTasks(
		context.Background(), taskInfos)

	suite.Equal(len(launchableTasks), numTasks)
	suite.Equal(len(skippedTaskInfos), 0)
	// launchableTasks list should now be updated with actual secret data.
	// Verify if it matches "test-data" for all tasks
	for _, task := range launchableTasks {
		if task.GetConfig().GetContainer().GetVolumes() != nil {
			secretFromTask := task.GetConfig().GetContainer().GetVolumes()[0].
				GetSource().GetSecret().GetValue().GetData()
			suite.Equal(secretFromTask, []byte(testSecretStr))
		}
	}

	// Simulate error in GetSecret() for one task
	// generate 5 test tasks
	taskInfos = make(map[string]*LaunchableTaskInfo)
	for i := 0; i < numTasks; i++ {
		idStr := fmt.Sprintf("bad-secret-id-%d", i)
		tmp := createTestTask(i)
		taskID := &peloton.TaskID{
			Value: tmp.JobId.Value + "-" + fmt.Sprint(tmp.InstanceId),
		}
		tmp.GetConfig().Container = &mesos.ContainerInfo{
			Type: &mesosContainerizer,
		}
		// add secrets for even number of tasks, to mix it up
		// simulate GetSecret failure for these tasks
		if i%2 == 0 {
			tmp.GetConfig().GetContainer().Volumes = []*mesos.Volume{
				util.CreateSecretVolume(testSecretPath, idStr),
			}
			suite.secretInfoOps.EXPECT().
				GetSecret(gomock.Any(), idStr).
				Return(nil, errors.New("get secret error"))
		}
		taskInfos[taskID.Value] = tmp
	}

	launchableTasks, skippedTaskInfos = suite.taskLauncher.CreateLaunchableTasks(
		context.Background(), taskInfos)
	// launchableTasks list should only contain tasks that don't have secrets.
	// GetSecret will fail for tasks that have secrets and the populateSecrets
	// will remove these tasks from the launchableTasks list.
	suite.Equal(len(launchableTasks), 2)
	suite.Equal(len(skippedTaskInfos), 3)

	for _, task := range launchableTasks {
		suite.Nil(task.GetConfig().GetContainer().GetVolumes())
	}

	// test secret not found error; make sure task goalstate is set to killed.
	taskInfos = make(map[string]*LaunchableTaskInfo)
	idStr := fmt.Sprintf("no-secret-id")
	tmp := createTestTask(0)
	taskID := &peloton.TaskID{
		Value: tmp.JobId.Value + "-" + fmt.Sprint(tmp.InstanceId),
	}
	tmp.GetConfig().Container = &mesos.ContainerInfo{
		Type: &mesosContainerizer,
	}
	tmp.GetConfig().GetContainer().Volumes = []*mesos.Volume{
		util.CreateSecretVolume(testSecretPath, idStr),
	}
	suite.jobFactory.EXPECT().GetJob(tmp.JobId).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) {
			suite.Equal(task.TaskState_KILLED, runtimeDiffs[0][jobmgrcommon.GoalStateField])
			suite.Equal("REASON_SECRET_NOT_FOUND", runtimeDiffs[0][jobmgrcommon.ReasonField])
		}).
		Return(nil)
	suite.secretInfoOps.EXPECT().
		GetSecret(gomock.Any(), idStr).
		Return(nil, yarpcerrors.NotFoundErrorf(
			"Cannot find secret wth id %v", idStr))

	taskInfos[taskID.Value] = tmp
	launchableTasks, skippedTaskInfos = suite.taskLauncher.CreateLaunchableTasks(
		context.Background(), taskInfos)
	// launchableTasks list should be empty
	suite.Equal(len(launchableTasks), 0)
	// since the GetSecret error is not retryable, this task will not be part of
	// the skippedTaskInfos
	suite.Equal(len(skippedTaskInfos), 0)

	// simulate error in base64 decoding of secret data.
	// use non-base64 encoded data in the secret
	secretInfoObject.Data = "testSecretStr"
	suite.secretInfoOps.EXPECT().
		GetSecret(gomock.Any(), idStr).
		Return(secretInfoObject, nil)
	launchableTasks, skippedTaskInfos = suite.taskLauncher.CreateLaunchableTasks(
		context.Background(), taskInfos)
	suite.Equal(len(launchableTasks), 0)
	// this task is skipped because of the base64 decode error
	suite.Equal(len(skippedTaskInfos), 1)
}

// TestPopulateExecutorData tests populateExecutorData function to properly
// fill out executor data in the launchable task, with the placement info
// passed in.
func (suite *LauncherTestSuite) TestPopulateExecutorData() {
	taskID := "067687c5-2461-475f-b006-68e717f0493b-3-1"
	agentID := "ca6bd27e-9abb-4a2e-9860-0a2c2a942510-S0"
	executorType := mesos.ExecutorInfo_CUSTOM
	launchableTask := &hostsvc.LaunchableTask{
		Config: &task.TaskConfig{
			Executor: &mesos.ExecutorInfo{
				Type: &executorType,
				Data: new(LauncherTestSuite).getTaskConfigData(suite.T()),
			},
		},
		TaskId: &mesos.TaskID{
			Value: &taskID,
		},
		Ports: map[string]uint32{"test": 12345},
	}
	placement := &resmgr.Placement{
		AgentId: &mesos.AgentID{
			Value: &agentID,
		},
		Hostname: "192.168.33.7",
	}

	err := populateExecutorData(launchableTask, placement)
	suite.NoError(err)
}

// TestGenerateAssignedTask tests generateAssignedTask function to verify
// it can serialize/deserialize thrift objects correctly.
func (suite *LauncherTestSuite) TestGenerateAssignedTask() {
	taskConfigData := new(LauncherTestSuite).getTaskConfigData(suite.T())
	assignment := assignmentInfo{
		taskID:        "067687c5-2461-475f-b006-68e717f0493b",
		slaveID:       "ca6bd27e-9abb-4a2e-9860-0a2c2a942510-S0",
		slaveHost:     "192.168.33.7",
		assignedPorts: map[string]int32{"test": 12345},
		instanceID:    3,
	}
	assignedTask, err := generateAssignedTask(taskConfigData, assignment)
	suite.NoError(err)
	// Since the ordering of the binary serialized data is slightly different
	// between thriftrw (used in generateAssignedTask) and official thrift
	// binding (used in the sample data), only compare the data length here.
	suite.Equal(len(getAssignedTaskData(suite.T())), len(assignedTask))
}

// TestGenerateAssignedTask tests various errors generateAssignedTask
// might return.
func (suite *LauncherTestSuite) TestGenerateAssignedTaskError() {
	assignment := assignmentInfo{}

	// Failed to decode binary data to wire model
	taskConfigData := []byte{}
	_, err := generateAssignedTask(taskConfigData, assignment)
	suite.Error(err)
}

// createPlacementMultipleTasks creates the placement with multiple tasks
func createPlacementMultipleTasks(tasks map[string]*LaunchableTaskInfo, hostOffer *hostsvc.HostOffer) *resmgr.Placement {
	var TasksIds []*peloton.TaskID

	for id := range tasks {
		TasksIds = append(TasksIds, &peloton.TaskID{Value: id})
	}
	placement := &resmgr.Placement{
		AgentId:  hostOffer.AgentId,
		Hostname: hostOffer.Hostname,
		Tasks:    TasksIds,
		Ports:    []uint32{testPort},
	}
	return placement
}

// createPlacements creates the placement
func createPlacements(t *LaunchableTaskInfo,
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
