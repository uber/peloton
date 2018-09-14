package launcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	"go.uber.org/yarpc/yarpcerrors"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/volume"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common/backoff"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	jobmgrtask "code.uber.internal/infra/peloton/jobmgr/task"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"
)

const (
	taskIDFmt      = _testJobID + "-%d-%s"
	testPort       = uint32(100)
	testSecretPath = "/tmp/secret"
	testSecretStr  = "test-data"
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

func createTestTask(instanceID int) *task.TaskInfo {
	var tid = fmt.Sprintf(taskIDFmt, instanceID, uuid.NewUUID().String())

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

func TestGetLaunchableTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)
	taskLauncher := launcher{
		hostMgrClient: mockHostMgr,
		jobFactory:    jobFactory,
		taskStore:     mockTaskStore,
		metrics:       metrics,
		retryPolicy:   backoff.NewRetryPolicy(5, 15*time.Millisecond),
	}

	// generate 25 test tasks
	numTasks := 25
	var tasks []*peloton.TaskID
	var selectedPorts []uint32
	taskInfos := make(map[string]*task.TaskInfo)
	for i := 0; i < numTasks; i++ {
		tmp := createTestTask(i)
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
		assert.NoError(t, err)
		jobFactory.EXPECT().
			GetJob(&peloton.JobID{Value: jobID}).Return(cachedJob)
		cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(instanceID)).
			Return(cachedTask, nil)
		mockTaskStore.EXPECT().
			GetTaskConfig(gomock.Any(), &peloton.JobID{Value: jobID}, uint32(instanceID), gomock.Any()).
			Return(taskInfos[tasks[i].GetValue()].GetConfig(), nil)
		cachedTask.EXPECT().
			GetRunTime(gomock.Any()).Return(taskInfos[tasks[i].GetValue()].GetRuntime(), nil).AnyTimes()
	}

	launchableTasks, err := taskLauncher.GetLaunchableTasks(
		context.Background(), tasks, hostOffer.Hostname,
		hostOffer.AgentId, selectedPorts)
	assert.NoError(t, err)
	for _, launchableTask := range launchableTasks {
		runtimeDiff := launchableTask.RuntimeDiff
		assert.Equal(t, task.TaskState_LAUNCHED, runtimeDiff[jobmgrcommon.StateField])
		assert.Equal(t, hostOffer.Hostname, runtimeDiff[jobmgrcommon.HostField])
		assert.Equal(t, hostOffer.AgentId, runtimeDiff[jobmgrcommon.AgentIDField])
	}
}

func TestGetLaunchableTasksStateful(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)
	taskLauncher := launcher{
		hostMgrClient: mockHostMgr,
		jobFactory:    jobFactory,
		taskStore:     mockTaskStore,
		metrics:       metrics,
		retryPolicy:   backoff.NewRetryPolicy(5, 15*time.Millisecond),
	}

	// generate 25 test tasks
	numTasks := 25
	var tasks []*peloton.TaskID
	var selectedPorts []uint32
	taskInfos := make(map[string]*task.TaskInfo)
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
		assert.NoError(t, err)
		jobFactory.EXPECT().
			GetJob(&peloton.JobID{Value: jobID}).Return(cachedJob)
		cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(instanceID)).
			Return(cachedTask, nil)
		mockTaskStore.EXPECT().
			GetTaskConfig(gomock.Any(), &peloton.JobID{Value: jobID}, uint32(instanceID), gomock.Any()).
			Return(taskInfos[tasks[i].GetValue()].GetConfig(), nil)
		cachedTask.EXPECT().
			GetRunTime(gomock.Any()).Return(taskInfos[tasks[i].GetValue()].GetRuntime(), nil).AnyTimes()
	}

	launchableTasks, err := taskLauncher.GetLaunchableTasks(
		context.Background(), tasks, hostOffer.Hostname,
		hostOffer.AgentId, selectedPorts)
	assert.NoError(t, err)
	for _, launchableTask := range launchableTasks {
		runtimeDiff := launchableTask.RuntimeDiff
		assert.Equal(t, task.TaskState_LAUNCHED, runtimeDiff[jobmgrcommon.StateField])
		assert.Equal(t, hostOffer.Hostname, runtimeDiff[jobmgrcommon.HostField])
		assert.Equal(t, hostOffer.AgentId, runtimeDiff[jobmgrcommon.AgentIDField])
		assert.NotNil(t, runtimeDiff[jobmgrcommon.VolumeIDField], "Volume ID should not be null")
	}
}

// This test ensures that multiple tasks can be launched in hostmgr
func TestMultipleTasksLaunched(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)
	taskLauncher := launcher{
		hostMgrClient: mockHostMgr,
		taskStore:     mockTaskStore,
		metrics:       metrics,
		retryPolicy:   backoff.NewRetryPolicy(5, 15*time.Millisecond),
	}

	// generate 25 test tasks
	numTasks := 25
	var launchableTasks []*hostsvc.LaunchableTask
	taskInfos := make(map[string]*task.TaskInfo)
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
		mockHostMgr.EXPECT().
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

	taskLauncher.ProcessPlacement(context.Background(), launchableTasks, placement)

	time.Sleep(1 * time.Second)
	expectedLaunchedHosts := map[string]bool{
		"hostname-0": true,
	}
	lock.Lock()
	defer lock.Unlock()
	assert.Equal(t, expectedLaunchedHosts, hostsLaunchedOn)
	assert.Equal(t, taskConfigs, launchedTasks)
}

// This test ensures that tasks got rescheduled when launched got invalid offer resp.
func TestLaunchTasksWithInvalidOfferResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)
	taskLauncher := launcher{
		hostMgrClient: mockHostMgr,
		taskStore:     mockTaskStore,
		metrics:       metrics,
		retryPolicy:   backoff.NewRetryPolicy(5, 15*time.Millisecond),
	}

	// generate 1 test task
	numTasks := 1
	var launchableTasks []*hostsvc.LaunchableTask
	taskInfos := make(map[string]*task.TaskInfo)
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
		mockHostMgr.EXPECT().
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

	err := taskLauncher.ProcessPlacement(context.Background(), launchableTasks, placement)
	assert.Error(t, err)

	time.Sleep(1 * time.Second)
	expectedLaunchedHosts := map[string]bool{
		"hostname-0": true,
	}
	lock.Lock()
	defer lock.Unlock()
	assert.Equal(t, expectedLaunchedHosts, hostsLaunchedOn)
	assert.Equal(t, taskConfigs, launchedTasks)
	assert.Equal(
		t,
		int64(0),
		testScope.Snapshot().Counters()["launch_tasks.retry+"].Value())
}

func TestLaunchTasksRetryWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)
	taskLauncher := launcher{
		hostMgrClient: mockHostMgr,
		taskStore:     mockTaskStore,
		metrics:       metrics,
		retryPolicy:   backoff.NewRetryPolicy(5, 15*time.Millisecond),
	}

	// generate 1 test task
	numTasks := 1
	var launchableTasks []*hostsvc.LaunchableTask
	taskInfos := make(map[string]*task.TaskInfo)
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
		mockHostMgr.EXPECT().
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
		mockHostMgr.EXPECT().ReleaseHostOffers(gomock.Any(), gomock.Any()).
			Return(&hostsvc.ReleaseHostOffersResponse{}, nil),
	)

	err := taskLauncher.ProcessPlacement(context.Background(), launchableTasks, placement)
	assert.Error(t, err)

	time.Sleep(1 * time.Second)
	expectedLaunchedHosts := map[string]bool{
		"hostname-0": true,
	}
	lock.Lock()
	defer lock.Unlock()
	assert.Equal(t, expectedLaunchedHosts, hostsLaunchedOn)
	assert.Equal(t, taskConfigs, launchedTasks)
	assert.Equal(
		t,
		int64(4),
		testScope.Snapshot().Counters()["launch_tasks.retry+"].Value())
}

func TestLaunchStatefulTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	mockVolumeStore := store_mocks.NewMockPersistentVolumeStore(ctrl)

	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)
	taskLauncher := launcher{
		hostMgrClient: mockHostMgr,
		taskStore:     mockTaskStore,
		volumeStore:   mockVolumeStore,
		metrics:       metrics,
		retryPolicy:   backoff.NewRetryPolicy(5, 15*time.Millisecond),
	}

	// generate 1 test task
	numTasks := 1
	var launchableTasks []*hostsvc.LaunchableTask
	taskInfos := make(map[string]*task.TaskInfo)
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
		mockVolumeStore.EXPECT().
			GetPersistentVolume(gomock.Any(), gomock.Any()).
			Return(volumeInfo, nil),

		// Mock OfferOperation call.
		mockHostMgr.EXPECT().
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
				assert.Equal(t, 3, len(req.Operations))
			}).
			Return(&hostsvc.OfferOperationsResponse{}, nil).
			Times(1),
	)

	taskLauncher.ProcessPlacement(context.Background(), launchableTasks, placement)

	time.Sleep(1 * time.Second)
	expectedLaunchedHosts := map[string]bool{
		"hostname-0": true,
	}
	lock.Lock()
	defer lock.Unlock()
	assert.Equal(t, expectedLaunchedHosts, hostsLaunchedOn)
}

// TestLaunchStatefulTaskLaunchWithVolume will return persistent volume info to be non-null
func TestLaunchStatefulTaskLaunchWithVolume(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	mockVolumeStore := store_mocks.NewMockPersistentVolumeStore(ctrl)

	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)
	taskLauncher := launcher{
		hostMgrClient: mockHostMgr,
		taskStore:     mockTaskStore,
		volumeStore:   mockVolumeStore,
		metrics:       metrics,
		retryPolicy:   backoff.NewRetryPolicy(5, 15*time.Millisecond),
	}

	// generate 1 test task
	numTasks := 1
	var launchableTasks []*hostsvc.LaunchableTask
	taskInfos := make(map[string]*task.TaskInfo)
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
		mockVolumeStore.EXPECT().
			GetPersistentVolume(gomock.Any(), gomock.Any()).
			Return(volumeInfo, nil),

		// Mock OfferOperation call.
		mockHostMgr.EXPECT().
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
				assert.Equal(t, 1, len(req.Operations))
			}).
			Return(&hostsvc.OfferOperationsResponse{}, nil).
			Times(1),
	)

	taskLauncher.ProcessPlacement(context.Background(), launchableTasks, placement)

	time.Sleep(1 * time.Second)
	expectedLaunchedHosts := map[string]bool{
		"hostname-0": true,
	}
	lock.Lock()
	defer lock.Unlock()
	assert.Equal(t, expectedLaunchedHosts, hostsLaunchedOn)
}

func TestProcessPlacementsWithNoTasksReleasesOffers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	taskLauncher := launcher{
		hostMgrClient: mockHostMgr,
		metrics:       NewMetrics(tally.NoopScope),
		retryPolicy:   backoff.NewRetryPolicy(5, 15*time.Millisecond),
	}

	// Mock OfferOperation call.
	mockHostMgr.EXPECT().ReleaseHostOffers(gomock.Any(), &hostsvc.ReleaseHostOffersRequest{
		HostOffers: []*hostsvc.HostOffer{{
			Hostname: "hostname-0",
			AgentId:  &mesos.AgentID{},
		}},
	}).
		Return(&hostsvc.ReleaseHostOffersResponse{}, nil)

	taskLauncher.ProcessPlacement(context.Background(), nil, &resmgr.Placement{
		Hostname: "hostname-0",
		AgentId:  &mesos.AgentID{},
	})

	time.Sleep(1 * time.Second)
}

// TestCreateLaunchableTasks tests the CreateLaunchableTasks function
// to make sure that all the tasks in launchableTasks list
// that contain a volume/secret will be populated with
// the actual secret data fetched from the secret store
func TestCreateLaunchableTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	mockSecretStore := store_mocks.NewMockSecretStore(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	taskLauncher := launcher{
		secretStore: mockSecretStore,
		jobFactory:  jobFactory,
		taskStore:   mockTaskStore,
		metrics:     NewMetrics(tally.NoopScope),
	}

	// Expected Secret
	secret := jobmgrtask.CreateSecretProto("", testSecretPath, []byte(testSecretStr))
	mesosContainerizer := mesos.ContainerInfo_MESOS

	// generate 5 test tasks
	numTasks := 5
	taskInfos := make(map[string]*task.TaskInfo)
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
			mockSecretStore.EXPECT().
				GetSecret(gomock.Any(), &peloton.SecretID{Value: idStr}).
				Return(secret, nil)
		}
		taskInfos[taskID.Value] = tmp
	}

	launchableTasks, skippedTaskInfos := taskLauncher.CreateLaunchableTasks(
		context.Background(), taskInfos)

	assert.Equal(t, len(launchableTasks), numTasks)
	assert.Equal(t, len(skippedTaskInfos), 0)
	// launchableTasks list should now be updated with actual secret data.
	// Verify if it matches "test-data" for all tasks
	for _, task := range launchableTasks {
		if task.GetConfig().GetContainer().GetVolumes() != nil {
			secretFromTask := task.GetConfig().GetContainer().GetVolumes()[0].
				GetSource().GetSecret().GetValue().GetData()
			assert.Equal(t, secretFromTask, []byte(testSecretStr))
		}
	}

	// Simulate error in GetSecret() for one task
	// generate 5 test tasks
	taskInfos = make(map[string]*task.TaskInfo)
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
			mockSecretStore.EXPECT().
				GetSecret(gomock.Any(), &peloton.SecretID{Value: idStr}).
				Return(nil, errors.New("get secret error"))
		}
		taskInfos[taskID.Value] = tmp
	}

	launchableTasks, skippedTaskInfos = taskLauncher.CreateLaunchableTasks(
		context.Background(), taskInfos)
	// launchableTasks list should only contain tasks that don't have secrets.
	// GetSecret will fail for tasks that have secrets and the populateSecrets
	// will remove these tasks from the launchableTasks list.
	assert.Equal(t, len(launchableTasks), 2)
	assert.Equal(t, len(skippedTaskInfos), 3)

	for _, task := range launchableTasks {
		assert.Nil(t, task.GetConfig().GetContainer().GetVolumes())
	}

	// test secret not found error. make sure, task goalstate is set to killed
	taskInfos = make(map[string]*task.TaskInfo)
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
	jobFactory.EXPECT().GetJob(tmp.JobId).Return(cachedJob)
	cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) {
			assert.Equal(t, task.TaskState_KILLED, runtimeDiffs[0][jobmgrcommon.GoalStateField])
			assert.Equal(t, "REASON_SECRET_NOT_FOUND", runtimeDiffs[0][jobmgrcommon.ReasonField])
		}).
		Return(nil)
	mockSecretStore.EXPECT().
		GetSecret(gomock.Any(), &peloton.SecretID{Value: idStr}).
		Return(nil, yarpcerrors.NotFoundErrorf(
			"Cannot find secret wth id %v", idStr))

	taskInfos[taskID.Value] = tmp
	launchableTasks, skippedTaskInfos = taskLauncher.CreateLaunchableTasks(
		context.Background(), taskInfos)
	// launchableTasks list should be empty
	assert.Equal(t, len(launchableTasks), 0)
	// since the GetSecret error is not retryable, this task will not be part of
	// the skippedTaskInfos
	assert.Equal(t, len(skippedTaskInfos), 0)

	// simulate error in base64 decoding of secret data.
	// use non-base64 encoded data in the secret
	secret.GetValue().Data = []byte(testSecretStr)
	mockSecretStore.EXPECT().
		GetSecret(gomock.Any(), &peloton.SecretID{Value: idStr}).
		Return(secret, nil)
	launchableTasks, skippedTaskInfos = taskLauncher.CreateLaunchableTasks(
		context.Background(), taskInfos)
	assert.Equal(t, len(launchableTasks), 0)
	// this task is skipped because of the base64 decode error
	assert.Equal(t, len(skippedTaskInfos), 1)
}

// createPlacementMultipleTasks creates the placement with multiple tasks
func createPlacementMultipleTasks(tasks map[string]*task.TaskInfo, hostOffer *hostsvc.HostOffer) *resmgr.Placement {
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
