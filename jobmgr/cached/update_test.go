package cached

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	mesosv1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	"code.uber.internal/infra/peloton/common/taskconfig"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type UpdateTestSuite struct {
	suite.Suite

	ctrl        *gomock.Controller
	jobStore    *storemocks.MockJobStore
	taskStore   *storemocks.MockTaskStore
	updateStore *storemocks.MockUpdateStore
	jobID       *peloton.JobID
	updateID    *peloton.UpdateID
	update      *update
}

func TestUpdate(t *testing.T) {
	suite.Run(t, new(UpdateTestSuite))
}

func (suite *UpdateTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.updateStore = storemocks.NewMockUpdateStore(suite.ctrl)
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.update = initializeUpdate(
		suite.jobStore,
		suite.taskStore,
		suite.updateStore,
		suite.jobID,
		suite.updateID,
	)
}

func (suite *UpdateTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// initializeUpdate initializes a job update in the suite
func initializeUpdate(
	jobStore *storemocks.MockJobStore,
	taskStore *storemocks.MockTaskStore,
	updateStore *storemocks.MockUpdateStore,
	jobID *peloton.JobID,
	updateID *peloton.UpdateID) *update {
	jobFactory := &jobFactory{
		mtx:       NewMetrics(tally.NoopScope),
		jobStore:  jobStore,
		taskStore: taskStore,
		jobs:      map[string]*job{},
		running:   true,
	}

	updateFactory := &updateFactory{
		mtx:         NewMetrics(tally.NoopScope),
		jobStore:    jobStore,
		taskStore:   taskStore,
		updateStore: updateStore,
		updates:     map[string]*update{},
	}

	u := newUpdate(updateID, jobFactory, updateFactory)
	u.updateFactory.updates[u.id.GetValue()] = u
	return u
}

// initializeTaskConfig initializes a task configuration
func initializeTaskConfig() *pbtask.TaskConfig {
	containerType := mesosv1.ContainerInfo_MESOS
	imageType := mesosv1.Image_DOCKER
	imageName := "image-1"
	commandShell := true
	commandValue := "entrypoint.sh"
	return &pbtask.TaskConfig{
		Name: "0",
		Resource: &pbtask.ResourceConfig{
			CpuLimit:   1.0,
			MemLimitMb: 2.0,
			GpuLimit:   3.0,
		},
		Container: &mesosv1.ContainerInfo{
			Type: &containerType,
			Mesos: &mesosv1.ContainerInfo_MesosInfo{
				Image: &mesosv1.Image{
					Type: &imageType,
					Docker: &mesosv1.Image_Docker{
						Name: &imageName,
					},
				},
			},
		},
		Command: &mesosv1.CommandInfo{
			Shell:     &commandShell,
			Value:     &commandValue,
			Arguments: []string{"a", "b"},
		},
		HealthCheck: &pbtask.HealthCheckConfig{
			Enabled: false,
		},
		Constraint: &pbtask.Constraint{
			Type: pbtask.Constraint_LABEL_CONSTRAINT,
			LabelConstraint: &pbtask.LabelConstraint{
				Kind:      pbtask.LabelConstraint_HOST,
				Condition: pbtask.LabelConstraint_CONDITION_EQUAL,
			},
		},
		RestartPolicy: &pbtask.RestartPolicy{
			MaxFailures: 3,
		},
		Volume: &pbtask.PersistentVolumeConfig{
			ContainerPath: "/mnt/a",
			SizeMB:        10,
		},
		PreemptionPolicy: &pbtask.PreemptionPolicy{
			KillOnPreempt: false,
		},
		Controller: false,
	}
}

// initializeJobConfig initializes the job configuration
func initializeJobConfig(
	instanceCount uint32,
	version uint64) *pbjob.JobConfig {
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: version,
		},
		Name:        "job1",
		Type:        pbjob.JobType_SERVICE,
		OwningTeam:  "team1",
		LdapGroups:  []string{"Peloton", "Infratructure"},
		Description: "sample job",
		Labels: []*peloton.Label{
			{Key: "group", Value: "label1"},
			{Key: "which", Value: "label2"},
			{Key: "why", Value: "label3"},
		},
		InstanceCount: instanceCount,
		SLA: &pbjob.SlaConfig{
			Priority:    3,
			Preemptible: true,
		},
		RespoolID: &peloton.ResourcePoolID{Value: "abc"},
	}
	jobConfig.DefaultConfig = initializeTaskConfig()
	return jobConfig
}

// initializeUpdateConfig initializes the job update configuration
func initializeUpdateConfig(batchSize uint32) *pbupdate.UpdateConfig {
	return &pbupdate.UpdateConfig{
		BatchSize: batchSize,
	}
}

// initializeJobRuntime initializes a job runtime
func initializeJobRuntime(version uint64) *pbjob.RuntimeInfo {
	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_SUCCEEDED,
		ConfigurationVersion: 1,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   version,
		},
	}
	return jobRuntime
}

// initializeUpdateTest initializes all job/update configurations and runtime
// needed to run a job update unit test.
func initializeUpdateTest(
	instanceCount uint32,
	version uint64,
	newInstanceCount uint32,
	newVersion uint64) (
	prevJobConfig *pbjob.JobConfig,
	newJobConfig *pbjob.JobConfig,
	updateConfig *pbupdate.UpdateConfig,
	jobRuntime *pbjob.RuntimeInfo) {
	batchSize := uint32(2)
	newCommand := "entrypoint2.sh"

	prevJobConfig = initializeJobConfig(instanceCount, version)
	newJobConfig = initializeJobConfig(newInstanceCount, newVersion)
	newJobConfig.DefaultConfig.Command.Value = &newCommand
	updateConfig = initializeUpdateConfig(batchSize)
	jobRuntime = initializeJobRuntime(version)
	return
}

// TestUpdateFetchID tests fetching update and job ID.
func (suite *UpdateTestSuite) TestUpdateFetchID() {
	suite.Equal(suite.updateID, suite.update.ID())
}

// TestValidCreateUpdate tests creating a valid job update with changes to the
// default configuration and the instance configuration,
// as well as adding one new instance.
func (suite *UpdateTestSuite) TestValidCreateUpdate() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, jobRuntime :=
		initializeUpdateTest(
			instanceCount, version, instanceCount+1, version+1)

	taskConfig0 := initializeTaskConfig()
	taskCommand0 := "run.sh"
	taskConfig0.Command.Value = &taskCommand0
	prevJobConfig.InstanceConfig = make(map[uint32]*pbtask.TaskConfig)
	prevJobConfig.InstanceConfig[uint32(0)] = taskConfig0
	prevJobConfig.InstanceConfig[uint32(2)] = taskConfig0
	prevJobConfig.InstanceConfig[uint32(3)] = taskConfig0

	taskConfig1 := initializeTaskConfig()
	taskCommand1 := "run2.sh"
	taskConfig1.Command.Value = &taskCommand1
	newJobConfig.InstanceConfig = make(map[uint32]*pbtask.TaskConfig)
	newJobConfig.InstanceConfig[uint32(0)] = taskConfig1
	newJobConfig.InstanceConfig[uint32(1)] = taskConfig1
	newJobConfig.InstanceConfig[uint32(3)] = taskConfig0

	for i := uint32(0); i < instanceCount; i++ {
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).
			Return(&pbtask.RuntimeInfo{
				ConfigVersion: version,
			}, nil)

		suite.taskStore.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, version).
			Return(taskconfig.Merge(
				prevJobConfig.DefaultConfig,
				prevJobConfig.InstanceConfig[i],
			), nil)
	}

	suite.updateStore.EXPECT().
		CreateUpdate(
			gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateModel *models.UpdateModel) {
			suite.Equal(suite.updateID, updateModel.UpdateID)
			suite.Equal(suite.jobID, updateModel.JobID)
			suite.Equal(updateConfig, updateModel.UpdateConfig)
			suite.Equal(version+1, updateModel.JobConfigVersion)
			suite.Equal(version, updateModel.PrevJobConfigVersion)
			suite.Equal(pbupdate.State_INITIALIZED, updateModel.State)
			suite.Equal(instanceCount, updateModel.InstancesTotal)
		}).
		Return(nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(prevJobConfig, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(jobRuntime, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, newJobConfig).
		Return(nil)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(
			_ context.Context,
			_ *peloton.JobID,
			runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.UpdateID, suite.updateID)
			suite.Equal(runtime.ConfigurationVersion, version+1)
		}).
		Return(nil)

	err := suite.update.Create(
		context.Background(),
		suite.jobID,
		newJobConfig,
		prevJobConfig,
		updateConfig,
	)

	suite.NoError(err)
	suite.Equal(suite.jobID, suite.update.JobID())
	suite.Equal(pbupdate.State_INITIALIZED, suite.update.state)
	suite.Equal(updateConfig.BatchSize, suite.update.updateConfig.BatchSize)
	suite.Equal(version, suite.update.jobPrevVersion)
	suite.Equal(version+1, suite.update.jobVersion)
	suite.Equal(instanceCount, uint32(len(suite.update.instancesTotal)))
	suite.Equal(1, len(suite.update.instancesAdded))
	suite.Equal(instanceCount-1, uint32(len(suite.update.instancesUpdated)))
}

// TestCreateUpdateWithLabelsChange tests creating a valid job update with
// changes only to the job labels.
func (suite *UpdateTestSuite) TestCreateUpdateWithLabelsChange() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, jobRuntime :=
		initializeUpdateTest(
			instanceCount, version, instanceCount, version+1)

	// ensure that only labels have changed
	newJobConfig.DefaultConfig.Command.Value =
		prevJobConfig.DefaultConfig.Command.Value
	newJobConfig.Labels = []*peloton.Label{
		{Key: "group", Value: "label1"},
		{Key: "which", Value: "label6"},
		{Key: "why", Value: "label3"},
	}

	suite.updateStore.EXPECT().
		CreateUpdate(
			gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateModel *models.UpdateModel) {
			suite.Equal(suite.updateID, updateModel.UpdateID)
			suite.Equal(suite.jobID, updateModel.JobID)
			suite.Equal(updateConfig, updateModel.UpdateConfig)
			suite.Equal(version+1, updateModel.JobConfigVersion)
			suite.Equal(version, updateModel.PrevJobConfigVersion)
			suite.Equal(pbupdate.State_INITIALIZED, updateModel.State)
			suite.Equal(instanceCount, updateModel.InstancesTotal)
		}).
		Return(nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(prevJobConfig, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(jobRuntime, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, newJobConfig).
		Return(nil)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.UpdateID, suite.updateID)
			suite.Equal(runtime.ConfigurationVersion, version+1)
		}).
		Return(nil)

	err := suite.update.Create(
		context.Background(),
		suite.jobID,
		newJobConfig,
		prevJobConfig,
		updateConfig,
	)

	suite.NoError(err)
	suite.Equal(suite.jobID, suite.update.JobID())
	suite.Equal(pbupdate.State_INITIALIZED, suite.update.state)
	suite.Equal(updateConfig.BatchSize, suite.update.updateConfig.BatchSize)
	suite.Equal(version, suite.update.jobPrevVersion)
	suite.Equal(version+1, suite.update.jobVersion)
	suite.Equal(instanceCount, uint32(len(suite.update.instancesTotal)))
	suite.Equal(0, len(suite.update.instancesAdded))
	suite.Equal(instanceCount, uint32(len(suite.update.instancesUpdated)))
}

// TestCreateUpdateTaskRuntimeGetFail tests creating a valid job update
// but fetching the task runtime from DB fails
func (suite *UpdateTestSuite) TestCreateUpdateTaskRuntimeGetFail() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, _ :=
		initializeUpdateTest(
			instanceCount, version, instanceCount, version+1)

	for i := uint32(0); i < instanceCount-1; i++ {
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).
			Return(&pbtask.RuntimeInfo{
				ConfigVersion: version,
			}, nil)

		suite.taskStore.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, version).
			Return(taskconfig.Merge(
				prevJobConfig.DefaultConfig,
				prevJobConfig.InstanceConfig[i],
			), nil)
	}

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instanceCount-1).
		Return(nil, fmt.Errorf("fake db error"))

	err := suite.update.Create(
		context.Background(),
		suite.jobID,
		newJobConfig,
		prevJobConfig,
		updateConfig,
	)
	suite.EqualError(err, "fake db error")
}

// TestCreateUpdateTaskConfigGetFail tests creating a valid job update
// but fetching the task config from DB fails
func (suite *UpdateTestSuite) TestCreateUpdateTaskConfigGetFail() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, _ :=
		initializeUpdateTest(
			instanceCount, version, instanceCount, version+1)

	for i := uint32(0); i < instanceCount-1; i++ {
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).
			Return(&pbtask.RuntimeInfo{
				ConfigVersion: version,
			}, nil)

		suite.taskStore.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, version).
			Return(taskconfig.Merge(
				prevJobConfig.DefaultConfig,
				prevJobConfig.InstanceConfig[i],
			), nil)
	}

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instanceCount-1).
		Return(&pbtask.RuntimeInfo{
			ConfigVersion: version,
		}, nil)

	suite.taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), suite.jobID, instanceCount-1, version).
		Return(nil, fmt.Errorf("fake db error"))

	err := suite.update.Create(
		context.Background(),
		suite.jobID,
		newJobConfig,
		prevJobConfig,
		updateConfig,
	)
	suite.EqualError(err, "fake db error")
}

// TestValidCreateUpdate tests failing to persist the job update in database
func (suite *UpdateTestSuite) TestCreateUpdateDBError() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, _ :=
		initializeUpdateTest(
			instanceCount, version, instanceCount, version+1)

	for i := uint32(0); i < instanceCount; i++ {
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).
			Return(&pbtask.RuntimeInfo{
				ConfigVersion: version,
			}, nil)

		suite.taskStore.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, version).
			Return(taskconfig.Merge(
				prevJobConfig.DefaultConfig,
				prevJobConfig.InstanceConfig[i],
			), nil)
	}

	suite.updateStore.EXPECT().
		CreateUpdate(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := suite.update.Create(
		context.Background(),
		suite.jobID,
		newJobConfig,
		prevJobConfig,
		updateConfig,
	)
	suite.EqualError(err, "fake db error")
}

// TestValidCreateUpdate tests failing to persist the new job
// configuration in database
func (suite *UpdateTestSuite) TestCreateUpdateJobConfigDBError() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, jobRuntime :=
		initializeUpdateTest(
			instanceCount, version, instanceCount, version+1)

	for i := uint32(0); i < instanceCount; i++ {
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).
			Return(&pbtask.RuntimeInfo{
				ConfigVersion: version,
			}, nil)

		suite.taskStore.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, version).
			Return(taskconfig.Merge(
				prevJobConfig.DefaultConfig,
				prevJobConfig.InstanceConfig[i],
			), nil)
	}

	suite.updateStore.EXPECT().
		CreateUpdate(gomock.Any(), gomock.Any()).
		Return(nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(prevJobConfig, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(jobRuntime, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, newJobConfig).
		Return(fmt.Errorf("fake db error"))

	err := suite.update.Create(
		context.Background(),
		suite.jobID,
		newJobConfig,
		prevJobConfig,
		updateConfig,
	)
	suite.EqualError(err, "fake db error")
}

// TestValidWriteProgress tests successfully writing the status
// of an update into the DB.
func (suite *UpdateTestSuite) TestValidWriteProgress() {
	state := pbupdate.State_ROLLING_FORWARD
	instancesDone := []uint32{0, 1, 2, 3}
	instancesCurrent := []uint32{4, 5}

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateModel *models.UpdateModel) {
			suite.Equal(suite.updateID, updateModel.UpdateID)
			suite.Equal(state, updateModel.State)
			suite.Equal(uint32(len(instancesDone)), updateModel.InstancesDone)
			suite.Equal(instancesCurrent, updateModel.InstancesCurrent)
		}).
		Return(nil)

	err := suite.update.WriteProgress(
		context.Background(),
		state,
		instancesDone,
		instancesCurrent,
	)

	suite.NoError(err)
	suite.Equal(state, suite.update.state)
	suite.Equal(instancesCurrent, suite.update.instancesCurrent)
	suite.Equal(instancesDone, suite.update.instancesDone)
}

// TestValidWriteProgress tests failing to persist the status
// of an update into the DB.
func (suite *UpdateTestSuite) TestWriteProgressDBError() {
	state := pbupdate.State_ROLLING_FORWARD
	instancesDone := []uint32{0, 1, 2, 3}
	instancesCurrent := []uint32{4, 5}

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := suite.update.WriteProgress(
		context.Background(),
		state,
		instancesDone,
		instancesCurrent,
	)
	suite.EqualError(err, "fake db error")
}

// TestCancelValid tests successfully canceling a job update
func (suite *UpdateTestSuite) TestCancelValid() {
	instancesDone := []uint32{1, 2, 3, 4, 5}
	instancesCurrent := []uint32{6, 7}
	suite.update.state = pbupdate.State_INITIALIZED
	suite.update.instancesDone = instancesDone
	suite.update.instancesCurrent = instancesCurrent

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateModel *models.UpdateModel) {
			suite.Equal(suite.updateID, updateModel.UpdateID)
			suite.Equal(pbupdate.State_ABORTED, updateModel.State)
			suite.Equal(uint32(len(instancesDone)), updateModel.InstancesDone)
			suite.Equal(instancesCurrent, updateModel.InstancesCurrent)
		}).
		Return(nil)

	err := suite.update.Cancel(context.Background())
	suite.NoError(err)
	suite.Equal(pbupdate.State_ABORTED, suite.update.state)
}

// TestCancelDBError tests receiving a DB eror when canceling a job update
func (suite *UpdateTestSuite) TestCancelDBError() {
	instancesDone := []uint32{1, 2, 3, 4, 5}
	instancesCurrent := []uint32{6, 7}
	suite.update.state = pbupdate.State_INITIALIZED
	suite.update.instancesDone = instancesDone
	suite.update.instancesCurrent = instancesCurrent

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := suite.update.Cancel(context.Background())
	suite.EqualError(err, "fake db error")
	suite.Equal(pbupdate.State_INITIALIZED, suite.update.state)
}

// TestUpdateGetState tests getting state of a job update
func (suite *UpdateTestSuite) TestUpdateGetState() {
	suite.update.instancesDone = []uint32{1, 2, 3, 4, 5}
	suite.update.state = pbupdate.State_ROLLING_FORWARD

	state := suite.update.GetState()
	suite.Equal(suite.update.state, state.State)
	suite.True(reflect.DeepEqual(state.Instances, suite.update.instancesDone))
}

// TestUpdateGetGoalState tests getting goal state of a job update
func (suite *UpdateTestSuite) TestUpdateGetGoalState() {
	suite.update.instancesTotal = []uint32{1, 2, 3, 4, 5}

	state := suite.update.GetGoalState()
	suite.True(reflect.DeepEqual(state.Instances, suite.update.instancesTotal))
}

// TestUpdateGetInstancesAdded tests getting instances added in a job update
func (suite *UpdateTestSuite) TestUpdateGetInstancesAdded() {
	suite.update.instancesAdded = []uint32{1, 2, 3, 4, 5}

	instances := suite.update.GetInstancesAdded()
	suite.True(reflect.DeepEqual(instances, suite.update.instancesAdded))
}

// TestTestUpdateGetInstancesUpdated tests getting
// udpated instances in a job update
func (suite *UpdateTestSuite) TestUpdateGetInstancesUpdated() {
	suite.update.instancesUpdated = []uint32{1, 2, 3, 4, 5}

	instances := suite.update.GetInstancesUpdated()
	suite.True(reflect.DeepEqual(instances, suite.update.instancesUpdated))
}

// TestUpdateGetInstancesCurrent tests getting current
// instances in a job update.
func (suite *UpdateTestSuite) TestUpdateGetInstancesCurrent() {
	suite.update.instancesCurrent = []uint32{1, 2, 3, 4, 5}

	instances := suite.update.GetInstancesCurrent()
	suite.True(reflect.DeepEqual(instances, suite.update.instancesCurrent))
}
