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

	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
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

// TestCreateUpdateInstancesUpdatedOutOfRange tests the case that instancesUpdated is out of
// job config instance count range
func (suite *UpdateTestSuite) TestCreateUpdateInstancesUpdatedOutOfRange() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, _ :=
		initializeUpdateTest(
			instanceCount, version, instanceCount, version)

	err := suite.update.Create(
		context.Background(),
		suite.jobID,
		newJobConfig,
		prevJobConfig,
		&models.ConfigAddOn{},
		nil,
		[]uint32{0, 1, 2, 4, 5, 6, 7, 8, 9, 10},
		nil,
		models.WorkflowType_UPDATE,
		updateConfig,
	)

	suite.Error(err)
}

// TestCreateUpdateInstancesUpdatedOutOfRange tests the case that instancesUpdated is out of
// job config instance count range
func (suite *UpdateTestSuite) TestCreateUpdateInstancesAddedOutOfRange() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, _ :=
		initializeUpdateTest(
			instanceCount, version, instanceCount+1, version+1)

	err := suite.update.Create(
		context.Background(),
		suite.jobID,
		newJobConfig,
		prevJobConfig,
		&models.ConfigAddOn{},
		[]uint32{10, 11},
		[]uint32{0, 1, 2, 4, 5, 6, 7, 8, 9},
		nil,
		models.WorkflowType_UPDATE,
		updateConfig,
	)

	suite.Error(err)
}

// TestCreateUpdateInstancesRemovedOutOfRange tests the case that
// instancesRemoved is out of job config instance count range
func (suite *UpdateTestSuite) TestCreateUpdateInstancesRemovedOutOfRange() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, _ :=
		initializeUpdateTest(
			instanceCount, version, instanceCount-1, version+1)

	err := suite.update.Create(
		context.Background(),
		suite.jobID,
		newJobConfig,
		prevJobConfig,
		&models.ConfigAddOn{},
		nil,
		[]uint32{0, 1, 2, 4, 5, 6, 7, 8},
		[]uint32{11},
		models.WorkflowType_UPDATE,
		updateConfig,
	)

	suite.Error(err)
}

// TestValidCreateUpdate tests creating a valid job update with changes to the
// default configuration and the instance configuration,
// as well as adding one new instance.
func (suite *UpdateTestSuite) TestValidCreateUpdate() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, jobRuntime :=
		initializeUpdateTest(
			instanceCount, version, instanceCount+1, version)

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

	configAddOn := &models.ConfigAddOn{}
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(prevJobConfig, configAddOn, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(jobRuntime, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(prevJobConfig.ChangeLog.Version, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, addOn *models.ConfigAddOn) {
			suite.Equal(newJobConfig.InstanceCount, config.InstanceCount)
			suite.Equal(
				newJobConfig.DefaultConfig.Command.Value,
				config.DefaultConfig.Command.Value,
			)
			suite.Equal(
				newJobConfig.ChangeLog.Version+1,
				config.ChangeLog.Version,
			)
		}).
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
		&models.ConfigAddOn{},
		[]uint32{10},
		[]uint32{0, 1, 2, 4, 5, 6, 7, 8, 9},
		nil,
		models.WorkflowType_UPDATE,
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

// TestValidCreateUpdateWithReducedInstanceCount tests creating a valid job
// update with changes to the default configuration and the instance
// configuration, as well as removing one instance.
func (suite *UpdateTestSuite) TestValidCreateUpdateWithReducedInstanceCount() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, jobRuntime :=
		initializeUpdateTest(
			instanceCount, version, instanceCount-1, version)

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

	configAddOn := &models.ConfigAddOn{}
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(prevJobConfig, configAddOn, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(jobRuntime, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(prevJobConfig.ChangeLog.Version, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, addOn *models.ConfigAddOn) {
			suite.Equal(newJobConfig.InstanceCount, config.InstanceCount)
			suite.Equal(
				newJobConfig.DefaultConfig.Command.Value,
				config.DefaultConfig.Command.Value,
			)
			suite.Equal(
				newJobConfig.ChangeLog.Version+1,
				config.ChangeLog.Version,
			)
			suite.Equal(*configAddOn, *addOn)
		}).
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
		configAddOn,
		nil,
		[]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8},
		[]uint32{9},
		models.WorkflowType_UPDATE,
		updateConfig,
	)

	suite.NoError(err)
	suite.Equal(suite.jobID, suite.update.JobID())
	suite.Equal(pbupdate.State_INITIALIZED, suite.update.state)
	suite.Equal(updateConfig.BatchSize, suite.update.updateConfig.BatchSize)
	suite.Equal(version, suite.update.jobPrevVersion)
	suite.Equal(version+1, suite.update.jobVersion)
	suite.Equal(instanceCount, uint32(len(suite.update.instancesTotal)))
	suite.Equal(1, len(suite.update.instancesRemoved))
	suite.Equal(instanceCount-1, uint32(len(suite.update.instancesUpdated)))
}

// TestCreateUpdateDBError tests failing to persist the job update in database
func (suite *UpdateTestSuite) TestCreateUpdateDBError() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, _ :=
		initializeUpdateTest(
			instanceCount, version, instanceCount, version)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: prevJobConfig.
				GetChangeLog().
				GetVersion(),
		}, nil)

	configAddOn := &models.ConfigAddOn{}
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(prevJobConfig, configAddOn, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(prevJobConfig.ChangeLog.Version, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, addOn *models.ConfigAddOn) {
			suite.Equal(newJobConfig.InstanceCount, config.InstanceCount)
			suite.Equal(
				newJobConfig.DefaultConfig.Command.Value,
				config.DefaultConfig.Command.Value,
			)
			suite.Equal(
				newJobConfig.ChangeLog.Version+1,
				config.ChangeLog.Version,
			)
			suite.Equal(*configAddOn, *addOn)
		}).
		Return(nil)

	suite.updateStore.EXPECT().
		CreateUpdate(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := suite.update.Create(
		context.Background(),
		suite.jobID,
		newJobConfig,
		prevJobConfig,
		configAddOn,
		nil,
		[]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		nil,
		models.WorkflowType_UPDATE,
		updateConfig,
	)
	suite.EqualError(err, "fake db error")
}

// TestCreateUpdateJobConfigDBError tests failing to persist the new job
// configuration in database
func (suite *UpdateTestSuite) TestCreateUpdateJobConfigDBError() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, _ :=
		initializeUpdateTest(
			instanceCount, version, instanceCount, version)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: prevJobConfig.
				GetChangeLog().
				GetVersion(),
		}, nil)

	configAddOn := &models.ConfigAddOn{}
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(prevJobConfig, configAddOn, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(prevJobConfig.ChangeLog.Version, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, addOn *models.ConfigAddOn) {
			suite.Equal(newJobConfig.InstanceCount, config.InstanceCount)
			suite.Equal(
				newJobConfig.DefaultConfig.Command.Value,
				config.DefaultConfig.Command.Value,
			)
			suite.Equal(
				newJobConfig.ChangeLog.Version+1,
				config.ChangeLog.Version,
			)
			suite.Equal(*configAddOn, *addOn)
		}).
		Return(fmt.Errorf("fake db error"))

	err := suite.update.Create(
		context.Background(),
		suite.jobID,
		newJobConfig,
		prevJobConfig,
		configAddOn,
		nil,
		[]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		nil,
		models.WorkflowType_UPDATE,
		updateConfig,
	)
	suite.EqualError(err, "fake db error")
}

// TestValidCreateRestart tests creating a valid job restart
func (suite *UpdateTestSuite) TestValidCreateRestart() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, jobRuntime :=
		initializeUpdateTest(
			instanceCount, version, instanceCount, version)

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

	configAddOn := &models.ConfigAddOn{}
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(prevJobConfig, configAddOn, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(jobRuntime, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(prevJobConfig.ChangeLog.Version, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, addOn *models.ConfigAddOn) {
			suite.Equal(newJobConfig.InstanceCount, config.InstanceCount)
			suite.Equal(
				newJobConfig.DefaultConfig.Command.Value,
				config.DefaultConfig.Command.Value,
			)
			suite.Equal(
				newJobConfig.ChangeLog.Version+1,
				config.ChangeLog.Version,
			)
			suite.Equal(*configAddOn, *addOn)
		}).
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
		configAddOn,
		nil,
		[]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		nil,
		models.WorkflowType_RESTART,
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
	suite.Equal(models.WorkflowType_RESTART, suite.update.GetWorkflowType())
}

// TestValidCreateStart tests creating a valid job start
func (suite *UpdateTestSuite) TestValidCreateStart() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, jobRuntime :=
		initializeUpdateTest(
			instanceCount, version, instanceCount, version)

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

	configAddOn := &models.ConfigAddOn{}
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(prevJobConfig, configAddOn, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(jobRuntime, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(prevJobConfig.ChangeLog.Version, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, addOn *models.ConfigAddOn) {
			suite.Equal(newJobConfig.InstanceCount, config.InstanceCount)
			suite.Equal(
				newJobConfig.DefaultConfig.Command.Value,
				config.DefaultConfig.Command.Value,
			)
			suite.Equal(
				newJobConfig.ChangeLog.Version+1,
				config.ChangeLog.Version,
			)
			suite.Equal(*configAddOn, *addOn)
		}).
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
		configAddOn,
		nil,
		[]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		nil,
		models.WorkflowType_START,
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
	suite.Equal(models.WorkflowType_START, suite.update.GetWorkflowType())
}

// TestWorkflowOverWrite tests which workflow overwrite
func (suite *UpdateTestSuite) TestWorkflowOverWrite() {
	tests := []struct {
		previousWorkflowType models.WorkflowType
		newWorkflowType      models.WorkflowType
		isValidOverWrite     bool
	}{
		{models.WorkflowType_UPDATE, models.WorkflowType_UPDATE, true},
		{models.WorkflowType_UPDATE, models.WorkflowType_START, false},
		{models.WorkflowType_UPDATE, models.WorkflowType_STOP, false},
		{models.WorkflowType_UPDATE, models.WorkflowType_RESTART, false},
		{models.WorkflowType_START, models.WorkflowType_START, false},
		{models.WorkflowType_STOP, models.WorkflowType_STOP, false},
		{models.WorkflowType_RESTART, models.WorkflowType_UPDATE, false},
	}

	for _, t := range tests {
		instanceCount := uint32(10)
		version := uint64(2)
		prevJobConfig, newJobConfig, updateConfig, jobRuntime :=
			initializeUpdateTest(
				instanceCount, version, instanceCount, version)
		existingUpdateID := &peloton.UpdateID{
			Value: uuid.New(),
		}
		jobRuntime.UpdateID = existingUpdateID

		suite.updateStore.EXPECT().
			CreateUpdate(
				gomock.Any(), gomock.Any()).
			Return(nil)

		configAddOn := &models.ConfigAddOn{}
		suite.jobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.jobID).
			Return(prevJobConfig, configAddOn, nil)

		suite.jobStore.EXPECT().
			GetJobRuntime(gomock.Any(), suite.jobID).
			Return(jobRuntime, nil)

		suite.jobStore.EXPECT().
			GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
			Return(prevJobConfig.ChangeLog.Version, nil)

		suite.updateStore.EXPECT().
			GetUpdate(gomock.Any(), existingUpdateID).
			Return(&models.UpdateModel{
				Type: t.previousWorkflowType,
			}, nil)

		suite.jobStore.EXPECT().
			UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
			Return(nil)

		suite.jobStore.EXPECT().
			UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
			Return(nil).
			MaxTimes(1)

		err := suite.update.Create(
			context.Background(),
			suite.jobID,
			newJobConfig,
			prevJobConfig,
			configAddOn,
			nil,
			[]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			nil,
			t.newWorkflowType,
			updateConfig,
		)

		if t.isValidOverWrite {
			suite.NoError(err)
		} else {
			suite.Error(err)
		}

		suite.update.jobFactory.ClearJob(suite.jobID)
	}
}

// TestValidCreateStop tests creating a valid job stop
func (suite *UpdateTestSuite) TestValidCreateStop() {
	instanceCount := uint32(10)
	version := uint64(2)
	prevJobConfig, newJobConfig, updateConfig, jobRuntime :=
		initializeUpdateTest(
			instanceCount, version, instanceCount, version)

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

	configAddOn := &models.ConfigAddOn{}
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(prevJobConfig, configAddOn, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(jobRuntime, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(prevJobConfig.ChangeLog.Version, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, addOn *models.ConfigAddOn) {
			suite.Equal(newJobConfig.InstanceCount, config.InstanceCount)
			suite.Equal(
				newJobConfig.DefaultConfig.Command.Value,
				config.DefaultConfig.Command.Value,
			)
			suite.Equal(
				newJobConfig.ChangeLog.Version+1,
				config.ChangeLog.Version,
			)
			suite.Equal(*configAddOn, *addOn)
		}).
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
		configAddOn,
		nil,
		[]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		nil,
		models.WorkflowType_STOP,
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
	suite.Equal(models.WorkflowType_STOP, suite.update.GetWorkflowType())
}

// TestValidWriteProgress tests successfully writing the status
// of an update into the DB.
func (suite *UpdateTestSuite) TestValidWriteProgress() {
	state := pbupdate.State_ROLLING_FORWARD
	instancesDone := []uint32{0, 1, 2, 3}
	instancesCurrent := []uint32{4, 5}
	instanceFailed := []uint32{6, 7}

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateModel *models.UpdateModel) {
			suite.Equal(suite.updateID, updateModel.UpdateID)
			suite.Equal(state, updateModel.State)
			suite.Equal(uint32(len(instancesDone)), updateModel.InstancesDone)
			suite.Equal(instancesCurrent, updateModel.InstancesCurrent)
			suite.Equal(uint32(len(instanceFailed)), updateModel.InstancesFailed)
		}).
		Return(nil)

	err := suite.update.WriteProgress(
		context.Background(),
		state,
		instancesDone,
		instanceFailed,
		instancesCurrent,
	)

	suite.NoError(err)
	suite.Equal(state, suite.update.state)
	suite.Equal(instancesCurrent, suite.update.instancesCurrent)
	suite.Equal(instancesDone, suite.update.instancesDone)
	suite.Equal(instanceFailed, suite.update.instancesFailed)
}

// TestWriteProgressAbortedUpdate tests WriteProgress invalidates
// progress update after it reaches terminated state
func (suite *UpdateTestSuite) TestWriteProgressAbortedUpdate() {
	suite.update.state = pbupdate.State_ABORTED
	state := pbupdate.State_ROLLING_FORWARD
	instancesDone := []uint32{0, 1, 2, 3}
	instancesCurrent := []uint32{4, 5}
	instanceFailed := []uint32{}

	err := suite.update.WriteProgress(
		context.Background(),
		state,
		instancesDone,
		instancesCurrent,
		instanceFailed,
	)

	suite.NoError(err)
	suite.Equal(suite.update.state, pbupdate.State_ABORTED)
}

// TestValidWriteProgress tests failing to persist the status
// of an update into the DB.
func (suite *UpdateTestSuite) TestWriteProgressDBError() {
	state := pbupdate.State_ROLLING_FORWARD
	instancesDone := []uint32{0, 1, 2, 3}
	instancesCurrent := []uint32{4, 5}
	instanceFailed := []uint32{}

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := suite.update.WriteProgress(
		context.Background(),
		state,
		instancesDone,
		instancesCurrent,
		instanceFailed,
	)
	suite.EqualError(err, "fake db error")
}

// TestConsecutiveWriteProgressPrevState tests after consecutive call to
// WriteProgress, prevState is correcct
func (suite *UpdateTestSuite) TestConsecutiveWriteProgressPrevState() {
	instancesDone := []uint32{0, 1, 2, 3}
	instancesCurrent := []uint32{4, 5}
	instanceFailed := []uint32{}

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	suite.update.state = pbupdate.State_INITIALIZED
	suite.NoError(suite.update.WriteProgress(
		context.Background(),
		pbupdate.State_ROLLING_FORWARD,
		instancesDone,
		instancesCurrent,
		instanceFailed,
	))
	suite.Equal(suite.update.prevState, pbupdate.State_INITIALIZED)
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_FORWARD)

	// prev state should still be INITIALIZED after we write
	// State_ROLLING_FORWARD twice
	suite.NoError(suite.update.WriteProgress(
		context.Background(),
		pbupdate.State_ROLLING_FORWARD,
		instancesDone,
		instancesCurrent,
		instanceFailed,
	))
	suite.Equal(suite.update.prevState, pbupdate.State_INITIALIZED)
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_FORWARD)

	suite.NoError(suite.update.WriteProgress(
		context.Background(),
		pbupdate.State_ROLLING_FORWARD,
		instancesDone,
		instancesCurrent,
		instanceFailed,
	))
	suite.Equal(suite.update.prevState, pbupdate.State_INITIALIZED)
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_FORWARD)

	suite.NoError(suite.update.WriteProgress(
		context.Background(),
		pbupdate.State_ROLLING_BACKWARD,
		instancesDone,
		instancesCurrent,
		instanceFailed,
	))
	suite.Equal(suite.update.prevState, pbupdate.State_ROLLING_FORWARD)
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_BACKWARD)
}

// TestPauseSuccess tests successfully pause an update
func (suite *UpdateTestSuite) TestPauseSuccess() {
	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateModel *models.UpdateModel) {
			suite.Equal(suite.updateID, updateModel.UpdateID)
			suite.Equal(pbupdate.State_PAUSED, updateModel.State)
		}).
		Return(nil)

	suite.NoError(suite.update.Pause(context.Background()))
}

// TestPausePausedUpdate tests pause an update already paused
func (suite *UpdateTestSuite) TestPausePausedUpdate() {
	suite.update.state = pbupdate.State_PAUSED
	suite.NoError(suite.update.Pause(context.Background()))
}

// TestPauseResumeRollingForwardUpdate tests pause and resume a rolling
// forward update
func (suite *UpdateTestSuite) TestPauseResumeRollingForwardUpdate() {
	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.NoError(suite.update.Pause(context.Background()))
	suite.NoError(suite.update.Resume(context.Background()))
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_FORWARD)
}

// TestPauseResumeRollingForwardUpdate tests pause and resume a rolling
// backward update
func (suite *UpdateTestSuite) TestPauseResumeRollingBackwardUpdate() {
	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	suite.update.state = pbupdate.State_ROLLING_BACKWARD
	suite.NoError(suite.update.Pause(context.Background()))
	suite.NoError(suite.update.Resume(context.Background()))
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_BACKWARD)
}

// TestPauseResumeInitializedUpdate tests pause and resume an
// initialized update
func (suite *UpdateTestSuite) TestPauseResumeInitializedUpdate() {
	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	suite.update.state = pbupdate.State_INITIALIZED
	suite.NoError(suite.update.Pause(context.Background()))
	suite.NoError(suite.update.Resume(context.Background()))
	suite.Equal(suite.update.state, pbupdate.State_INITIALIZED)
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
	suite.Equal(pbupdate.State_INVALID, suite.update.state)
}

// TestCancelTerminatedUpdate tests canceling a terminated update
func (suite *UpdateTestSuite) TestCancelTerminatedUpdate() {
	suite.update.state = pbupdate.State_SUCCEEDED

	err := suite.update.Cancel(context.Background())
	suite.NoError(err)
}

// TestUpdateGetState tests getting state of a job update
func (suite *UpdateTestSuite) TestUpdateGetState() {
	suite.update.instancesDone = []uint32{1, 2, 3, 4, 5}
	suite.update.instancesFailed = []uint32{6, 7}
	suite.update.state = pbupdate.State_ROLLING_FORWARD

	state := suite.update.GetState()
	suite.Equal(suite.update.state, state.State)
	suite.Equal(state.Instances,
		append(suite.update.instancesDone, suite.update.instancesFailed...))
}

// TestUpdateGetState tests getting update config
func (suite *UpdateTestSuite) TestUpdateGetConfig() {
	batchSize := uint32(5)
	suite.update.updateConfig = &pbupdate.UpdateConfig{
		BatchSize: batchSize,
	}

	suite.Equal(suite.update.GetUpdateConfig().GetBatchSize(), batchSize)
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

// TestUpdateGetInstancesDone tests getting instances done in a job update
func (suite *UpdateTestSuite) TestUpdateGetInstancesDone() {
	suite.update.instancesDone = []uint32{1, 2, 3, 4, 5}

	instances := suite.update.GetInstancesDone()
	suite.True(reflect.DeepEqual(instances, suite.update.instancesDone))
}

// TestUpdateGetInstancesFailed tests getting instances failed in a job update
func (suite *UpdateTestSuite) TestUpdateGetInstancesFailed() {
	suite.update.instancesFailed = []uint32{1, 2, 3, 4, 5}

	instances := suite.update.GetInstancesFailed()
	suite.True(reflect.DeepEqual(instances, suite.update.instancesFailed))
}

// TestTestUpdateGetInstancesUpdated tests getting
// udpated instances in a job update
func (suite *UpdateTestSuite) TestUpdateGetInstancesUpdated() {
	suite.update.instancesUpdated = []uint32{1, 2, 3, 4, 5}

	instances := suite.update.GetInstancesUpdated()
	suite.True(reflect.DeepEqual(instances, suite.update.instancesUpdated))
}

// TestUpdateGetInstancesRemoved tests getting instances removed in a job update
func (suite *UpdateTestSuite) TestUpdateGetInstancesRemoved() {
	suite.update.instancesRemoved = []uint32{1, 2, 3, 4, 5}

	instances := suite.update.GetInstancesRemoved()
	suite.True(reflect.DeepEqual(instances, suite.update.instancesRemoved))
}

// TestUpdateGetInstancesCurrent tests getting current
// instances in a job update.
func (suite *UpdateTestSuite) TestUpdateGetInstancesCurrent() {
	suite.update.instancesCurrent = []uint32{1, 2, 3, 4, 5}

	instances := suite.update.GetInstancesCurrent()
	suite.True(reflect.DeepEqual(instances, suite.update.instancesCurrent))
}

func (suite *UpdateTestSuite) TestUpdateRecover_RollingForward() {
	instancesTotal := []uint32{0, 1, 2, 3, 4}
	instancesDone := []uint32{0, 1}
	instancesCurrent := []uint32{2, 3, 4}
	instanceCount := uint32(len(instancesTotal))
	preJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 0,
		},
		InstanceCount: instanceCount,
	}
	newJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
		InstanceCount: instanceCount,
		Labels:        []*peloton.Label{{"test-key", "test-value"}},
	}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID:                suite.jobID,
			InstancesTotal:       uint32(len(instancesTotal)),
			InstancesUpdated:     instancesTotal,
			InstancesDone:        uint32(len(instancesDone)),
			InstancesCurrent:     instancesCurrent,
			PrevJobConfigVersion: preJobConfig.GetChangeLog().GetVersion(),
			JobConfigVersion:     newJobConfig.GetChangeLog().GetVersion(),
			State:                pbupdate.State_ROLLING_FORWARD,
			Type:                 models.WorkflowType_UPDATE,
		}, nil)

	for i := uint32(0); i < instanceCount; i++ {
		taskRuntime := &pbtask.RuntimeInfo{
			State:   pbtask.TaskState_RUNNING,
			Healthy: pbtask.HealthState_DISABLED,
		}
		if contains(i, instancesCurrent) {
			taskRuntime.DesiredConfigVersion = newJobConfig.GetChangeLog().GetVersion()
			taskRuntime.ConfigVersion = preJobConfig.GetChangeLog().GetVersion()
		} else if contains(i, instancesDone) {
			taskRuntime.DesiredConfigVersion = newJobConfig.GetChangeLog().GetVersion()
			taskRuntime.ConfigVersion = newJobConfig.GetChangeLog().GetVersion()
		} else {
			taskRuntime.DesiredConfigVersion = preJobConfig.GetChangeLog().GetVersion()
			taskRuntime.ConfigVersion = preJobConfig.GetChangeLog().GetVersion()
		}

		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).
			Return(taskRuntime, nil)
	}

	err := suite.update.Recover(context.Background())
	suite.NoError(err)

	suite.Equal(suite.update.instancesDone, instancesDone)
	suite.Equal(suite.update.instancesCurrent, instancesCurrent)
	suite.Equal(suite.update.instancesTotal, instancesTotal)
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_FORWARD)
	suite.Equal(suite.update.GetWorkflowType(), models.WorkflowType_UPDATE)
}

func (suite *UpdateTestSuite) TestUpdateRecover_Succeeded() {
	var instancesCurrent []uint32
	instancesTotal := []uint32{0, 1, 2, 3, 4}
	instancesDone := instancesTotal

	instanceCount := uint32(len(instancesTotal))
	preJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 0,
		},
		InstanceCount: instanceCount,
	}
	newJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
		InstanceCount: instanceCount,
		Labels:        []*peloton.Label{{"test-key", "test-value"}},
	}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID:                suite.jobID,
			InstancesTotal:       uint32(len(instancesTotal)),
			InstancesDone:        uint32(len(instancesDone)),
			InstancesCurrent:     instancesCurrent,
			PrevJobConfigVersion: preJobConfig.GetChangeLog().GetVersion(),
			JobConfigVersion:     newJobConfig.GetChangeLog().GetVersion(),
			State:                pbupdate.State_SUCCEEDED,
			Type:                 models.WorkflowType_UPDATE,
		}, nil)

	err := suite.update.Recover(context.Background())
	suite.NoError(err)

	suite.Empty(suite.update.instancesDone)
	suite.Equal(suite.update.instancesCurrent, instancesCurrent)
	suite.Empty(suite.update.instancesTotal)
	suite.Equal(suite.update.state, pbupdate.State_SUCCEEDED)
	suite.Equal(suite.update.GetWorkflowType(), models.WorkflowType_UPDATE)
}

func (suite *UpdateTestSuite) TestUpdateRecover_Initialized() {
	instancesTotal := []uint32{0, 1, 2, 3, 4}
	instancesCurrent := []uint32{0, 1, 2, 3, 4}
	var instancesDone []uint32
	instanceCount := uint32(len(instancesTotal))
	preJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 0,
		},
		InstanceCount: instanceCount,
	}
	newJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
		InstanceCount: instanceCount,
		Labels:        []*peloton.Label{{"test-key", "test-value"}},
	}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID:                suite.jobID,
			InstancesTotal:       uint32(len(instancesTotal)),
			InstancesUpdated:     instancesTotal,
			InstancesDone:        uint32(len(instancesDone)),
			InstancesCurrent:     instancesCurrent,
			PrevJobConfigVersion: preJobConfig.GetChangeLog().GetVersion(),
			JobConfigVersion:     newJobConfig.GetChangeLog().GetVersion(),
			State:                pbupdate.State_INITIALIZED,
			Type:                 models.WorkflowType_UPDATE,
		}, nil)

	for i := uint32(0); i < instanceCount; i++ {
		taskRuntime := &pbtask.RuntimeInfo{
			State: pbtask.TaskState_RUNNING,
		}
		if contains(i, instancesCurrent) {
			taskRuntime.DesiredConfigVersion = newJobConfig.GetChangeLog().GetVersion()
			taskRuntime.ConfigVersion = preJobConfig.GetChangeLog().GetVersion()
		} else if contains(i, instancesDone) {
			taskRuntime.DesiredConfigVersion = newJobConfig.GetChangeLog().GetVersion()
			taskRuntime.ConfigVersion = newJobConfig.GetChangeLog().GetVersion()
		} else {
			taskRuntime.DesiredConfigVersion = preJobConfig.GetChangeLog().GetVersion()
			taskRuntime.ConfigVersion = preJobConfig.GetChangeLog().GetVersion()
		}

		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).
			Return(taskRuntime, nil)
	}

	err := suite.update.Recover(context.Background())
	suite.NoError(err)

	suite.Equal(suite.update.instancesDone, instancesDone)
	suite.Equal(suite.update.instancesCurrent, instancesCurrent)
	suite.Equal(suite.update.instancesTotal, instancesTotal)
	suite.Equal(suite.update.state, pbupdate.State_INITIALIZED)
	suite.Equal(suite.update.GetWorkflowType(), models.WorkflowType_UPDATE)
}

func (suite *UpdateTestSuite) TestUpdateRecover_UpdateStoreErr() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(nil, yarpcerrors.UnavailableErrorf("test error"))

	err := suite.update.Recover(context.Background())
	suite.Error(err)

	suite.Empty(suite.update.instancesDone)
	suite.Empty(suite.update.instancesCurrent)
	suite.Empty(suite.update.instancesTotal)
	suite.Equal(suite.update.state, pbupdate.State_INVALID)
}

func (suite *UpdateTestSuite) TestUpdateRecoverGetRuntimeFailure() {
	instancesTotal := []uint32{0, 1, 2, 3, 4}
	instancesCurrent := []uint32{0, 1, 2, 3, 4}
	var instancesDone []uint32
	instanceCount := uint32(len(instancesTotal))
	preJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 0,
		},
		InstanceCount: instanceCount,
	}
	newJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
		InstanceCount: instanceCount,
		Labels:        []*peloton.Label{{"test-key", "test-value"}},
	}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID:                suite.jobID,
			InstancesTotal:       uint32(len(instancesTotal)),
			InstancesUpdated:     instancesTotal,
			InstancesDone:        uint32(len(instancesDone)),
			InstancesCurrent:     instancesCurrent,
			PrevJobConfigVersion: preJobConfig.GetChangeLog().GetVersion(),
			JobConfigVersion:     newJobConfig.GetChangeLog().GetVersion(),
			State:                pbupdate.State_INITIALIZED,
			Type:                 models.WorkflowType_UPDATE,
		}, nil)

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, uint32(0)).
		Return(nil, fmt.Errorf("test db error"))

	err := suite.update.Recover(context.Background())
	suite.Error(err)

	suite.Empty(suite.update.instancesDone)
	suite.Empty(suite.update.instancesCurrent)
	suite.Empty(suite.update.instancesTotal)
	suite.Equal(suite.update.state, pbupdate.State_INVALID)
}

func (suite *UpdateTestSuite) TestUpdateRecoverRemoveInstances() {
	instancesTotal := []uint32{0, 1, 2, 3, 4}
	instancesCurrent := []uint32{0, 1, 2, 3, 4}
	var instancesDone []uint32
	instanceCount := uint32(len(instancesTotal))
	preJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 0,
		},
		InstanceCount: instanceCount,
	}
	newJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
		InstanceCount: instanceCount,
		Labels:        []*peloton.Label{{"test-key", "test-value"}},
	}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID:                suite.jobID,
			InstancesTotal:       uint32(len(instancesTotal)),
			InstancesUpdated:     []uint32{},
			InstancesDone:        uint32(len(instancesDone)),
			InstancesCurrent:     instancesCurrent,
			InstancesRemoved:     instancesCurrent,
			PrevJobConfigVersion: preJobConfig.GetChangeLog().GetVersion(),
			JobConfigVersion:     newJobConfig.GetChangeLog().GetVersion(),
			State:                pbupdate.State_INITIALIZED,
			Type:                 models.WorkflowType_UPDATE,
		}, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: 2,
		}, nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(newJobConfig, &models.ConfigAddOn{}, nil).AnyTimes()

	for i := uint32(0); i < instanceCount; i++ {
		taskRuntime := &pbtask.RuntimeInfo{
			State:   pbtask.TaskState_RUNNING,
			Healthy: pbtask.HealthState_DISABLED,
		}

		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).
			Return(taskRuntime, yarpcerrors.NotFoundErrorf("not-found"))
	}

	err := suite.update.Recover(context.Background())
	suite.NoError(err)

	suite.Equal(suite.update.instancesDone, instancesCurrent)
}

// testCachedJob is a test object for cached job, which enables user to
// overwrite some of its behavior
type testCachedJob struct {
	*job

	compareAndSetRuntime func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) (*pbjob.RuntimeInfo, error)
}

// if testCachedJob.compareAndSetRuntime is set, CompareAndSetRuntime will call
// testCachedJob.compareAndSetRuntime. Otherwise, it will inherit the behavior
// of job.CompareAndSetRuntime
func (j *testCachedJob) CompareAndSetRuntime(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) (*pbjob.RuntimeInfo, error) {
	if j.compareAndSetRuntime != nil {
		return j.compareAndSetRuntime(ctx, jobRuntime)
	}
	return j.CompareAndSetRuntime(ctx, jobRuntime)
}

// TestUpdateWorkflowWithUnexpectedVersionError tests the case that
// creating update workflow with UnexpectedVersionError can be recovered
// by retry
func (suite *UpdateTestSuite) TestUpdateWorkflowWithUnexpectedVersionError() {
	testJob := &testCachedJob{
		job: &job{
			runtime: &pbjob.RuntimeInfo{
				Revision: &peloton.ChangeLog{
					Version: 1,
				},
			},
			jobFactory: &jobFactory{
				jobStore: suite.jobStore,
			},
		},
	}

	testJob.compareAndSetRuntime = func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) (*pbjob.RuntimeInfo, error) {
		// simulate the case that when CompareAndSetRuntime is called,
		// job runtime is changed by another goroutine
		testJob.runtime = &pbjob.RuntimeInfo{
			Revision: &peloton.ChangeLog{
				Version: 2,
			},
		}
		return testJob.job.CompareAndSetRuntime(ctx, jobRuntime)
	}

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	err := suite.update.updateJobConfigAndUpdateID(
		context.Background(),
		testJob,
		models.WorkflowType_RESTART,
		2,
	)

	suite.NoError(err)
}

// TestUpdateWorkflowWithTooManyUnexpectedVersionError tests the case that
// creating update workflow with too many UnexpectedVersionError
func (suite *UpdateTestSuite) TestUpdateWorkflowWithTooManyUnexpectedVersionError() {
	testJob := &testCachedJob{
		job: &job{
			runtime: &pbjob.RuntimeInfo{
				Revision: &peloton.ChangeLog{
					Version: 1,
				},
			},
			jobFactory: &jobFactory{
				jobStore: suite.jobStore,
			},
		},
	}

	testJob.compareAndSetRuntime = func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) (*pbjob.RuntimeInfo, error) {
		// always change the version to something different form input
		testJob.runtime = &pbjob.RuntimeInfo{
			Revision: &peloton.ChangeLog{
				Version: jobRuntime.GetRevision().GetVersion() + 1,
			},
		}
		return testJob.job.CompareAndSetRuntime(ctx, jobRuntime)
	}

	err := suite.update.updateJobConfigAndUpdateID(
		context.Background(),
		testJob,
		models.WorkflowType_RESTART,
		2,
	)

	suite.Error(err)
}

// Test function IsTaskInUpdateProgress
func (suite *UpdateTestSuite) TestIsTaskInUpdateProgress() {
	suite.update.instancesCurrent = []uint32{1, 2}
	suite.True(suite.update.IsTaskInUpdateProgress(uint32(1)))
	suite.False(suite.update.IsTaskInUpdateProgress(uint32(0)))
}

// TestUpdateRollbackSuccess tests successful rollback
func (suite *UpdateTestSuite) TestUpdateRollbackSuccess() {
	jobPrevVersion := uint64(1)
	jobPrevConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: jobPrevVersion,
		},
		InstanceCount: 5,
	}

	jobVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: jobVersion,
		},
		InstanceCount: 10,
	}
	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.update.jobID = suite.jobID
	suite.update.jobVersion = jobVersion
	suite.update.jobPrevVersion = jobPrevVersion

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: jobVersion,
		}, nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(jobConfig, nil, nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), suite.jobID, jobPrevVersion).
		Return(jobPrevConfig, nil, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(jobVersion, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, _ *models.ConfigAddOn) {
			suite.Equal(config.ChangeLog.Version, jobVersion+1)
		}).
		Return(nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, jobPrevConfig, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, jobConfig *pbjob.JobConfig, _ *models.ConfigAddOn) {
			// version number should increase
			suite.Equal(jobConfig.GetChangeLog().GetVersion(), jobVersion+1)
		}).
		Return(nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), suite.jobID, jobVersion).
		Return(jobConfig, nil, nil)

	for i := uint32(0); i < jobPrevConfig.GetInstanceCount(); i++ {
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).
			Return(&pbtask.RuntimeInfo{
				ConfigVersion: jobPrevVersion,
			}, nil)

		suite.taskStore.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, jobPrevVersion).
			Return(&pbtask.TaskConfig{}, nil, nil)
	}

	suite.updateStore.EXPECT().
		ModifyUpdate(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateInfo *models.UpdateModel) {
			suite.Equal(updateInfo.GetState(), pbupdate.State_ROLLING_BACKWARD)
			suite.Equal(updateInfo.GetJobConfigVersion(), jobVersion+1)
			suite.Equal(updateInfo.GetPrevJobConfigVersion(), jobVersion)
			suite.Equal(updateInfo.GetInstancesDone(), uint32(0))
			suite.Equal(updateInfo.GetInstancesFailed(), uint32(0))
			suite.Empty(updateInfo.GetInstancesCurrent())
		}).Return(nil)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, jobVersion+1)
		}).
		Return(nil)

	suite.NoError(suite.update.Rollback(context.Background()))

	suite.Empty(suite.update.GetInstancesFailed())
	suite.Empty(suite.update.GetInstancesDone())
	suite.Empty(suite.update.GetInstancesCurrent())
	suite.Equal(suite.update.GetState().State, pbupdate.State_ROLLING_BACKWARD)
	suite.Equal(suite.update.GetInstancesRemoved(), []uint32{5, 6, 7, 8, 9})
	suite.Equal(suite.update.GetInstancesUpdated(), []uint32{0, 1, 2, 3, 4})
}

// TestUpdateRollbackRetrySuccessAfterModifyUpdateFails tests update rollback can
// be retried successfully after it fails due to updateStore.ModifyUpdate
func (suite *UpdateTestSuite) TestUpdateRollbackRetrySuccessAfterModifyUpdateFails() {
	jobPrevVersion := uint64(1)
	jobPrevConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: jobPrevVersion,
		},
		InstanceCount: 5,
	}

	jobVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: jobVersion,
		},
		InstanceCount: 10,
	}
	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.update.jobID = suite.jobID
	suite.update.jobVersion = jobVersion
	suite.update.jobPrevVersion = jobPrevVersion

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: jobVersion,
		}, nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(jobConfig, nil, nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), suite.jobID, jobPrevVersion).
		Return(jobPrevConfig, nil, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(jobVersion, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, _ *models.ConfigAddOn) {
			suite.Equal(config.ChangeLog.Version, jobVersion+1)
		}).
		Return(nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, jobPrevConfig, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, jobConfig *pbjob.JobConfig, _ *models.ConfigAddOn) {
			// version number should increase
			suite.Equal(jobConfig.GetChangeLog().GetVersion(), jobVersion+1)
		}).
		Return(nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), suite.jobID, jobVersion).
		Return(jobConfig, nil, nil)

	for i := uint32(0); i < jobPrevConfig.GetInstanceCount(); i++ {
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).
			Return(&pbtask.RuntimeInfo{
				ConfigVersion: jobPrevVersion,
			}, nil)

		suite.taskStore.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, jobPrevVersion).
			Return(&pbtask.TaskConfig{}, nil, nil)
	}

	suite.updateStore.EXPECT().
		ModifyUpdate(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateInfo *models.UpdateModel) {
			suite.Equal(updateInfo.GetState(), pbupdate.State_ROLLING_BACKWARD)
			suite.Equal(updateInfo.GetJobConfigVersion(), jobVersion+1)
			suite.Equal(updateInfo.GetPrevJobConfigVersion(), jobVersion)
			suite.Equal(updateInfo.GetInstancesDone(), uint32(0))
			suite.Equal(updateInfo.GetInstancesFailed(), uint32(0))
			suite.Empty(updateInfo.GetInstancesCurrent())
		}).Return(fmt.Errorf("test error"))

	suite.Error(suite.update.Rollback(context.Background()))

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(jobConfig, nil, nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), suite.jobID, jobPrevVersion).
		Return(jobPrevConfig, nil, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(jobVersion, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, _ *models.ConfigAddOn) {
			suite.Equal(config.ChangeLog.Version, jobVersion+1)
		}).
		Return(nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, jobPrevConfig, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, jobConfig *pbjob.JobConfig, _ *models.ConfigAddOn) {
			// version number should increase
			suite.Equal(jobConfig.GetChangeLog().GetVersion(), jobVersion+1)
		}).
		Return(nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), suite.jobID, jobVersion).
		Return(jobConfig, nil, nil)

	for i := uint32(0); i < jobPrevConfig.GetInstanceCount(); i++ {
		suite.taskStore.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, jobPrevVersion).
			Return(&pbtask.TaskConfig{}, nil, nil)
	}

	suite.updateStore.EXPECT().
		ModifyUpdate(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateInfo *models.UpdateModel) {
			suite.Equal(updateInfo.GetState(), pbupdate.State_ROLLING_BACKWARD)
			suite.Equal(updateInfo.GetJobConfigVersion(), jobVersion+1)
			suite.Equal(updateInfo.GetPrevJobConfigVersion(), jobVersion)
			suite.Equal(updateInfo.GetInstancesDone(), uint32(0))
			suite.Equal(updateInfo.GetInstancesFailed(), uint32(0))
			suite.Empty(updateInfo.GetInstancesCurrent())
		}).Return(nil)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, jobVersion+1)
		}).
		Return(nil)

	suite.NoError(suite.update.Rollback(context.Background()))

	suite.Empty(suite.update.GetInstancesFailed())
	suite.Empty(suite.update.GetInstancesDone())
	suite.Empty(suite.update.GetInstancesCurrent())
	suite.Equal(suite.update.GetState().State, pbupdate.State_ROLLING_BACKWARD)
	suite.Equal(suite.update.GetInstancesRemoved(), []uint32{5, 6, 7, 8, 9})
	suite.Equal(suite.update.GetInstancesUpdated(), []uint32{0, 1, 2, 3, 4})
}

// TestUpdateRollbackRetrySuccessAfterJobRuntimeUpdateFails tests update rollback can
// be retried successfully after it fails due to job runtime update fails
func (suite *UpdateTestSuite) TestUpdateRollbackRetrySuccessAfterJobRuntimeUpdateFails() {
	jobPrevVersion := uint64(1)
	jobPrevConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: jobPrevVersion,
		},
		InstanceCount: 5,
	}

	jobVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: jobVersion,
		},
		InstanceCount: 10,
	}
	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.update.jobID = suite.jobID
	suite.update.jobVersion = jobVersion
	suite.update.jobPrevVersion = jobPrevVersion

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: jobVersion,
		}, nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(jobConfig, nil, nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), suite.jobID, jobPrevVersion).
		Return(jobPrevConfig, nil, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(jobVersion, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, _ *models.ConfigAddOn) {
			suite.Equal(config.ChangeLog.Version, jobVersion+1)
		}).
		Return(nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, jobPrevConfig, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, jobConfig *pbjob.JobConfig, _ *models.ConfigAddOn) {
			// version number should increase
			suite.Equal(jobConfig.GetChangeLog().GetVersion(), jobVersion+1)
		}).
		Return(nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), suite.jobID, jobVersion).
		Return(jobConfig, nil, nil)

	for i := uint32(0); i < jobPrevConfig.GetInstanceCount(); i++ {
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).
			Return(&pbtask.RuntimeInfo{
				ConfigVersion: jobPrevVersion,
			}, nil)

		suite.taskStore.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, jobPrevVersion).
			Return(&pbtask.TaskConfig{}, nil, nil)
	}

	suite.updateStore.EXPECT().
		ModifyUpdate(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateInfo *models.UpdateModel) {
			suite.Equal(updateInfo.GetState(), pbupdate.State_ROLLING_BACKWARD)
			suite.Equal(updateInfo.GetJobConfigVersion(), jobVersion+1)
			suite.Equal(updateInfo.GetPrevJobConfigVersion(), jobVersion)
			suite.Equal(updateInfo.GetInstancesDone(), uint32(0))
			suite.Equal(updateInfo.GetInstancesFailed(), uint32(0))
			suite.Empty(updateInfo.GetInstancesCurrent())
		}).Return(nil)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, jobVersion+1)
		}).
		Return(fmt.Errorf("test error"))

	suite.Error(suite.update.Rollback(context.Background()))

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: jobVersion,
		}, nil)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, jobVersion+1)
		}).
		Return(nil)

	suite.NoError(suite.update.Rollback(context.Background()))

	suite.Empty(suite.update.GetInstancesFailed())
	suite.Empty(suite.update.GetInstancesDone())
	suite.Empty(suite.update.GetInstancesCurrent())
	suite.Equal(suite.update.GetState().State, pbupdate.State_ROLLING_BACKWARD)
	suite.Equal(suite.update.GetInstancesRemoved(), []uint32{5, 6, 7, 8, 9})
	suite.Equal(suite.update.GetInstancesUpdated(), []uint32{0, 1, 2, 3, 4})
}

func (suite *UpdateTestSuite) TestUpdateRollbackRollingBackwardUpdate() {
	jobVersion := uint64(1)

	suite.update.state = pbupdate.State_ROLLING_BACKWARD
	suite.update.jobVersion = jobVersion
	suite.update.jobID = suite.jobID

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: jobVersion,
		}, nil)

	suite.NoError(suite.update.Rollback(context.Background()))
}

// TestUpdateRollbackFailsToGetPrevConfig tests rollback fails
// to get previous config
func (suite *UpdateTestSuite) TestUpdateRollbackFailsToGetPrevConfig() {
	jobPrevVersion := uint64(1)
	jobVersion := uint64(2)

	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.update.jobID = suite.jobID
	suite.update.jobVersion = jobVersion
	suite.update.jobPrevVersion = jobPrevVersion

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), suite.jobID, jobPrevVersion).
		Return(nil, nil, fmt.Errorf("test error"))

	suite.Error(suite.update.Rollback(context.Background()))
}

// TestUpdateRollbackFailToGetCurrentConfig tests rollback fails due to
// fail to get current config
func (suite *UpdateTestSuite) TestUpdateRollbackFailToGetCurrentConfig() {
	jobPrevVersion := uint64(1)
	jobPrevConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: jobPrevVersion,
		},
		InstanceCount: 5,
	}
	jobVersion := uint64(2)
	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.update.jobID = suite.jobID
	suite.update.jobVersion = jobVersion
	suite.update.jobPrevVersion = jobPrevVersion

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: jobVersion,
		}, nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), suite.jobID, jobPrevVersion).
		Return(jobPrevConfig, nil, nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(nil, nil, fmt.Errorf("test error"))

	suite.Error(suite.update.Rollback(context.Background()))
}

// TestUpdateRollbackFailsToUpdateConfig tests rollback fails due to fail
// to update config
func (suite *UpdateTestSuite) TestUpdateRollbackFailsToUpdateConfig() {
	jobPrevVersion := uint64(1)
	jobPrevConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: jobPrevVersion,
		},
		InstanceCount: 5,
	}

	jobVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: jobVersion,
		},
		InstanceCount: 10,
	}
	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.update.jobID = suite.jobID
	suite.update.jobVersion = jobVersion
	suite.update.jobPrevVersion = jobPrevVersion

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: jobVersion,
		}, nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(jobConfig, nil, nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), suite.jobID, jobPrevVersion).
		Return(jobPrevConfig, nil, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(jobVersion, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, _ *models.ConfigAddOn) {
			suite.Equal(config.ChangeLog.Version, jobVersion+1)
		}).
		Return(fmt.Errorf("test error"))

	suite.Error(suite.update.Rollback(context.Background()))
}

// TestUpdateRollbackFailsDueToTaskConfigCreation tests rollback fails due
// to fail to create task config
func (suite *UpdateTestSuite) TestUpdateRollbackFailsDueToTaskConfigCreation() {
	jobPrevVersion := uint64(1)
	jobPrevConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: jobPrevVersion,
		},
		InstanceCount: 5,
	}

	jobVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: jobVersion,
		},
		InstanceCount: 10,
	}
	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.update.jobID = suite.jobID
	suite.update.jobVersion = jobVersion
	suite.update.jobPrevVersion = jobPrevVersion

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: jobVersion,
		}, nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(jobConfig, nil, nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(gomock.Any(), suite.jobID, jobPrevVersion).
		Return(jobPrevConfig, nil, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(jobVersion, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, _ *models.ConfigAddOn) {
			suite.Equal(config.ChangeLog.Version, jobVersion+1)
		}).
		Return(nil)

	suite.taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), suite.jobID, jobPrevConfig, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, jobConfig *pbjob.JobConfig, _ *models.ConfigAddOn) {
			// version number should increase
			suite.Equal(jobConfig.GetChangeLog().GetVersion(), jobVersion+1)
		}).
		Return(fmt.Errorf("test error"))

	suite.Error(suite.update.Rollback(context.Background()))
}

// TestGetInstancesToProcessForUpdateWithLabelAddAndUpdate tests
// GetInstancesToProcessForUpdate, when config have labels added and updated
func (suite *UpdateTestSuite) TestGetInstancesToProcessForUpdateWithLabelAddAndUpdate() {
	instanceCount := uint32(10)
	prevJobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{"key1", "val1"},
			{"key2", "val2"},
		},
	}
	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{"key1", "val1"},
			{"key2", "val2-1"},
			{"key3", "val3"},
		},
	}

	instancesAdded, instancesUpdated, instancesRemoved, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			&job{},
			prevJobConfig,
			jobConfig,
			suite.taskStore)
	suite.NoError(err)
	suite.Len(instancesUpdated, int(instanceCount))
	suite.Empty(instancesRemoved)
	suite.Empty(instancesAdded)
}

// TestGetInstancesToProcessForUpdateWithLabelUpdated tests
// GetInstancesToProcessForUpdate, when config have labels updated
func (suite *UpdateTestSuite) TestGetInstancesToProcessForUpdateWithLabelUpdated() {
	instanceCount := uint32(10)
	prevJobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{"key1", "val1"},
			{"key2", "val2"},
		},
	}
	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{"key1", "val1"},
			{"key2", "val2-1"},
		},
	}

	instancesAdded, instancesUpdated, instancesRemoved, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			&job{},
			prevJobConfig,
			jobConfig,
			suite.taskStore)
	suite.NoError(err)
	suite.Len(instancesUpdated, int(instanceCount))
	suite.Empty(instancesRemoved)
	suite.Empty(instancesAdded)
}

func (suite *UpdateTestSuite) TestGetInstancesToProcessForUpdateWithAddedInstances() {
	commandValue := "entrypoint.sh"
	taskConfig := &pbtask.TaskConfig{
		Command: &mesosv1.CommandInfo{
			Value: &commandValue,
		},
	}
	testJob := &job{
		runtime: &pbjob.RuntimeInfo{
			Revision: &peloton.ChangeLog{
				Version: 1,
			},
		},
		jobFactory: &jobFactory{
			jobStore:  suite.jobStore,
			taskStore: suite.taskStore,
		},
		tasks: make(map[uint32]*task),
	}
	prevInstanceCount := uint32(10)
	prevJobConfig := &pbjob.JobConfig{
		InstanceCount: prevInstanceCount,
		DefaultConfig: taskConfig,
	}
	instanceCount := uint32(22)
	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		DefaultConfig: taskConfig,
	}

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&pbtask.RuntimeInfo{}, nil).
		AnyTimes()

	suite.taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(taskConfig, nil, nil).
		AnyTimes()

	instancesAdded, instancesUpdated, instancesRemoved, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob,
			prevJobConfig,
			jobConfig,
			suite.taskStore)
	suite.NoError(err)
	suite.Len(instancesAdded, int(instanceCount-prevInstanceCount))
	suite.Empty(instancesRemoved)
	suite.Empty(instancesUpdated)
}
