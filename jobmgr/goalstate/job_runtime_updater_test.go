package goalstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/uber-go/tally"
)

const (
	jobStartTime      = "2017-01-02T15:04:05.456789016Z"
	jobCompletionTime = "2017-01-03T18:04:05.987654447Z"
)

type JobRuntimeUpdaterTestSuite struct {
	suite.Suite

	ctrl                *gomock.Controller
	jobStore            *store_mocks.MockJobStore
	taskStore           *store_mocks.MockTaskStore
	jobGoalStateEngine  *goalstatemocks.MockEngine
	taskGoalStateEngine *goalstatemocks.MockEngine
	jobFactory          *cachedmocks.MockJobFactory
	cachedJob           *cachedmocks.MockJob
	cachedTask          *cachedmocks.MockTask
	goalStateDriver     *driver
	resmgrClient        *res_mocks.MockResourceManagerServiceYARPCClient
	jobID               *peloton.JobID
	jobEnt              *jobEntity
}

func TestJobRuntimeUpdater(t *testing.T) {
	suite.Run(t, new(JobRuntimeUpdaterTestSuite))
}

func (suite *JobRuntimeUpdaterTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobStore = store_mocks.NewMockJobStore(suite.ctrl)
	suite.taskStore = store_mocks.NewMockTaskStore(suite.ctrl)

	suite.resmgrClient = res_mocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
	suite.goalStateDriver = &driver{
		jobEngine:    suite.jobGoalStateEngine,
		taskEngine:   suite.taskGoalStateEngine,
		jobStore:     suite.jobStore,
		taskStore:    suite.taskStore,
		jobFactory:   suite.jobFactory,
		resmgrClient: suite.resmgrClient,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.jobEnt = &jobEntity{
		id:     suite.jobID,
		driver: suite.goalStateDriver,
	}

	suite.goalStateDriver.cfg.normalize()
}

func (suite *JobRuntimeUpdaterTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// Verify that completion time of a completed job shouldn't be empty.
func (suite *JobRuntimeUpdaterTestSuite) TestJobCompletionTimeNotEmpty() {
	instanceCount := uint32(100)
	jobRuntime := pb_job.RuntimeInfo{
		State:     pb_job.JobState_KILLED,
		GoalState: pb_job.JobState_SUCCEEDED,
	}
	startTime, _ := time.Parse(time.RFC3339Nano, jobStartTime)
	startTimeUnix := float64(startTime.UnixNano()) / float64(time.Second/time.Nanosecond)

	// Simulate KILLED job which never ran
	stateCounts := make(map[string]uint32)
	stateCounts[pb_task.TaskState_KILLED.String()] = instanceCount

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&jobRuntime, nil)

	suite.cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	suite.taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), suite.jobID).
		Return(stateCounts, nil)

	suite.cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	// Because the job never ran, GetLastTaskUpdateTime will return 0
	// Mock it to return 0 here
	suite.cachedJob.EXPECT().
		GetLastTaskUpdateTime().
		Return(float64(0))

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			suite.NotEqual(runtime.CompletionTime, "")
		}).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()
	err := JobRuntimeUpdater(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

func (suite *JobRuntimeUpdaterTestSuite) TestJobUpdateJobRuntime() {
	instanceCount := uint32(100)
	jobRuntime := pb_job.RuntimeInfo{
		State:     pb_job.JobState_PENDING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	// Simulate RUNNING job
	stateCounts := make(map[string]uint32)
	stateCounts[pb_task.TaskState_PENDING.String()] = instanceCount / 4
	stateCounts[pb_task.TaskState_RUNNING.String()] = instanceCount / 4
	stateCounts[pb_task.TaskState_LAUNCHED.String()] = instanceCount / 4
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 4

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&jobRuntime, nil)

	suite.cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	suite.taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), suite.jobID).
		Return(stateCounts, nil)

	suite.cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(float64(0))

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			suite.Equal(runtime.State, pb_job.JobState_RUNNING)
			suite.Equal(runtime.TaskStats[pb_task.TaskState_PENDING.String()], instanceCount/4)
			suite.Equal(runtime.TaskStats[pb_task.TaskState_RUNNING.String()], instanceCount/4)
			suite.Equal(runtime.TaskStats[pb_task.TaskState_LAUNCHED.String()], instanceCount/4)
			suite.Equal(runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/4)
		}).
		Return(nil)

	err := JobRuntimeUpdater(context.Background(), suite.jobEnt)
	suite.NoError(err)

	// Simulate SUCCEEDED job
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount

	startTime, _ := time.Parse(time.RFC3339Nano, jobStartTime)
	startTimeUnix := float64(startTime.UnixNano()) / float64(time.Second/time.Nanosecond)
	endTime, _ := time.Parse(time.RFC3339Nano, jobCompletionTime)
	endTimeUnix := float64(endTime.UnixNano()) / float64(time.Second/time.Nanosecond)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&jobRuntime, nil)

	suite.cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	suite.taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), suite.jobID).
		Return(stateCounts, nil)

	suite.cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	suite.cachedJob.EXPECT().
		GetLastTaskUpdateTime().
		Return(endTimeUnix)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			suite.Equal(runtime.State, pb_job.JobState_SUCCEEDED)
			suite.Equal(runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount)
			suite.Equal(runtime.StartTime, jobStartTime)
			suite.Equal(runtime.CompletionTime, jobCompletionTime)
		}).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err = JobRuntimeUpdater(context.Background(), suite.jobEnt)
	suite.NoError(err)

	// Simulate PENDING job
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_PENDING.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 2

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&jobRuntime, nil)

	suite.cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	suite.taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), suite.jobID).
		Return(stateCounts, nil)

	suite.cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			suite.Equal(runtime.State, pb_job.JobState_PENDING)
			suite.Equal(runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/2)
			suite.Equal(runtime.TaskStats[pb_task.TaskState_PENDING.String()], instanceCount/2)
		}).
		Return(nil)

	err = JobRuntimeUpdater(context.Background(), suite.jobEnt)
	suite.NoError(err)

	// Simulate FAILED job
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_FAILED.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 2

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&jobRuntime, nil)

	suite.cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	suite.taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), suite.jobID).
		Return(stateCounts, nil)

	suite.cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	suite.cachedJob.EXPECT().
		GetLastTaskUpdateTime().
		Return(endTimeUnix)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			suite.Equal(runtime.State, pb_job.JobState_FAILED)
			suite.Equal(runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/2)
			suite.Equal(runtime.TaskStats[pb_task.TaskState_FAILED.String()], instanceCount/2)
		}).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err = JobRuntimeUpdater(context.Background(), suite.jobEnt)
	suite.NoError(err)

	// Simulate KILLED job
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_FAILED.String()] = instanceCount / 4
	stateCounts[pb_task.TaskState_KILLED.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 4

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&jobRuntime, nil)

	suite.cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	suite.taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), suite.jobID).
		Return(stateCounts, nil)

	suite.cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	suite.cachedJob.EXPECT().
		GetLastTaskUpdateTime().
		Return(endTimeUnix)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			suite.Equal(runtime.State, pb_job.JobState_KILLED)
			suite.Equal(runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/4)
			suite.Equal(runtime.TaskStats[pb_task.TaskState_FAILED.String()], instanceCount/4)
			suite.Equal(runtime.TaskStats[pb_task.TaskState_KILLED.String()], instanceCount/2)
		}).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err = JobRuntimeUpdater(context.Background(), suite.jobEnt)
	suite.NoError(err)

	// Simulate INITIALIZED job
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 2
	jobRuntime = pb_job.RuntimeInfo{
		State:     pb_job.JobState_INITIALIZED,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&jobRuntime, nil)

	suite.cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	suite.taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), suite.jobID).
		Return(stateCounts, nil)

	suite.cachedJob.EXPECT().
		IsPartiallyCreated().
		Return(true).AnyTimes()

	suite.cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			suite.Equal(runtime.State, pb_job.JobState_INITIALIZED)
			suite.Equal(runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/2)
		}).
		Return(nil)

	err = JobRuntimeUpdater(context.Background(), suite.jobEnt)
	suite.Error(err)

	// Simulate fake DB error
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount
	jobRuntime = pb_job.RuntimeInfo{
		State:     pb_job.JobState_RUNNING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&jobRuntime, nil)

	suite.cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	suite.taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), suite.jobID).
		Return(stateCounts, nil)

	suite.cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	suite.cachedJob.EXPECT().
		GetLastTaskUpdateTime().
		Return(endTimeUnix)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err = JobRuntimeUpdater(context.Background(), suite.jobEnt)
	suite.Error(err)

	// Simulate SUCCEEDED job with correct task stats in runtime but incorrect state
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount
	jobRuntime = pb_job.RuntimeInfo{
		State:     pb_job.JobState_RUNNING,
		GoalState: pb_job.JobState_SUCCEEDED,
		TaskStats: stateCounts,
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&jobRuntime, nil)

	suite.cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	suite.taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), suite.jobID).
		Return(stateCounts, nil)

	suite.cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	suite.cachedJob.EXPECT().
		GetLastTaskUpdateTime().
		Return(endTimeUnix)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			suite.Equal(runtime.State, pb_job.JobState_SUCCEEDED)
		}).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err = JobRuntimeUpdater(context.Background(), suite.jobEnt)
	suite.NoError(err)

	// Simulate killed job with no tasks created
	stateCounts = make(map[string]uint32)
	jobRuntime = pb_job.RuntimeInfo{
		State:     pb_job.JobState_KILLED,
		GoalState: pb_job.JobState_KILLED,
		TaskStats: stateCounts,
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&jobRuntime, nil)

	suite.cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	suite.taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), suite.jobID).
		Return(stateCounts, nil)

	err = JobRuntimeUpdater(context.Background(), suite.jobEnt)
	suite.NoError(err)
}

func (suite *JobRuntimeUpdaterTestSuite) TestJobEvaluateMaxRunningInstances() {
	instanceCount := uint32(100)
	maxRunningInstances := uint32(10)
	jobConfig := pb_job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: instanceCount,
		Type:          pb_job.JobType_BATCH,
		Sla: &pb_job.SlaConfig{
			MaximumRunningInstances: maxRunningInstances,
		},
	}

	jobRuntime := pb_job.RuntimeInfo{
		State:     pb_job.JobState_RUNNING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	// Simulate RUNNING job
	stateCounts := make(map[string]uint32)
	stateCounts[pb_task.TaskState_INITIALIZED.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 2
	jobRuntime.TaskStats = stateCounts

	var initializedTasks []uint32
	for i := uint32(0); i < instanceCount/2; i++ {
		initializedTasks = append(initializedTasks, i)
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(&jobConfig, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&jobRuntime, nil)

	suite.taskStore.EXPECT().
		GetTaskIDsForJobAndState(gomock.Any(), suite.jobID, pb_task.TaskState_INITIALIZED.String()).
		Return(initializedTasks, nil)

	for i := uint32(0); i < jobConfig.Sla.MaximumRunningInstances; i++ {
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, gomock.Any()).
			Return(&pb_task.RuntimeInfo{
				State: pb_task.TaskState_INITIALIZED,
			}, nil)
		suite.cachedJob.EXPECT().
			GetTask(gomock.Any()).Return(suite.cachedTask)
		suite.taskGoalStateEngine.EXPECT().
			IsScheduled(gomock.Any()).
			Return(false)
	}

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(ctx context.Context, runtimes map[uint32]*pb_task.RuntimeInfo, req cached.UpdateRequest) {
			suite.Equal(uint32(len(runtimes)), jobConfig.Sla.MaximumRunningInstances)
			for _, runtime := range runtimes {
				suite.Equal(runtime.GetState(), pb_task.TaskState_PENDING)
			}
		}).
		Return(nil)

	err := JobEvaluateMaxRunningInstancesSLA(context.Background(), suite.jobEnt)
	suite.NoError(err)

	// Simulate when max running instances are already running
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_INITIALIZED.String()] = instanceCount - maxRunningInstances
	stateCounts[pb_task.TaskState_RUNNING.String()] = maxRunningInstances
	jobRuntime.TaskStats = stateCounts

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(&jobConfig, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&jobRuntime, nil)

	err = JobEvaluateMaxRunningInstancesSLA(context.Background(), suite.jobEnt)
	suite.NoError(err)

	// Simulate error when scheduled instances is greater than maximum running instances
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_INITIALIZED.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_RUNNING.String()] = instanceCount / 2
	jobRuntime.TaskStats = stateCounts

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(&jobConfig, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&jobRuntime, nil)

	err = JobEvaluateMaxRunningInstancesSLA(context.Background(), suite.jobEnt)
	suite.NoError(err)
}
