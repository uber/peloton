package recovery

import (
	"context"
	"fmt"
	"sync"
	"testing"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/storage/cassandra"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

var csStore *cassandra.Store
var pendingJobID *peloton.JobID
var runningJobID *peloton.JobID
var failedJobID *peloton.JobID
var unknownJobID *peloton.JobID
var mutex = &sync.Mutex{}
var receivedPendingJobID []string

func init() {
	conf := cassandra.MigrateForTest()
	var err error
	csStore, err = cassandra.NewStore(conf, tally.NoopScope)
	if err != nil {
		log.Fatal(err)
	}
}

func createJob(ctx context.Context, state pb_job.JobState, goalState pb_job.JobState) (*peloton.JobID, error) {
	var jobID = &peloton.JobID{Value: uuid.New()}
	var sla = pb_job.SlaConfig{
		Priority:                22,
		MaximumRunningInstances: 3,
		Preemptible:             false,
	}
	var taskConfig = pb_task.TaskConfig{
		Resource: &pb_task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
	}
	var jobConfig = pb_job.JobConfig{
		Name:          "TestValidatorWithStore",
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "gsg9"},
		Sla:           &sla,
		DefaultConfig: &taskConfig,
		InstanceCount: 2,
	}

	err := csStore.CreateJob(ctx, jobID, &jobConfig, "gsg9")
	if err != nil {
		return nil, err
	}

	jobRuntime, err := csStore.GetJobRuntime(ctx, jobID)
	if err != nil {
		return nil, err
	}

	jobRuntime.State = state
	jobRuntime.GoalState = goalState
	err = csStore.UpdateJobRuntime(ctx, jobID, jobRuntime)
	if err != nil {
		return nil, err
	}

	return jobID, nil
}

func recoverPendingTask(ctx context.Context, jobID string, jobConfig *pb_job.JobConfig, jobRuntime *pb_job.RuntimeInfo, batch TasksBatch, errChan chan<- error) {
	var err error

	if jobID != pendingJobID.GetValue() {
		err = fmt.Errorf("Got the wrong job id")
	}
	errChan <- err
	return
}

func recoverRunningTask(ctx context.Context, jobID string, jobConfig *pb_job.JobConfig, jobRuntime *pb_job.RuntimeInfo, batch TasksBatch, errChan chan<- error) {
	var err error

	if jobID != runningJobID.GetValue() {
		err = fmt.Errorf("Got the wrong job id")
	}
	errChan <- err
	return
}

func recoverAllTask(ctx context.Context, jobID string, jobConfig *pb_job.JobConfig, jobRuntime *pb_job.RuntimeInfo, batch TasksBatch, errChan chan<- error) {
	mutex.Lock()
	receivedPendingJobID = append(receivedPendingJobID, jobID)
	mutex.Unlock()
	return
}

func TestJobRecoveryWithStore(t *testing.T) {
	var err error
	var jobStatesPending = []pb_job.JobState{
		pb_job.JobState_PENDING,
	}
	var jobStatesRunning = []pb_job.JobState{
		pb_job.JobState_RUNNING,
	}
	var jobStatesAll = []pb_job.JobState{
		pb_job.JobState_PENDING,
		pb_job.JobState_RUNNING,
		pb_job.JobState_FAILED,
	}

	ctx := context.Background()

	pendingJobID, err = createJob(ctx, pb_job.JobState_PENDING, pb_job.JobState_SUCCEEDED)
	assert.NoError(t, err)

	runningJobID, err = createJob(ctx, pb_job.JobState_RUNNING, pb_job.JobState_SUCCEEDED)
	assert.NoError(t, err)

	// this job should not be recovered
	failedJobID, err = createJob(ctx, pb_job.JobState_FAILED, pb_job.JobState_SUCCEEDED)
	assert.NoError(t, err)

	// this job should not be recovered
	unknownJobID, err = createJob(ctx, pb_job.JobState_FAILED, pb_job.JobState_UNKNOWN)
	assert.NoError(t, err)

	err = RecoverJobsByState(ctx, csStore, jobStatesPending, recoverPendingTask)
	assert.NoError(t, err)
	err = RecoverJobsByState(ctx, csStore, jobStatesRunning, recoverRunningTask)
	assert.NoError(t, err)
	err = RecoverJobsByState(ctx, csStore, jobStatesAll, recoverAllTask)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(receivedPendingJobID))
}
