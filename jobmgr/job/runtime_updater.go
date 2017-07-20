package job

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"
)

// jobStateUpdateInterval is the interval at which the job update is checked
// and persisted
var jobStateUpdateInterval = 15 * time.Second

// checkAllJobsInterval is the interval at which all non-terminal jobs are checked
var checkAllJobsInterval = 1 * time.Hour

// taskStatesAfterStart is the set of Peloton task states which
// indicate a task is being or has already been started.
var taskStatesAfterStart = []task.TaskState{
	task.TaskState_STARTING,
	task.TaskState_RUNNING,
	task.TaskState_SUCCEEDED,
	task.TaskState_FAILED,
	task.TaskState_LOST,
	task.TaskState_PREEMPTING,
	task.TaskState_KILLING,
	task.TaskState_KILLED,
}

// NonTerminatedStates represents the non terminal states of a job
var NonTerminatedStates = map[job.JobState]bool{
	job.JobState_PENDING: true,
	job.JobState_RUNNING: true,
	job.JobState_UNKNOWN: true,
}

// NewJobRuntimeUpdater creates a new JobRuntimeUpdater
func NewJobRuntimeUpdater(
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	resmgrClient resmgrsvc.ResourceManagerServiceYARPCClient,
	parentScope tally.Scope) *RuntimeUpdater {
	// TODO: load firstTaskUpdateTime from DB after restart
	updater := RuntimeUpdater{
		jobStore:            jobStore,
		taskStore:           taskStore,
		firstTaskUpdateTime: make(map[string]float64),
		lastTaskUpdateTime:  make(map[string]float64),
		taskUpdatedFlags:    make(map[string]bool),
		metrics:             NewRuntimeUpdaterMetrics(parentScope.SubScope("runtime_updater")),
		jobRecovery: NewJobRecovery(
			jobStore,
			taskStore,
			resmgrClient,
			parentScope),
	}
	t := time.NewTicker(jobStateUpdateInterval)
	go updater.updateJobStateLoop(t.C)
	return &updater
}

// RuntimeUpdater updates the job runtime states
type RuntimeUpdater struct {
	sync.Mutex

	// jobID -> first task update time
	firstTaskUpdateTime map[string]float64
	// jobID -> last task update time
	lastTaskUpdateTime map[string]float64
	// jobID -> if task has updated
	taskUpdatedFlags map[string]bool
	started          atomic.Bool
	progress         atomic.Uint64

	jobStore  storage.JobStore
	taskStore storage.TaskStore

	lastCheckAllJobsTime time.Time

	jobRecovery *Recovery

	metrics *RuntimeUpdaterMetrics
}

// OnEvent callback
func (j *RuntimeUpdater) OnEvent(event *pb_eventstream.Event) {
	log.Error("Not implemented")
}

// OnEvents is the implementation of the event stream handler callback
func (j *RuntimeUpdater) OnEvents(events []*pb_eventstream.Event) {
	j.Lock()
	defer j.Unlock()

	for _, event := range events {
		mesosTaskID := event.MesosTaskStatus.GetTaskId().GetValue()
		jobID, _, err := util.ParseJobAndInstanceID(mesosTaskID)
		if err != nil {
			log.WithError(err).
				WithField("mesos_task_id", mesosTaskID).
				Error("Failed to ParseJobAndInstanceID")
			continue
		}
		// Mark the corresponding job as "taskUpdated", and track the update time
		j.taskUpdatedFlags[jobID] = true
		j.lastTaskUpdateTime[jobID] = *event.MesosTaskStatus.Timestamp
		if _, ok := j.firstTaskUpdateTime[jobID]; !ok {
			j.firstTaskUpdateTime[jobID] = *event.MesosTaskStatus.Timestamp
		}
		j.progress.Store(event.Offset)
	}
}

// formatTime converts a Unix timestamp to a string format of the
// given layout in UTC. See https://golang.org/pkg/time/ for possible
// time layout in golang. For example, it will return RFC3339 format
// string like 2017-01-02T11:00:00.123456789Z if the layout is
// time.RFC3339Nano
func formatTime(timestamp float64, layout string) string {
	seconds := int64(timestamp)
	nanoSec := int64((timestamp - float64(seconds)) *
		float64(time.Second/time.Nanosecond))
	return time.Unix(seconds, nanoSec).UTC().Format(layout)
}

// UpdateJob updates the job runtime synchronously
func (j *RuntimeUpdater) UpdateJob(ctx context.Context, jobID *peloton.JobID) error {
	j.Lock()
	defer j.Unlock()
	if j.started.Load() {
		err := j.updateJobRuntime(ctx, jobID)
		if err == nil {
			j.metrics.JobRuntimeUpdated.Inc(1)
			return err
		}
		j.metrics.JobRuntimeUpdateFailed.Inc(1)
		return nil
	}
	return errors.New("RuntimeUpdater has not started")
}

func (j *RuntimeUpdater) updateJobRuntime(ctx context.Context, jobID *peloton.JobID) error {
	log.WithField("job_id", jobID).
		Info("JobRuntimeUpdater updateJobRuntime")

	// Read the job config and job runtime
	jobConfig, err := j.jobStore.GetJobConfig(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to get jobConfig")
		return err
	}
	if jobConfig == nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Cannot find jobConfig")
		return fmt.Errorf("Cannot find jobConfig for %s", jobID.Value)
	}

	jobRuntime, err := j.jobStore.GetJobRuntime(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to get jobRuntime")
		return err
	}
	if jobRuntime == nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Cannot find jobRuntime")
		return fmt.Errorf("Cannot find jobRuntime for %s", jobID.Value)
	}

	var jobState job.JobState
	var instances = jobConfig.InstanceCount
	var stateCounts = make(map[string]uint32)

	// Build the per-task-state task count map for the job
	for _, stateVal := range task.TaskState_value {
		var state = task.TaskState(stateVal)
		// TODO: update GetTasksForJobAndState to return instanceID only
		tasks, err := j.taskStore.GetTasksForJobAndState(ctx, jobID, state.String())
		if err != nil {
			log.WithError(err).
				WithField("job_id", jobID).
				WithField("state", state).
				Error("Failed to GetTasksForJobAndState")
			return err
		}
		stateCounts[state.String()] = uint32(len(tasks))
	}

	if reflect.DeepEqual(stateCounts, jobRuntime.TaskStats) {
		log.WithField("job_id", jobID).
			WithField("task_stats", stateCounts).
			Debug("Task stats did not change, return")
		return nil
	}

	// Update job start time if necessary
	firstTaskUpdateTime, ok := j.firstTaskUpdateTime[jobID.Value]
	if ok && jobRuntime.StartTime == "" {
		count := uint32(0)
		for _, state := range taskStatesAfterStart {
			count += stateCounts[state.String()]
		}
		if count > 0 {
			jobRuntime.StartTime = formatTime(firstTaskUpdateTime, time.RFC3339Nano)
		}
	}

	// Decide the new job state from the task state counts
	lastTaskUpdateTime, ok := j.lastTaskUpdateTime[jobID.Value]
	completionTime := ""
	if ok {
		completionTime = formatTime(lastTaskUpdateTime, time.RFC3339Nano)
	}
	if stateCounts[task.TaskState_SUCCEEDED.String()] == instances {
		jobState = job.JobState_SUCCEEDED
		jobRuntime.CompletionTime = completionTime
		j.metrics.JobSucceeded.Inc(1)
	} else if stateCounts[task.TaskState_SUCCEEDED.String()]+
		stateCounts[task.TaskState_FAILED.String()] == instances {
		jobState = job.JobState_FAILED
		jobRuntime.CompletionTime = completionTime
		j.metrics.JobFailed.Inc(1)
	} else if stateCounts[task.TaskState_KILLED.String()] > 0 &&
		(stateCounts[task.TaskState_SUCCEEDED.String()]+
			stateCounts[task.TaskState_FAILED.String()]+
			stateCounts[task.TaskState_KILLED.String()] == instances) {
		jobState = job.JobState_KILLED
		jobRuntime.CompletionTime = completionTime
		j.metrics.JobKilled.Inc(1)
	} else if stateCounts[task.TaskState_RUNNING.String()] > 0 {
		jobState = job.JobState_RUNNING
	} else {
		jobState = job.JobState_PENDING
	}

	jobRuntime.State = jobState
	jobRuntime.TaskStats = stateCounts

	// Update the job runtime
	err = j.jobStore.UpdateJobRuntime(ctx, jobID, jobRuntime)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to update jobRuntime")
		return err
	}
	return nil
}

func (j *RuntimeUpdater) updateJobsRuntime(ctx context.Context) {
	j.Lock()
	defer j.Unlock()

	log.Debug("JobRuntimeUpdater updateJobsRuntime")
	for jobID, taskUpdated := range j.taskUpdatedFlags {
		if taskUpdated && j.started.Load() {
			j.updateJobRuntime(ctx, &peloton.JobID{Value: jobID})
			delete(j.taskUpdatedFlags, jobID)
			delete(j.firstTaskUpdateTime, jobID)
			delete(j.lastTaskUpdateTime, jobID)
		}
	}
}

// checkAllJobs would check and update all jobs that is not in terminal state
// every checkAllJobsInterval time
func (j *RuntimeUpdater) checkAllJobs(ctx context.Context) {
	j.Lock()
	defer j.Unlock()

	if time.Since(j.lastCheckAllJobsTime) < checkAllJobsInterval {
		return
	}
	log.Info("JobRuntimeUpdater checkAllJobs")
	j.lastCheckAllJobsTime = time.Now()

	nonTerminatedStates := []job.JobState{
		job.JobState_PENDING,
		job.JobState_RUNNING,
		job.JobState_UNKNOWN,
	}
	jobIDs, err := j.jobStore.GetJobsByStates(ctx, nonTerminatedStates)
	if err != nil {
		log.WithError(err).
			WithField("states", nonTerminatedStates).
			Error("Failed to GetJobsByStates")
	}
	for _, jobID := range jobIDs {
		err := j.updateJobRuntime(ctx, &jobID)
		if err == nil {
			j.metrics.JobRuntimeUpdated.Inc(1)
		} else {
			j.metrics.JobRuntimeUpdateFailed.Inc(1)
		}
	}
}

func (j *RuntimeUpdater) clear() {
	log.Info("jobRuntimeUpdater cleared")

	j.taskUpdatedFlags = make(map[string]bool)
	j.firstTaskUpdateTime = make(map[string]float64)
	j.lastTaskUpdateTime = make(map[string]float64)

}

func (j *RuntimeUpdater) updateJobStateLoop(c <-chan time.Time) {
	for range c {
		if j.started.Load() {
			// update job runtime based on taskUpdatedFlags
			j.updateJobsRuntime(context.Background())
			// Also scan all jobs and see if their runtime state need to be
			// updated, in case some task updates are lost
			j.checkAllJobs(context.Background())
			// jobRecovery is ran from time to time on the leader JobManager.
			// This is because all JM can fail and leave partially created jobs.
			// Thus the leader need to check from time to time to recover the
			// partially created jobs
			j.jobRecovery.recoverJobs(context.Background())
		}
	}
}

// Start starts processing status update events
func (j *RuntimeUpdater) Start() {
	j.Lock()
	defer j.Unlock()

	log.Info("JobRuntimeUpdater started")
	j.started.Store(true)
	j.metrics.IsLeader.Update(1.0)
}

// Stop stops processing status update events
func (j *RuntimeUpdater) Stop() {
	j.Lock()
	defer j.Unlock()

	log.Info("JobRuntimeUpdater stopped")
	j.started.Store(false)
	j.metrics.IsLeader.Update(0.0)
	j.clear()

}

// GetEventProgress returns the progress
func (j *RuntimeUpdater) GetEventProgress() uint64 {
	return j.progress.Load()
}
