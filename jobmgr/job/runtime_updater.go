package job

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"peloton/api/job"
	"peloton/api/peloton"
	"peloton/api/task"
	pb_eventstream "peloton/private/eventstream"

	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/atomic"
)

// jobStateUpdateInterval is the interval at which the job update is checked
// and persisted
var jobStateUpdateInterval = 15 * time.Second

// checkAllJobsInterval is the interval at which all non-terminal jobs are checked
var checkAllJobsInterval = 1 * time.Hour

// NewJobRuntimeUpdater creates a new JobRuntimeUpdater
func NewJobRuntimeUpdater(jobStore storage.JobStore, taskStore storage.TaskStore) *RuntimeUpdater {
	updater := RuntimeUpdater{
		jobStore:           jobStore,
		taskStore:          taskStore,
		lastTaskUpdateTime: make(map[string]float64),
		taskUpdatedFlags:   make(map[string]bool),
	}
	t := time.NewTicker(jobStateUpdateInterval)
	go updater.updateJobStateLoop(t.C)
	return &updater
}

// RuntimeUpdater updates the job runtime states
type RuntimeUpdater struct {
	sync.Mutex

	// jobID -> last task update time
	lastTaskUpdateTime map[string]float64
	// jobID -> if task has updated
	taskUpdatedFlags map[string]bool
	started          atomic.Bool
	progress         atomic.Uint64

	jobStore  storage.JobStore
	taskStore storage.TaskStore

	lastCheckAllJobsTime time.Time

	//TODO: add metrics
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
		mesosTaskID := event.TaskStatus.GetTaskId().GetValue()
		taskID, err := util.ParseTaskIDFromMesosTaskID(mesosTaskID)
		if err != nil {
			log.WithError(err).
				WithField("mesos_task_id", mesosTaskID).
				Error("Failed to ParseTaskIDFromMesosTaskID")
			continue
		}
		jobID, _, err := util.ParseTaskID(taskID)
		if err != nil {
			log.WithError(err).
				WithField("task_id", taskID).
				Error("Failed to ParseTaskID")
			continue
		}
		// Mark the corresponding job as "taskUpdated", and track the update time
		j.taskUpdatedFlags[jobID] = true
		j.lastTaskUpdateTime[jobID] = *event.TaskStatus.Timestamp
		j.progress.Store(event.Offset)
	}
}

func (j *RuntimeUpdater) updateJobRuntime(jobID *peloton.JobID) error {
	log.WithField("job_id", jobID).
		Info("JobRuntimeUpdater updateJobState")

	// Read the job config and job runtime
	jobConfig, err := j.jobStore.GetJobConfig(jobID)
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

	jobRuntime, err := j.jobStore.GetJobRuntime(jobID)
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
		tasks, err := j.taskStore.GetTasksForJobAndState(jobID, state.String())
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

	seconds := int64(j.lastTaskUpdateTime[jobID.Value])
	nanoSec := int64((j.lastTaskUpdateTime[jobID.Value] - float64(seconds)) *
		float64(time.Second/time.Nanosecond))

	lastTaskUpdateTime := time.Time{}.
		Add(time.Duration(seconds) * time.Second).
		Add(time.Duration(nanoSec) * time.Nanosecond)

	// Decide the new job state from the task state counts
	if stateCounts[task.TaskState_SUCCEEDED.String()] == instances {
		jobState = job.JobState_SUCCEEDED
		jobRuntime.CompletionTime = lastTaskUpdateTime.String()
	} else if stateCounts[task.TaskState_SUCCEEDED.String()]+
		stateCounts[task.TaskState_FAILED.String()] == instances {
		jobState = job.JobState_FAILED
		jobRuntime.CompletionTime = lastTaskUpdateTime.String()
	} else if stateCounts[task.TaskState_KILLED.String()] > 0 &&
		(stateCounts[task.TaskState_SUCCEEDED.String()]+
			stateCounts[task.TaskState_FAILED.String()]+
			stateCounts[task.TaskState_KILLED.String()] == instances) {
		jobState = job.JobState_KILLED
		jobRuntime.CompletionTime = lastTaskUpdateTime.String()
	} else if stateCounts[task.TaskState_RUNNING.String()] > 0 {
		jobState = job.JobState_RUNNING
	} else if stateCounts[task.TaskState_PENDING.String()] > 0 {
		jobState = job.JobState_PENDING
	} else {
		jobState = job.JobState_INITIALIZED
	}

	jobRuntime.State = jobState
	jobRuntime.TaskStats = stateCounts

	// Update the job runtime
	err = j.jobStore.UpdateJobRuntime(jobID, jobRuntime)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to update jobRuntime")
		return err
	}
	return nil
}

func (j *RuntimeUpdater) updateJobsRuntime() {
	j.Lock()
	defer j.Unlock()

	log.Debug("JobRuntimeUpdater updateJobsRuntime")
	for jobID, taskUpdated := range j.taskUpdatedFlags {
		if taskUpdated && j.started.Load() {
			j.updateJobRuntime(&peloton.JobID{Value: jobID})
			delete(j.taskUpdatedFlags, jobID)
			delete(j.lastTaskUpdateTime, jobID)
		}
	}
}

// checkAllJobs would check and update all jobs that is not in terminal state
// every checkAllJobsInterval time
func (j *RuntimeUpdater) checkAllJobs() {
	j.Lock()
	defer j.Unlock()

	if time.Since(j.lastCheckAllJobsTime) < checkAllJobsInterval {
		return
	}
	log.Info("JobRuntimeUpdater checkAllJobs")
	j.lastCheckAllJobsTime = time.Now()

	nonTerminatedStates := []job.JobState{
		job.JobState_INITIALIZED,
		job.JobState_PENDING,
		job.JobState_RUNNING,
		job.JobState_UNKNOWN,
	}
	for _, state := range nonTerminatedStates {
		jobIDs, err := j.jobStore.GetJobsByState(state)
		if err != nil {
			log.WithError(err).
				WithField("state", state).
				Error("Failed to GetJobsByState")
			continue
		}
		for _, jobID := range jobIDs {
			j.updateJobRuntime(&jobID)
		}
	}
}

func (j *RuntimeUpdater) clear() {
	log.Info("jobRuntimeHolder cleared")

	j.taskUpdatedFlags = make(map[string]bool)
	j.lastTaskUpdateTime = make(map[string]float64)

}

func (j *RuntimeUpdater) updateJobStateLoop(c <-chan time.Time) {
	for range c {
		if j.started.Load() {
			j.updateJobsRuntime()
			j.checkAllJobs()
		}
	}
}

// Start starts processing status update events
func (j *RuntimeUpdater) Start() {
	j.Lock()
	defer j.Unlock()

	log.Info("JobRuntimeUpdater started")
	j.started.Store(true)
}

// Stop stops processing status update events
func (j *RuntimeUpdater) Stop() {
	j.Lock()
	defer j.Unlock()

	log.Info("JobRuntimeUpdater stopped")
	j.started.Store(false)
	j.clear()

}

// GetEventProgress returns the progress
func (j *RuntimeUpdater) GetEventProgress() uint64 {
	return j.progress.Load()
}
