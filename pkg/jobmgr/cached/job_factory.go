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

package cached

import (
	"sync"
	"time"

	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	v1peloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/storage"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// JobFactory is the entrypoint object into the cache which stores job and tasks.
// This only runs in the job manager leader.
type JobFactory interface {
	// AddJob will create a Job if not present in cache,
	// else returns the current cached Job.
	AddJob(id *peloton.JobID) Job

	// ClearJob cleans up the job from the cache.
	ClearJob(jobID *peloton.JobID)

	// GetJob will return the current cached Job,
	// and nil if currently not in cache.
	GetJob(id *peloton.JobID) Job

	// GetAllJobs returns the list of all jobs in cache.
	GetAllJobs() map[string]Job

	// Start emitting metrics.
	Start()

	// Stop clears the current jobs and tasks in cache, stops metrics.
	Stop()
}

type jobFactory struct {
	sync.RWMutex //  Mutex to acquire before accessing any variables in the job factory object

	// map of active jobs (job identifier -> cache job object) in the system
	jobs               map[string]*job
	running            bool                          // whether job factory is running
	jobStore           storage.JobStore              // storage job store object
	taskStore          storage.TaskStore             // storage task store object
	updateStore        storage.UpdateStore           // storage update store object
	volumeStore        storage.PersistentVolumeStore // storage volume store object
	activeJobsOps      ormobjects.ActiveJobsOps      // DB ops for active_jobs table
	jobIndexOps        ormobjects.JobIndexOps        // DB ops for job_index table
	jobConfigOps       ormobjects.JobConfigOps       // DB ops for job_config table
	jobRuntimeOps      ormobjects.JobRuntimeOps      // DB ops for job_runtime table
	jobNameToIDOps     ormobjects.JobNameToIDOps     // DB ops for job_name_to_id table
	jobUpdateEventsOps ormobjects.JobUpdateEventsOps // DB ops for job_update_events table
	taskConfigV2Ops    ormobjects.TaskConfigV2Ops    // DB ops for task_config_v2 table
	mtx                *Metrics                      // cache metrics
	taskMetrics        *TaskMetrics                  // task metrics
	// Job/task listeners. This list is immutable after object is created.
	// So it can read without a lock.
	listeners []JobTaskListener
	// channel to indicate that the job factory needs to stop
	stopChan chan struct{}
}

// InitJobFactory initializes the job factory object.
func InitJobFactory(
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	updateStore storage.UpdateStore,
	volumeStore storage.PersistentVolumeStore,
	ormStore *ormobjects.Store,
	parentScope tally.Scope,
	listeners []JobTaskListener,
) JobFactory {
	return &jobFactory{
		jobs:               map[string]*job{},
		jobStore:           jobStore,
		taskStore:          taskStore,
		updateStore:        updateStore,
		volumeStore:        volumeStore,
		activeJobsOps:      ormobjects.NewActiveJobsOps(ormStore),
		jobIndexOps:        ormobjects.NewJobIndexOps(ormStore),
		jobConfigOps:       ormobjects.NewJobConfigOps(ormStore),
		jobRuntimeOps:      ormobjects.NewJobRuntimeOps(ormStore),
		jobNameToIDOps:     ormobjects.NewJobNameToIDOps(ormStore),
		jobUpdateEventsOps: ormobjects.NewJobUpdateEventsOps(ormStore),
		taskConfigV2Ops:    ormobjects.NewTaskConfigV2Ops(ormStore),
		mtx:                NewMetrics(parentScope.SubScope("cache")),
		taskMetrics:        NewTaskMetrics(parentScope.SubScope("task")),
		listeners:          listeners,
	}
}

func (f *jobFactory) AddJob(id *peloton.JobID) Job {
	if j := f.GetJob(id); j != nil {
		return j
	}

	f.Lock()
	defer f.Unlock()
	// check whether the job exists again, in case it
	// is created between RUnlock and Lock
	j, ok := f.jobs[id.GetValue()]
	if !ok {
		j = newJob(id, f)
		f.jobs[id.GetValue()] = j
	}

	return j
}

// ClearJob removes the job and all it tasks from inventory
func (f *jobFactory) ClearJob(id *peloton.JobID) {
	j := f.GetJob(id)
	if j == nil {
		return
	}

	f.Lock()
	defer f.Unlock()
	delete(f.jobs, j.ID().GetValue())
}

func (f *jobFactory) GetJob(id *peloton.JobID) Job {
	f.RLock()
	defer f.RUnlock()

	if j, ok := f.jobs[id.GetValue()]; ok {
		return j
	}

	return nil
}

func (f *jobFactory) GetAllJobs() map[string]Job {
	f.RLock()
	defer f.RUnlock()

	jobMap := make(map[string]Job)
	for k, v := range f.jobs {
		jobMap[k] = v
	}
	return jobMap
}

// Start the job factory, starts emitting metrics.
func (f *jobFactory) Start() {
	f.Lock()
	defer f.Unlock()

	if f.running {
		return
	}
	f.running = true

	f.stopChan = make(chan struct{})
	go f.runPublishMetrics(f.stopChan)
	log.Info("job factory started")
}

// Stop clears the current jobs and tasks in cache, stops emitting metrics.
func (f *jobFactory) Stop() {
	f.Lock()
	defer f.Unlock()

	// Do not do anything if not runnning
	if !f.running {
		log.Info("job factory stopped")
		return
	}

	f.running = false
	f.jobs = map[string]*job{}
	close(f.stopChan)
	log.Info("job factory stopped")
}

//TODO Refactor to remove the metrics loop into a separate component.
// JobFactory should only implement an interface like MetricsProvides
// to periodically publish metrics instead of having its own go routine.
// runPublishMetrics is the entrypoint to start and stop publishing cache metrics
func (f *jobFactory) runPublishMetrics(stopChan <-chan struct{}) {
	ticker := time.NewTicker(_defaultMetricsUpdateTick)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.publishMetrics()
		case <-stopChan:
			return
		}
	}
}

// publishMetrics is the routine which publishes cache metrics to M3
// return state count for test purpose
func (f *jobFactory) publishMetrics() map[pbtask.TaskState]map[pbtask.TaskState]int {
	stopWatch := f.mtx.scope.Timer("publish_duration").Start()
	defer stopWatch.Stop()

	// Initialise tasks count map for all possible pairs of (state, goal_state)
	tCount := map[pbtask.TaskState]map[pbtask.TaskState]int{}
	for s := range pbtask.TaskState_name {
		tCount[pbtask.TaskState(s)] = map[pbtask.TaskState]int{}
		for gs := range pbtask.TaskState_name {
			tCount[pbtask.TaskState(s)][pbtask.TaskState(gs)] = 0
		}
	}

	// Initialize update count map for all states
	workflowCount := map[pbupdate.State]int{}
	for s := range pbupdate.State_name {
		workflowCount[pbupdate.State(s)] = 0
	}

	// Iterate through jobs, tasks and count
	jobs := f.GetAllJobs()
	var (
		totalThrottledTasks int
		spreadQuotientSum   float64
		spreadQuotientCount int64
		slaViolatedJobIDs   []string
	)
	for _, j := range jobs {
		unavailableInstances := uint32(0)
		unknownInstances := uint32(0)

		taskStateCount, throttledTasks, spread := j.GetTaskStateCount()
		for stateSummary, count := range taskStateCount {
			currentState := stateSummary.CurrentState
			goalState := stateSummary.GoalState
			healthState := stateSummary.HealthState

			tCount[currentState][goalState] += count

			if currentState == pbtask.TaskState_UNKNOWN ||
				goalState == pbtask.TaskState_UNKNOWN {
				unknownInstances = unknownInstances + uint32(count)
				continue
			}

			if goalState == pbtask.TaskState_RUNNING {
				if currentState == pbtask.TaskState_RUNNING {
					switch healthState {
					case pbtask.HealthState_DISABLED, pbtask.HealthState_HEALTHY:
						continue
					}
				}
				unavailableInstances = unavailableInstances + uint32(count)
			}
		}

		totalThrottledTasks = totalThrottledTasks + throttledTasks

		workflowStateCount := j.GetWorkflowStateCount()
		for currentState, count := range workflowStateCount {
			workflowCount[currentState] += count
		}

		if spread.hostCount > 0 {
			spreadQuotientCount++
			spreadQuotientSum +=
				(float64(spread.taskCount) / float64(spread.hostCount))
		}

		// SLA is currently defined only for stateless jobs
		if j.GetJobType() != pbjob.JobType_SERVICE {
			continue
		}

		jobConfig := j.GetCachedConfig()
		if jobConfig == nil {
			log.WithField("job_id", j.ID().GetValue()).
				Debug("job config not present in cache, skipping SLA metrics for job")
			continue
		}

		if unknownInstances > 0 {
			log.WithFields(log.Fields{
				"job_id":                j.ID().GetValue(),
				"num_unknown_instances": unknownInstances,
			}).Debug("job has instances in unknown state")
		}

		var hasActiveUpdate bool
		for _, w := range j.GetAllWorkflows() {
			if w.GetWorkflowType() == models.WorkflowType_UPDATE &&
				IsUpdateStateActive(w.GetState().State) {
				hasActiveUpdate = true
				break
			}
		}

		// skip check for SLA violation if job has an ongoing update
		if hasActiveUpdate {
			continue
		}

		if jobConfig.GetSLA().GetMaximumUnavailableInstances() > 0 &&
			unavailableInstances > jobConfig.GetSLA().GetMaximumUnavailableInstances() {
			log.WithField("job_id", j.ID().GetValue()).
				Info("job sla violated")
			slaViolatedJobIDs = append(slaViolatedJobIDs, j.ID().GetValue())
		}
	}

	// Publish
	f.mtx.scope.Gauge("jobs_count").Update(float64(len(jobs)))
	f.mtx.scope.Gauge("sla_violated_jobs").Update(float64(len(slaViolatedJobIDs)))
	f.mtx.scope.Gauge("throttled_tasks").Update(float64(totalThrottledTasks))
	if spreadQuotientCount > 0 {
		f.taskMetrics.MeanSpreadQuotient.
			Update(spreadQuotientSum / float64(spreadQuotientCount))
	}
	for s, sm := range tCount {
		for gs, tc := range sm {
			f.mtx.scope.Tagged(map[string]string{"state": s.String(), "goal_state": gs.String()}).Gauge("tasks_count").Update(float64(tc))
		}
	}

	for s, tc := range workflowCount {
		f.mtx.scope.Tagged(map[string]string{"state": s.String()}).Gauge("workflow_count").Update(float64(tc))
	}

	return tCount
}

func (f *jobFactory) notifyJobSummaryChanged(
	jobID *peloton.JobID,
	jobType pbjob.JobType,
	jobSummary *pbjob.JobSummary,
	updateInfo *models.UpdateModel,
) {
	if jobSummary == nil {
		return
	}

	if jobType == pbjob.JobType_SERVICE {
		s := api.ConvertJobSummary(jobSummary, updateInfo)
		for _, l := range f.listeners {
			l.StatelessJobSummaryChanged(s)
		}
		// TODO add metric for listener execution latency
	}

	if jobType == pbjob.JobType_BATCH {
		for _, l := range f.listeners {
			l.BatchJobSummaryChanged(jobID, jobSummary)
		}
		// TODO add metric for listener execution latency
	}
}

func (f *jobFactory) notifyTaskRuntimeChanged(
	jobID *peloton.JobID,
	instanceID uint32,
	jobType pbjob.JobType,
	runtime *pbtask.RuntimeInfo,
	labels []*peloton.Label,
) {
	if runtime != nil {
		summary := &pod.PodSummary{
			PodName: &v1peloton.PodName{
				Value: util.CreatePelotonTaskID(jobID.GetValue(), instanceID),
			},
			Status: api.ConvertTaskRuntimeToPodStatus(runtime),
		}
		for _, l := range f.listeners {
			l.PodSummaryChanged(jobType, summary, api.ConvertLabels(labels))
		}
		// TODO add metric for listener execution latency
	}
}
