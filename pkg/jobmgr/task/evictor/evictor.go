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

package evictor

import (
	"context"
	"sync"
	"time"

	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	"github.com/uber/peloton/pkg/jobmgr/goalstate"
	"github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

const (
	// Task runtime message that indicates task is being evicted.
	_msgEvictingRunningTask = "evicting running task"
)

type EvictionReason_Type int

const (
	// Invalid task eviction reason.
	EvictionReason_INVALID EvictionReason_Type = 0
	// Task is being evicted due to preemption
	EvictionReason_PREEMPTION EvictionReason_Type = 1
	// Task is being evicted due to host maintenance
	EvictionReason_HOST_MAINTENANCE EvictionReason_Type = 2
)

var EvictionReason_name = map[EvictionReason_Type]string{
	EvictionReason_INVALID:          "EvictionReason_INVALID",
	EvictionReason_PREEMPTION:       "EvictionReason_PREEMPTION",
	EvictionReason_HOST_MAINTENANCE: "EvictionReason_HOST_MAINTENANCE",
}

// String returns the string value of eviction reason type
func (er EvictionReason_Type) String() string {
	return EvictionReason_name[er]
}

// Config is Task evictor specific config
type Config struct {
	// EvictionPeriod is the period to check for tasks for eviction
	EvictionPeriod time.Duration `yaml:"eviction_period"`

	// EvictionDequeueLimit is the limit for the number of tasks evictor
	// dequeues from resource manager and host manager
	EvictionDequeueLimit int `yaml:"eviction_dequeue_limit"`

	// EvictionDequeueTimeout is the timeout value for
	// dequeuing tasks from resource manager and host manager
	EvictionDequeueTimeout int `yaml:"eviction_dequeue_timeout_ms"`
}

// Evictor defines the interface of task evictor which kills
// tasks from the preemption queue of resource manager and maintenance queue of
// host manager
type Evictor interface {
	// Start starts the task Evictor goroutines
	Start() error
	// Stop stops the task Evictor goroutines
	Stop() error
}

// evictor implements the Evictor interface
type evictor struct {
	resMgrClient    resmgrsvc.ResourceManagerServiceYARPCClient
	jobFactory      cached.JobFactory
	goalStateDriver goalstate.Driver
	lm              lifecyclemgr.Manager
	config          *Config
	metrics         *Metrics
	lifeCycle       lifecycle.LifeCycle // lifecycle manager
	taskConfigV2Ops ormobjects.TaskConfigV2Ops
}

var _timeoutFunctionCall = 120 * time.Second

// New create a new Task Evictor
func New(
	d *yarpc.Dispatcher,
	resMgrClientName string,
	ormStore *ormobjects.Store,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	hmVersion api.Version,
	config *Config,
	parent tally.Scope,
) Evictor {

	return &evictor{
		resMgrClient: resmgrsvc.NewResourceManagerServiceYARPCClient(
			d.ClientConfig(resMgrClientName),
		),
		jobFactory:      jobFactory,
		goalStateDriver: goalStateDriver,
		lm:              lifecyclemgr.New(hmVersion, d, parent),
		config:          config,
		metrics:         NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
		lifeCycle:       lifecycle.NewLifeCycle(),
		taskConfigV2Ops: ormobjects.NewTaskConfigV2Ops(ormStore),
	}
}

// Start starts Task Evictor
func (e *evictor) Start() error {
	if !e.lifeCycle.Start() {
		log.Warn("Task Evictor is already running, no action will be " +
			"performed")
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	evictorStarted := make(chan int, 1)
	preemptionThreadStarted := make(chan int, 1)
	hostMaintenanceThreadStarted := make(chan int, 1)

	go func() {
		defer wg.Done()

		ticker := time.NewTicker(e.config.EvictionPeriod)
		defer ticker.Stop()

		log.Info("Starting preemption thread of evictor")
		close(preemptionThreadStarted)
		for {
			select {
			case <-e.lifeCycle.StopCh():
				return
			case <-ticker.C:
				err := e.performPreemptionCycle()
				if err != nil {
					e.metrics.TaskEvictPreemptionFail.Inc(1)
					log.WithError(err).Error("preemption cycle failed")
					continue
				}
				e.metrics.TaskEvictPreemptionSuccess.Inc(1)
			}
		}
	}()

	go func() {
		defer wg.Done()

		ticker := time.NewTicker(e.config.EvictionPeriod)
		defer ticker.Stop()

		log.Info("Starting host maintenance thread of evictor")
		close(hostMaintenanceThreadStarted)
		for {
			select {
			case <-e.lifeCycle.StopCh():
				return
			case <-ticker.C:
				err := e.performHostMaintenanceCycle()
				if err != nil {
					e.metrics.TaskEvictHostMaintenanceFail.Inc(1)
					log.WithError(err).Error("host maintenance cycle failed")
					continue
				}
				e.metrics.TaskEvictHostMaintenanceSuccess.Inc(1)
			}
		}
	}()

	go func() {
		defer e.lifeCycle.StopComplete()

		<-preemptionThreadStarted
		<-hostMaintenanceThreadStarted
		close(evictorStarted)
		wg.Wait()
	}()

	<-evictorStarted
	log.Info("Task Evictor started")
	return nil
}

// Stop stops Task Evictor process
func (e *evictor) Stop() error {
	if !e.lifeCycle.Stop() {
		log.Warn("Task Evictor is already stopped, no action will be performed")
		return nil
	}

	log.Info("Stopping Task Evictor")

	// Wait for task evictor to be stopped
	e.lifeCycle.Wait()
	log.Info("Task Evictor Stopped")
	return nil
}

func (e *evictor) performPreemptionCycle() error {
	tasks, err := e.getTasksToPreempt()
	if err != nil {
		return errors.Wrapf(err, "jobmgr failed to get preemptible tasks")
	}

	if len(tasks) == 0 {
		// log a debug to make it not verbose
		log.Debug("No tasks to preempt")
		return nil
	}

	// TODO: remove the below translation once job manager polls host manager
	//  for tasks to be evicted for host maintenance
	taskIDsByEvictionReason := make(map[EvictionReason_Type][]string)
	for _, t := range tasks {
		var reason EvictionReason_Type
		switch t.Reason {
		case resmgr.PreemptionReason_PREEMPTION_REASON_UNKNOWN:
			reason = EvictionReason_INVALID
		case resmgr.PreemptionReason_PREEMPTION_REASON_HOST_MAINTENANCE:
			reason = EvictionReason_HOST_MAINTENANCE
		case resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES:
			reason = EvictionReason_PREEMPTION
		}

		taskIDsByEvictionReason[reason] = append(
			taskIDsByEvictionReason[reason],
			t.GetTaskId().GetValue(),
		)
	}

	// evict tasks
	var errs error
	for t := range EvictionReason_name {
		taskIDs := taskIDsByEvictionReason[t]
		if len(taskIDs) == 0 {
			continue
		}

		err = e.evictTasks(context.Background(), taskIDs, t)
		if err != nil {
			errs = multierror.Append(
				errs,
				errors.Wrapf(err, "failed to preempt some tasks"),
			)
		}
	}

	return errs
}

func (e *evictor) performHostMaintenanceCycle() error {
	tasks, err := e.getTasksOnDrainingHosts()
	if err != nil {
		return errors.Wrapf(err, "jobmgr failed to get tasks on draining hosts")
	}

	return e.evictTasks(context.Background(), tasks, EvictionReason_HOST_MAINTENANCE)
}

func (e *evictor) evictTasks(
	ctx context.Context,
	taskIDs []string,
	reason EvictionReason_Type,
) error {
	errs := new(multierror.Error)
	for _, id := range taskIDs {
		log.WithField("task_id", id).
			Info("evicting task")

		jobIDString, instanceID, err := util.ParseJobAndInstanceID(id)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		jobID := &peloton.JobID{Value: jobIDString}
		cachedJob := e.jobFactory.AddJob(jobID)
		cachedTask, err := cachedJob.AddTask(ctx, uint32(instanceID))
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		runtime, err := cachedTask.GetRuntime(ctx)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		// if the task-id of the task to be preempted is different from
		// that of the task in runtime, do not kill the task. This is because
		// the previous run of the task might already be killed and resource
		// manager might be slow to process the KILLED event.
		if runtime.GetMesosTaskId().GetValue() != id {
			continue
		}

		if runtime.GetGoalState() == pbtask.TaskState_KILLED {
			continue
		}

		preemptPolicy, err := e.getTaskPreemptionPolicy(
			ctx,
			jobID,
			uint32(instanceID),
			runtime.GetConfigVersion(),
		)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		runtimeDiff := getRuntimeDiffForPreempt(
			jobID,
			cachedJob.GetJobType(),
			uint32(instanceID),
			reason,
			runtime,
			preemptPolicy)

		// update the task and SLAInfo in cache and enqueue to goal state engine.
		// We do not need to handle the `instancesToBeRetried` here since the
		// task is being enqueued into the goalstate. The goalstate will reload
		// runtime into cache if needed. The task preemption will be retried
		// in the next preemption cycle.
		_, _, err = cachedJob.PatchTasks(
			ctx,
			map[uint32]jobmgrcommon.RuntimeDiff{uint32(instanceID): runtimeDiff},
			false,
		)
		if err != nil {
			errs = multierror.Append(errs, err)
		} else {
			e.goalStateDriver.EnqueueTask(jobID, uint32(instanceID), time.Now())
			goalstate.EnqueueJobWithDefaultDelay(
				jobID, e.goalStateDriver, cachedJob)
		}
	}
	return errs.ErrorOrNil()
}

func (e *evictor) getTasksToPreempt() ([]*resmgr.PreemptionCandidate, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), _timeoutFunctionCall)
	defer cancelFunc()

	request := &resmgrsvc.GetPreemptibleTasksRequest{
		Limit:   uint32(e.config.EvictionDequeueLimit),
		Timeout: uint32(e.config.EvictionDequeueTimeout),
	}

	callStart := time.Now()
	response, err := e.resMgrClient.GetPreemptibleTasks(ctx, request)
	callDuration := time.Since(callStart)

	if err != nil {
		return nil, err
	}

	if response.GetError() != nil {
		return nil, errors.New(response.GetError().String())
	}

	if len(response.GetPreemptionCandidates()) != 0 {
		log.WithFields(log.Fields{
			"num_tasks": len(response.PreemptionCandidates),
			"duration":  callDuration.Seconds(),
		}).Info("tasks to preempt")
	}

	e.metrics.GetPreemptibleTasksCallDuration.Record(callDuration)

	return response.GetPreemptionCandidates(), nil
}

func (e *evictor) getTasksOnDrainingHosts() ([]string, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), _timeoutFunctionCall)
	defer cancelFunc()

	callStart := time.Now()
	taskIDs, err := e.lm.GetTasksOnDrainingHosts(ctx,
		uint32(e.config.EvictionDequeueLimit),
		uint32(e.config.EvictionDequeueTimeout),
	)
	callDuration := time.Since(callStart)

	if err != nil {
		return nil, err
	}

	if len(taskIDs) != 0 {
		log.WithFields(log.Fields{
			"num_tasks": len(taskIDs),
			"duration":  callDuration.Seconds(),
		}).Info("tasks to evict for host maintenance")
	}

	e.metrics.GetTasksOnDrainingHostsCallDuration.Record(callDuration)

	return taskIDs, nil
}

// getTaskPreemptionPolicy returns the preempt policy config of a task
func (e *evictor) getTaskPreemptionPolicy(
	ctx context.Context,
	jobID *peloton.JobID,
	instanceID uint32,
	configVersion uint64) (*pbtask.PreemptionPolicy, error) {
	config, _, err := e.taskConfigV2Ops.GetTaskConfig(
		ctx,
		jobID,
		instanceID,
		configVersion)
	if err != nil {
		return nil, err
	}
	return config.GetPreemptionPolicy(), nil
}

// getRuntimeDiffForPreempt returns the RuntimeDiff to preempt a task.
// Given the preempt policy it decides whether the task should be killed
// or restarted.
func getRuntimeDiffForPreempt(
	jobID *peloton.JobID,
	jobType pbjob.JobType,
	instanceID uint32,
	evictReason EvictionReason_Type,
	taskRuntime *pbtask.RuntimeInfo,
	preemptPolicy *pbtask.PreemptionPolicy,
) jobmgrcommon.RuntimeDiff {

	tsReason := pbtask.TerminationStatus_TERMINATION_STATUS_REASON_INVALID
	switch evictReason {
	case EvictionReason_HOST_MAINTENANCE:
		tsReason = pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE
	case EvictionReason_PREEMPTION:
		tsReason = pbtask.TerminationStatus_TERMINATION_STATUS_REASON_PREEMPTED_RESOURCES
	}

	runtimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.MessageField: _msgEvictingRunningTask,
		jobmgrcommon.ReasonField:  evictReason.String(),
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: tsReason,
		},
	}

	if preemptPolicy.GetKillOnPreempt() {
		if jobType == pbjob.JobType_BATCH {
			// If its a batch and preemption policy is set to kill then we set the
			// goal state to preempting. This is a hack for spark.
			// TaskState_PREEMPTING should not be used as a goalstate.
			runtimeDiff[jobmgrcommon.GoalStateField] = pbtask.TaskState_PREEMPTING
		} else {
			// kill the task if GetKillOnPreempt is true
			runtimeDiff[jobmgrcommon.GoalStateField] = pbtask.TaskState_KILLED
		}

		return runtimeDiff
	}

	// otherwise restart the task
	prevRunID, err := util.ParseRunID(taskRuntime.GetMesosTaskId().GetValue())
	if err != nil {
		prevRunID = 0
	}

	runtimeDiff[jobmgrcommon.DesiredMesosTaskIDField] = util.CreateMesosTaskID(
		jobID,
		instanceID,
		prevRunID+1,
	)

	return runtimeDiff
}
