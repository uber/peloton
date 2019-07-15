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

package preemptor

import (
	"context"
	"time"

	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	"github.com/uber/peloton/pkg/jobmgr/goalstate"
	"github.com/uber/peloton/pkg/storage"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

const (
	// Task runtime message that indicates task is being pre-empted.
	_msgPreemptingRunningTask = "Preempting running task"
)

// Config is Task preemptor specific config
type Config struct {
	// PreemptionPeriod is the period to check for tasks for preemption
	PreemptionPeriod time.Duration `yaml:"preemption_period"`

	// PreemptionDequeueLimit is the limit which task preemptor get the
	// tasks
	PreemptionDequeueLimit int `yaml:"preemption_dequeue_limit"`

	// DequeuePreemptionTimeout is the timeout value for task preemptor to
	// call GetPreemptibleTasks
	DequeuePreemptionTimeout int `yaml:"preemption_dequeue_timeout_ms"`
}

// Preemptor defines the interface of task preemptor which kills
// tasks from the preemption queue of resource manager
type Preemptor interface {
	// Start starts the task Preemptor goroutines
	Start() error
	// Stop stops the task Preemptor goroutines
	Stop() error
}

// preemptor implements the Preemptor interface
type preemptor struct {
	resMgrClient    resmgrsvc.ResourceManagerServiceYARPCClient
	taskStore       storage.TaskStore
	jobFactory      cached.JobFactory
	goalStateDriver goalstate.Driver
	config          *Config
	metrics         *Metrics
	lifeCycle       lifecycle.LifeCycle // lifecycle manager
}

var _timeoutFunctionCall = 120 * time.Second

// New create a new Task Preemptor
func New(
	d *yarpc.Dispatcher,
	resMgrClientName string,
	taskStore storage.TaskStore,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	config *Config,
	parent tally.Scope,
) Preemptor {

	return &preemptor{
		resMgrClient:    resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(resMgrClientName)),
		taskStore:       taskStore,
		jobFactory:      jobFactory,
		goalStateDriver: goalStateDriver,
		config:          config,
		metrics:         NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
		lifeCycle:       lifecycle.NewLifeCycle(),
	}
}

// Start starts Task Preemptor
func (p *preemptor) Start() error {
	if !p.lifeCycle.Start() {
		log.Warn("Task Preemptor is already running, no action will be " +
			"performed")
		return nil
	}

	started := make(chan int, 1)
	go func() {
		defer p.lifeCycle.StopComplete()

		ticker := time.NewTicker(p.config.PreemptionPeriod)
		defer ticker.Stop()

		log.Info("Starting Task Preemptor")
		close(started)
		for {
			select {
			case <-p.lifeCycle.StopCh():
				return
			case <-ticker.C:
				err := p.performPreemptionCycle()
				if err != nil {
					p.metrics.TaskPreemptFail.Inc(1)
					log.WithError(err).Error("preemption cycle failed")
					continue
				}
				p.metrics.TaskPreemptSuccess.Inc(1)
			}
		}
	}()
	<-started
	log.Info("Task Preemptor started")
	return nil
}

func (p *preemptor) performPreemptionCycle() error {
	tasks, err := p.getTasks()
	if err != nil {
		p.metrics.GetPreemptibleTasksFail.Inc(1)
		return errors.Wrapf(err, "jobmgr failed to get preemptible tasks")
	}
	p.metrics.GetPreemptibleTasks.Inc(1)

	if len(tasks) == 0 {
		// log a debug to make it not verbose
		log.Debug("No tasks to preempt")
		return nil
	}

	// preempt tasks
	err = p.preemptTasks(context.Background(), tasks)
	if err != nil {
		return errors.Wrapf(err, "failed to preempt some tasks")
	}
	return nil
}

func (p *preemptor) preemptTasks(
	ctx context.Context,
	preemptionCandidates []*resmgr.PreemptionCandidate,
) error {
	errs := new(multierror.Error)
	for _, task := range preemptionCandidates {
		log.WithField("task_ID", task.Id.Value).
			Info("preempting running task")

		id, instanceID, err := util.ParseTaskID(task.Id.Value)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		jobID := &peloton.JobID{Value: id}
		cachedJob := p.jobFactory.AddJob(jobID)
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

		if runtime.GetGoalState() == pbtask.TaskState_KILLED {
			continue
		}

		preemptPolicy, err := p.getTaskPreemptionPolicy(
			ctx, jobID, uint32(instanceID), runtime.GetConfigVersion())
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		runtimeDiff := getRuntimeDiffForPreempt(
			jobID,
			cachedJob.GetJobType(),
			uint32(instanceID),
			task.GetReason(),
			runtime,
			preemptPolicy)

		// update the task in cache and enqueue to goal state engine
		err = cachedJob.PatchTasks(ctx, map[uint32]jobmgrcommon.RuntimeDiff{uint32(instanceID): runtimeDiff})
		if err != nil {
			errs = multierror.Append(errs, err)
		} else {
			p.goalStateDriver.EnqueueTask(jobID, uint32(instanceID), time.Now())
			goalstate.EnqueueJobWithDefaultDelay(
				jobID, p.goalStateDriver, cachedJob)
		}
	}
	return errs.ErrorOrNil()
}

func (p *preemptor) getTasks() ([]*resmgr.PreemptionCandidate, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), _timeoutFunctionCall)
	defer cancelFunc()

	request := &resmgrsvc.GetPreemptibleTasksRequest{
		Limit:   uint32(p.config.PreemptionDequeueLimit),
		Timeout: uint32(p.config.DequeuePreemptionTimeout),
	}

	callStart := time.Now()
	response, err := p.resMgrClient.GetPreemptibleTasks(ctx, request)
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

	p.metrics.GetPreemptibleTasks.Inc(int64(len(response.PreemptionCandidates)))
	p.metrics.GetPreemptibleTasksCallDuration.Record(callDuration)

	return response.GetPreemptionCandidates(), nil
}

// Stop stops Task Preemptor process
func (p *preemptor) Stop() error {
	if !p.lifeCycle.Stop() {
		log.Warn("Task Preemptor is already stopped, no action will be performed")
		return nil
	}

	log.Info("Stopping Task Preemptor")

	// Wait for task preemptor to be stopped
	p.lifeCycle.Wait()
	log.Info("Task Preemptor Stopped")
	return nil
}

// getTaskPreemptionPolicy returns the preempt policy config of a task
func (p *preemptor) getTaskPreemptionPolicy(
	ctx context.Context,
	jobID *peloton.JobID,
	instanceID uint32,
	configVersion uint64) (*pbtask.PreemptionPolicy, error) {
	config, _, err := p.taskStore.GetTaskConfig(
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
	taskReason resmgr.PreemptionReason,
	taskRuntime *pbtask.RuntimeInfo,
	preemptPolicy *pbtask.PreemptionPolicy) jobmgrcommon.RuntimeDiff {
	runtimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.MessageField: _msgPreemptingRunningTask,
		jobmgrcommon.ReasonField:  taskReason.String(),
	}

	if preemptPolicy != nil && preemptPolicy.GetKillOnPreempt() {
		if jobType == pbjob.JobType_BATCH {
			// If its a batch and preemption policy is set to kill then we set the
			// goal state to preempting. This is a hack for spark.
			// TaskState_PREEMPTING should not be used as a goalstate.
			runtimeDiff[jobmgrcommon.GoalStateField] = pbtask.TaskState_PREEMPTING
		} else {
			// kill the task if GetKillOnPreempt is true
			runtimeDiff[jobmgrcommon.GoalStateField] = pbtask.TaskState_KILLED
		}
		tsReason := pbtask.TerminationStatus_TERMINATION_STATUS_REASON_INVALID
		switch taskReason {
		case resmgr.PreemptionReason_PREEMPTION_REASON_HOST_MAINTENANCE:
			tsReason = pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE
		case resmgr.PreemptionReason_PREEMPTION_REASON_REVOKE_RESOURCES:
			tsReason = pbtask.TerminationStatus_TERMINATION_STATUS_REASON_PREEMPTED_RESOURCES
		}
		runtimeDiff[jobmgrcommon.TerminationStatusField] =
			&pbtask.TerminationStatus{Reason: tsReason}
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
