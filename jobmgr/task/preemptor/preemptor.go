package preemptor

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common/lifecycle"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
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

func (p *preemptor) preemptTasks(ctx context.Context, preemptionCandidates []*resmgr.PreemptionCandidate) error {
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

		runtime, err := cachedTask.GetRunTime(ctx)
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
			jobID, uint32(instanceID), runtime, preemptPolicy)
		runtimeDiff[jobmgrcommon.ReasonField] = task.GetReason().String()

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
	config, err := p.taskStore.GetTaskConfig(
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
// or restarted
func getRuntimeDiffForPreempt(
	jobID *peloton.JobID,
	instanceID uint32,
	taskRuntime *pbtask.RuntimeInfo,
	preemptPolicy *pbtask.PreemptionPolicy) jobmgrcommon.RuntimeDiff {
	runtimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.MessageField: "Preempting running task",
	}

	// kill the task if GetKillOnPreempt is true
	if preemptPolicy != nil && preemptPolicy.GetKillOnPreempt() {
		runtimeDiff[jobmgrcommon.GoalStateField] = pbtask.TaskState_KILLED
	} else {
		// otherwise restart the task
		prevRunID, err := util.ParseRunID(taskRuntime.GetMesosTaskId().GetValue())
		if err != nil {
			prevRunID = 0
		}
		runtimeDiff[jobmgrcommon.DesiredMesosTaskIDField] = util.CreateMesosTaskID(jobID, instanceID, prevRunID+1)
	}
	return runtimeDiff
}
