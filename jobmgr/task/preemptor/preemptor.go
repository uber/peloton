package preemptor

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/storage"

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
	sync.Mutex

	resMgrClient   resmgrsvc.ResourceManagerServiceYARPCClient
	started        int32
	taskStore      storage.TaskStore
	trackedManager tracked.Manager
	config         *Config
	metrics        *Metrics
}

var _timeoutFunctionCall = 120 * time.Second

// New create a new Task Preemptor
func New(
	d *yarpc.Dispatcher,
	resMgrClientName string,
	taskStore storage.TaskStore,
	trackedManager tracked.Manager,
	config *Config,
	parent tally.Scope,
) Preemptor {

	return &preemptor{
		resMgrClient:   resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(resMgrClientName)),
		taskStore:      taskStore,
		trackedManager: trackedManager,
		config:         config,
		metrics:        NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
	}
}

// Start starts Task Preemptor
func (p *preemptor) Start() error {
	if atomic.CompareAndSwapInt32(&p.started, 0, 1) {
		log.Info("Starting Task Preemptor")
		go func() {
			ticker := time.NewTicker(p.config.PreemptionPeriod)
			defer ticker.Stop()

			for range ticker.C {
				if !p.isRunning() {
					break
				}
				err := p.performPreemptionCycle()
				if err != nil {
					p.metrics.TaskPreemptFail.Inc(1)
					log.WithError(err).Error("preemption cycle failed")
					continue
				}
				p.metrics.TaskPreemptSuccess.Inc(1)
			}
		}()
	}
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

func (p *preemptor) preemptTasks(ctx context.Context, tasks []*resmgr.Task) error {
	errs := new(multierror.Error)
	for _, task := range tasks {
		log.WithField("task_ID", task.Id.Value).
			Info("preempting running task")

		taskInfo, err := p.taskStore.GetTaskByID(ctx, task.Id.Value)
		if err != nil {
			errs = multierror.Append(errs, err)
		}

		// set goal state to TaskState_PREEMPTING
		taskInfo.GetRuntime().GoalState = pb_task.TaskState_PREEMPTING

		// update the task in the tracked manager
		err = p.trackedManager.UpdateTaskRuntime(ctx, taskInfo.JobId, taskInfo.InstanceId, taskInfo.Runtime)
		if err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	return errs.ErrorOrNil()
}

func (p *preemptor) getTasks() ([]*resmgr.Task, error) {
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

	if len(response.GetTasks()) != 0 {
		log.WithFields(log.Fields{
			"num_tasks": len(response.Tasks),
			"duration":  callDuration.Seconds(),
		}).Info("tasks to preempt")
	}

	p.metrics.GetPreemptibleTasks.Inc(int64(len(response.GetTasks())))
	p.metrics.GetPreemptibleTasksCallDuration.Record(callDuration)

	return response.GetTasks(), nil
}

// Stop stops Task Preemptor process
func (p *preemptor) Stop() error {
	defer p.Unlock()
	p.Lock()

	if !(p.isRunning()) {
		log.Warn("Task Preemptor is already stopped, no action will be performed")
		return nil
	}

	log.Info("Stopping Task Preemptor")
	atomic.StoreInt32(&p.started, 0)
	// Wait for task preemptor to be stopped
	for {
		if p.isRunning() {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	log.Info("Task Preemptor Stopped")
	return nil
}

func (p *preemptor) isRunning() bool {
	status := atomic.LoadInt32(&p.started)
	return status == 1
}
