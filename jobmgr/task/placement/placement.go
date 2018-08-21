package placement

import (
	"context"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"

	"go.uber.org/yarpc"

	mesosv1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/lifecycle"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/task/launcher"
	taskutil "code.uber.internal/infra/peloton/jobmgr/util/task"
	"code.uber.internal/infra/peloton/util"
)

// Config is placement processor specific config
type Config struct {
	// PlacementDequeueLimit is the limit which placement processor get the
	// placements
	PlacementDequeueLimit int `yaml:"placement_dequeue_limit"`

	// GetPlacementsTimeout is the timeout value for placement processor to
	// call GetPlacements
	GetPlacementsTimeout int `yaml:"get_placements_timeout_ms"`
}

// Processor defines the interface of placement processor
// which dequeues placed tasks and sends them to host manager via task launcher
type Processor interface {
	// Start starts the placement processor goroutines
	Start() error
	// Stop stops the placement processor goroutines
	Stop() error
}

// launcher implements the Launcher interface
type processor struct {
	resMgrClient    resmgrsvc.ResourceManagerServiceYARPCClient
	jobFactory      cached.JobFactory
	goalStateDriver goalstate.Driver
	taskLauncher    launcher.Launcher
	lifeCycle       lifecycle.LifeCycle
	config          *Config
	metrics         *Metrics
}

const (
	// Time out for the function to time out
	_rpcTimeout = 10 * time.Second
	// maxRetryCount is the maximum number of times a transient error from the DB will be retried.
	// This is a safety mechanism to avoid placement engine getting stuck in
	// retrying errors wrongly classified as transient errors.
	maxRetryCount = 1000
)

// InitProcessor initializes placement processor
func InitProcessor(
	d *yarpc.Dispatcher,
	resMgrClientName string,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	taskLauncher launcher.Launcher,
	config *Config,
	parent tally.Scope,
) Processor {
	return &processor{
		resMgrClient:    resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(resMgrClientName)),
		jobFactory:      jobFactory,
		goalStateDriver: goalStateDriver,
		taskLauncher:    taskLauncher,
		config:          config,
		metrics:         NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
		lifeCycle:       lifecycle.NewLifeCycle(),
	}
}

// Start starts Processor
func (p *processor) Start() error {
	if !p.lifeCycle.Start() {
		// already running
		log.Warn("placement processor is already running, no action will be performed")
		return nil
	}
	log.Info("starting placement processor")

	go p.run()

	log.Info("placement processor started")
	return nil
}

func (p *processor) run() {
	for {
		select {
		case <-p.lifeCycle.StopCh():
			p.lifeCycle.StopComplete()
			return
		default:
			p.process()
		}
	}
}

func (p *processor) process() {
	placements, err := p.getPlacements()
	if err != nil {
		log.WithError(err).Error("jobmgr failed to dequeue placements")
		return
	}

	select {
	case <-p.lifeCycle.StopCh():
		// placement dequeued but not processed
		log.WithField("placements", placements).
			Warn("ignoring placement after dequeue due to lost leadership")
		return
	default:
		break
	}

	if len(placements) == 0 {
		// log a debug to make it not verbose
		log.Debug("No placements")
		return
	}

	ctx := context.Background()

	// Getting and launching placements in different go routine
	log.WithField("placements", placements).Debug("Start processing placements")
	for _, placement := range placements {
		go p.processPlacement(ctx, placement)
	}
}

func (p *processor) processPlacement(ctx context.Context, placement *resmgr.Placement) {
	tasks := placement.GetTasks()
	lauchableTasks, err := p.taskLauncher.GetLaunchableTasks(
		ctx,
		placement.GetTasks(),
		placement.GetHostname(),
		placement.GetAgentId(),
		placement.GetPorts())
	if err != nil {
		err = p.taskLauncher.TryReturnOffers(ctx, err, placement)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"placement":   placement,
				"tasks_total": len(tasks),
			}).Error("Failed to get launchable tasks")
		}
		return
	}

	taskInfos := p.createTaskInfos(ctx, lauchableTasks)

	if len(taskInfos) == 0 {
		// nothing to launch
		return
	}

	// CreateLaunchableTasks returns a list of launchableTasks and taskInfo map
	// of tasks that could not be launched because of transient error in getting
	// secrets.
	launchableTasks, skippedTaskInfos := p.taskLauncher.CreateLaunchableTasks(ctx, taskInfos)
	// enqueue skipped tasks back to resmgr to launch again, instead of waiting
	// for resmgr timeout
	p.enqueueTasksToResMgr(ctx, skippedTaskInfos)

	if err = p.taskLauncher.ProcessPlacement(ctx, launchableTasks, placement); err != nil {
		p.enqueueTasksToResMgr(ctx, taskInfos)
		return
	}

	// Finally, enqueue tasks into goalstate
	p.enqueueTaskToGoalState(taskInfos)

}

func (p *processor) enqueueTaskToGoalState(taskInfos map[string]*task.TaskInfo) {
	for id := range taskInfos {
		jobID, instanceID, err := util.ParseTaskID(id)
		if err != nil {
			log.WithError(err).
				WithField("task_id", id).
				Error("failed to parse the task id in placement processor")
			continue
		}

		p.goalStateDriver.EnqueueTask(
			&peloton.JobID{Value: jobID},
			uint32(instanceID),
			time.Now())

		cachedJob := p.jobFactory.AddJob(&peloton.JobID{Value: jobID})
		goalstate.EnqueueJobWithDefaultDelay(
			&peloton.JobID{Value: jobID},
			p.goalStateDriver,
			cachedJob)
	}
}

func (p *processor) createTaskInfos(
	ctx context.Context,
	lauchableTasks map[string]*launcher.LaunchableTask,
) map[string]*task.TaskInfo {
	taskInfos := make(map[string]*task.TaskInfo)
	for taskID, launchableTask := range lauchableTasks {
		id, instanceID, err := util.ParseTaskID(taskID)
		if err != nil {
			continue
		}
		jobID := &peloton.JobID{Value: id}
		cachedJob := p.jobFactory.AddJob(jobID)
		cachedTask := cachedJob.AddTask(uint32(instanceID))

		cachedRuntime, err := cachedTask.GetRunTime(ctx)
		if err != nil {
			log.WithError(err).
				WithField("task_id", taskID).
				Error("cannot fetch task runtime")
			continue
		}

		if cachedRuntime.GetGoalState() == task.TaskState_KILLED {
			if cachedRuntime.GetState() != task.TaskState_KILLED {
				// Received placement for task which needs to be killed, retry killing the task.
				p.goalStateDriver.EnqueueTask(jobID, uint32(instanceID), time.Now())
			}

			// Skip launching of deleted tasks.
			log.WithField("task_id", taskID).Info("skipping launch of killed task")
		} else {
			retry := 0
			for retry < maxRetryCount {
				err = cachedJob.PatchTasks(ctx,
					map[uint32]cached.RuntimeDiff{
						uint32(instanceID): launchableTask.RuntimeDiff,
					})
				if err == nil {
					runtime, _ := cachedTask.GetRunTime(ctx)
					taskInfos[taskID] = &task.TaskInfo{
						Runtime:    runtime,
						Config:     launchableTask.Config,
						InstanceId: uint32(instanceID),
						JobId:      jobID,
					}
					break
				}
				if common.IsTransientError(err) {
					// TBD add a max retry to bail out after a few retries.
					log.WithError(err).WithFields(log.Fields{
						"job_id":      id,
						"instance_id": instanceID,
					}).Warn("retrying update task runtime on transient error")
				} else {
					log.WithError(err).WithFields(log.Fields{
						"job_id":      id,
						"instance_id": instanceID,
					}).Error("cannot process placement due to non-transient db error")
					delete(taskInfos, taskID)
					break
				}
				retry++
			}
		}
	}
	return taskInfos
}

func (p *processor) getPlacements() ([]*resmgr.Placement, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), _rpcTimeout)
	defer cancelFunc()

	request := &resmgrsvc.GetPlacementsRequest{
		Limit:   uint32(p.config.PlacementDequeueLimit),
		Timeout: uint32(p.config.GetPlacementsTimeout),
	}

	callStart := time.Now()
	response, err := p.resMgrClient.GetPlacements(ctx, request)
	callDuration := time.Since(callStart)

	if err != nil {
		p.metrics.GetPlacementFail.Inc(1)
		return nil, err
	}

	if response.GetError() != nil {
		p.metrics.GetPlacementFail.Inc(1)
		return nil, errors.New(response.GetError().String())
	}

	if len(response.GetPlacements()) != 0 {
		log.WithFields(log.Fields{
			"num_placements": len(response.Placements),
			"duration":       callDuration.Seconds(),
		}).Info("GetPlacements")
	}

	// TODO: turn getplacement metric into gauge so we can
	//       get the current get_placements counts
	p.metrics.GetPlacement.Inc(int64(len(response.GetPlacements())))
	p.metrics.GetPlacementsCallDuration.Record(callDuration)
	return response.GetPlacements(), nil
}

// enqueueTask enqueues given task to resmgr to launch again.
func (p *processor) enqueueTasksToResMgr(ctx context.Context, tasks map[string]*task.TaskInfo) (err error) {
	defer func() {
		if err == nil {
			return
		}
		var taskIDs []string
		for taskID := range tasks {
			taskIDs = append(taskIDs, taskID)
		}
		log.WithError(err).WithFields(log.Fields{
			"task_ids":    taskIDs,
			"tasks_total": len(tasks),
		}).Error("failed to enqueue tasks to resmgr")
	}()

	if len(tasks) == 0 {
		return nil
	}

	for _, t := range tasks {
		runtimeDiff := taskutil.RegenerateMesosTaskIDDiff(
			t.JobId, t.InstanceId, t.GetRuntime())
		runtimeDiff[cached.MessageField] = "Regenerate placement"
		runtimeDiff[cached.ReasonField] = "REASON_HOST_REJECT_OFFER"
		runtimeDiff[cached.AgentIDField] = &mesosv1.AgentID{}
		runtimeDiff[cached.PortsField] = make(map[string]uint32)
		retry := 0
		for retry < maxRetryCount {
			cachedJob := p.jobFactory.AddJob(t.JobId)
			err = cachedJob.PatchTasks(
				ctx,
				map[uint32]cached.RuntimeDiff{uint32(t.InstanceId): runtimeDiff},
			)
			if err == nil {
				p.goalStateDriver.EnqueueTask(t.JobId, t.InstanceId, time.Now())
				goalstate.EnqueueJobWithDefaultDelay(
					t.JobId, p.goalStateDriver, cachedJob)
				break
			}
			if common.IsTransientError(err) {
				log.WithError(err).WithFields(log.Fields{
					"job_id":      t.JobId,
					"instance_id": t.InstanceId,
				}).Warn("retrying update task runtime on transient error")
			} else {
				return err
			}
			retry++
		}
	}
	return err
}

// Stop stops placement processor
func (p *processor) Stop() error {
	if !(p.lifeCycle.Stop()) {
		log.Warn("placement processor is already stopped, no action will be performed")
		return nil
	}

	p.lifeCycle.Wait()
	log.Info("placement processor stopped")
	return nil
}
