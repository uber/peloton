package placement

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"

	"go.uber.org/yarpc"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/jobmgr/task/launcher"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
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
	resMgrClient   resmgrsvc.ResourceManagerServiceYARPCClient
	trackedManager tracked.Manager
	taskLauncher   launcher.Launcher
	running        int32
	done           int32
	config         *Config
	metrics        *Metrics
}

const (
	// Time out for the function to time out
	_rpcTimeout = 10 * time.Second
)

// InitProcessor initializes placement processor
func InitProcessor(
	d *yarpc.Dispatcher,
	resMgrClientName string,
	trackedManager tracked.Manager,
	taskLauncher launcher.Launcher,
	config *Config,
	parent tally.Scope,
) Processor {
	return &processor{
		resMgrClient:   resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(resMgrClientName)),
		trackedManager: trackedManager,
		taskLauncher:   taskLauncher,
		config:         config,
		metrics:        NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
	}
}

// Start starts Processor
func (p *processor) Start() error {
	if p.isRunning() {
		// already running
		log.Warn("placement processor is already running, no action will be performed")
		return nil
	}

	log.Info("starting placement processor")
	atomic.StoreInt32(&p.running, 1)
	go func() {
		for p.isRunning() {
			placements, err := p.getPlacements()
			if err != nil {
				log.WithError(err).Error("jobmgr failed to dequeue placements")
				continue
			}

			if !p.isRunning() {
				// placement dequeued but not processed
				log.WithField("placements", placements).
					Warn("ignoring placement after dequeue due to lost leadership")
				break
			}

			if len(placements) == 0 {
				// log a debug to make it not verbose
				log.Debug("No placements")
				continue
			}

			ctx := context.Background()

			// Getting and launching placements in different go routine
			log.WithField("placements", placements).Debug("Start processing placements")
			for _, placement := range placements {
				go p.ProcessPlacement(ctx, placement)
			}
		}
		// All placements completed processing,
		atomic.StoreInt32(&p.done, 1)
	}()
	log.Info("placement processor started")
	return nil
}

func (p *processor) ProcessPlacement(ctx context.Context, placement *resmgr.Placement) {
	tasks := placement.GetTasks()
	taskInfos, err := p.taskLauncher.GetLaunchableTasks(
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

	for taskID, taskInfo := range taskInfos {
		id, instanceID, err := util.ParseTaskID(taskID)
		if err != nil {
			continue
		}
		jobID := &peloton.JobID{Value: id}
		runtime := taskInfo.GetRuntime()

		if runtime.GetGoalState() == task.TaskState_KILLED {
			if runtime.GetState() != task.TaskState_KILLED {
				// Received placement for task which needs to be killed, retry killing the task.
				killTasks := make(map[uint32]*task.RuntimeInfo)
				killTasks[uint32(instanceID)] = runtime
				p.trackedManager.SetTasks(jobID, killTasks, tracked.UpdateAndSchedule)
			}

			// Skip launching of deleted tasks.
			log.WithField("task_id", taskID).Info("skipping launch of killed task")
			delete(taskInfos, taskID)
		} else {
			for {
				err := p.trackedManager.UpdateTaskRuntime(ctx, jobID, uint32(instanceID), runtime, tracked.UpdateOnly)
				if err == nil {
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
			}
		}
	}

	if len(taskInfos) == 0 {
		// nothing to launch
		return
	}

	launchableTasks := launcher.CreateLaunchableTasks(taskInfos)
	err = p.taskLauncher.ProcessPlacement(ctx, launchableTasks, placement)
	if err != nil {
		if err = p.enqueueTasks(ctx, taskInfos); err != nil {
			log.WithError(err).WithFields(log.Fields{
				"task_infos":  taskInfos,
				"tasks_total": len(taskInfos),
			}).Error("failed to enqueue tasks back to resmgr")
		}
	}
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
func (p *processor) enqueueTasks(ctx context.Context, tasks map[string]*task.TaskInfo) error {
	if len(tasks) == 0 {
		return nil
	}

	var err error
	for _, t := range tasks {
		t.Runtime.State = task.TaskState_INITIALIZED
		t.Runtime.Message = "Regenerate placement"
		t.Runtime.Reason = "REASON_HOST_REJECT_OFFER"
		t.Runtime.Host = ""
		t.Runtime.AgentID = nil
		t.Runtime.Ports = nil
		util.RegenerateMesosTaskID(t.JobId, t.InstanceId, t.Runtime)
		for {
			err = p.trackedManager.UpdateTaskRuntime(ctx, t.JobId, t.InstanceId, t.Runtime, tracked.UpdateAndSchedule)
			if err == nil {
				break
			}
			if common.IsTransientError(err) {
				// TBD add a max retry to bail out after a few retries.
				log.WithError(err).WithFields(log.Fields{
					"job_id":      t.JobId,
					"instance_id": t.InstanceId,
				}).Warn("retrying update task runtime on transient error")
			} else {
				return err
			}
		}
	}
	return err
}

func (p *processor) isRunning() bool {
	running := atomic.LoadInt32(&p.running)
	return running == 1
}

func (p *processor) isDone() bool {
	done := atomic.LoadInt32(&p.done)
	return done == 1
}

// Stop stops placement processor
func (p *processor) Stop() error {
	atomic.StoreInt32(&p.done, 0)
	if !(p.isRunning()) {
		log.Warn("placement processor is already stopped, no action will be performed")
		return nil
	}

	log.Info("stopping placement processor")
	atomic.StoreInt32(&p.running, 0)
	for {
		// Wait for all placements to be done processing
		if !p.isDone() {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	log.Info("placement processor stopped")
	return nil
}
