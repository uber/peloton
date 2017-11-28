package placement

import (
	"context"
	"sync"
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
	"code.uber.internal/infra/peloton/jobmgr/task/launcher"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/storage"
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
	sync.Mutex
	resMgrClient   resmgrsvc.ResourceManagerServiceYARPCClient
	trackedManager tracked.Manager
	taskLauncher   launcher.Launcher
	taskStore      storage.TaskStore
	started        int32
	shutdown       int32
	config         *Config
	metrics        *Metrics
}

const (
	// Time out for the function to time out
	_rpcTimeout = 10 * time.Second
)

var pp *processor
var onceInitPP sync.Once

// InitProcessor initializes placement processor
func InitProcessor(
	d *yarpc.Dispatcher,
	resMgrClientName string,
	trackedManager tracked.Manager,
	taskLauncher launcher.Launcher,
	taskStore storage.TaskStore,
	config *Config,
	parent tally.Scope,
) {
	onceInitPP.Do(func() {
		if pp != nil {
			log.Warning("placement processor has already been initialized")
			return
		}

		pp = &processor{
			resMgrClient:   resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(resMgrClientName)),
			trackedManager: trackedManager,
			taskLauncher:   taskLauncher,
			taskStore:      taskStore,
			config:         config,
			metrics:        NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
		}
	})
}

// GetProcessor returns the placement processor instance
func GetProcessor() Processor {
	if pp == nil {
		log.Fatal("placement processor is not initialized")
	}
	return pp
}

// Start starts Processor
func (p *processor) Start() error {
	if atomic.CompareAndSwapInt32(&p.started, 0, 1) {
		log.Info("starting placement processor")
		atomic.StoreInt32(&p.shutdown, 0)
		go func() {
			for p.isRunning() {
				placements, err := p.getPlacements()
				if err != nil {
					log.WithError(err).Error("jobmgr failed to dequeue placements")
					continue
				}

				if len(placements) == 0 {
					// log a debug to make it not verbose
					log.Debug("No placements")
					continue
				}

				ctx := context.Background()

				p.preProcessPlacements(ctx, placements)

				// Getting and launching placements in different go routine
				p.taskLauncher.ProcessPlacements(ctx, p.resMgrClient, placements)
			}
		}()
	}
	log.Info("placement processor started")
	return nil
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

func (p *processor) preProcessPlacements(ctx context.Context, placements []*resmgr.Placement) {
	for _, placement := range placements {
		tasks := placement.GetTasks()
		for _, taskID := range tasks {
			id, instanceID, err := util.ParseTaskID(taskID.GetValue())
			if err != nil {
				continue
			}
			jobID := &peloton.JobID{Value: id}
			runtime, err := p.taskStore.GetTaskRuntime(ctx, jobID, uint32(instanceID))
			if err != nil {
				continue
			}
			if runtime.GetGoalState() == task.TaskState_KILLED && runtime.GetState() != task.TaskState_KILLED {
				// Received placement for task which needs to be killed, retry killing the task.
				p.trackedManager.SetTask(jobID, uint32(instanceID), runtime)
			}
		}
	}
}

func (p *processor) isRunning() bool {
	shutdown := atomic.LoadInt32(&p.shutdown)
	return shutdown == 0
}

// Stop stops placement processor
func (p *processor) Stop() error {
	defer p.Unlock()
	p.Lock()

	if !(p.isRunning()) {
		log.Warn("placement processor is already stopped, no action will be performed")
		return nil
	}

	log.Info("Stopping placement processor")
	atomic.StoreInt32(&p.shutdown, 1)
	// Wait for placement processor to be stopped
	for {
		if p.isRunning() {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	log.Info("placement processor stopped")
	return nil
}
