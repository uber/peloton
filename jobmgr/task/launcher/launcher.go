package launcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"

	"go.uber.org/yarpc"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

// Config is Task launcher specific config
type Config struct {
	// PlacementDequeueLimit is the limit which task launcher get the
	// placements
	PlacementDequeueLimit int `yaml:"placement_dequeue_limit"`

	// GetPlacementsTimeout is the timeout value for task launcher to
	// call GetPlacements
	GetPlacementsTimeout int `yaml:"get_placements_timeout_ms"`
}

// Launcher defines the interface of task launcher which launches
// tasks from the placed queues of resource pool
type Launcher interface {
	// Start starts the task Launcher goroutines
	Start() error
	// Stop stops the task Launcher goroutines
	Stop() error
	// LaunchTaskWithReservedResource launches task with reserved resource
	// to hostmgr directly.
	LaunchTaskWithReservedResource(ctx context.Context, taskInfo *task.TaskInfo) error
}

// launcher implements the Launcher interface
type launcher struct {
	sync.Mutex

	resMgrClient  resmgrsvc.ResourceManagerServiceYARPCClient
	hostMgrClient hostsvc.InternalHostServiceYARPCClient
	rootCtx       context.Context
	started       int32
	shutdown      int32
	taskStore     storage.TaskStore
	volumeStore   storage.PersistentVolumeStore
	config        *Config
	metrics       *Metrics
}

const (
	// Time out for the function to time out
	_timeoutFunctionCall = 120 * time.Second
	_rpcTimeout          = 10 * time.Second
)

var (
	errEmptyTasks = errors.New("empty tasks infos")
)

var taskLauncher *launcher
var onceInitTaskLauncher sync.Once

// InitTaskLauncher initializes a Task Launcher
func InitTaskLauncher(
	d *yarpc.Dispatcher,
	resMgrClientName string,
	hostMgrClientName string,
	taskStore storage.TaskStore,
	volumeStore storage.PersistentVolumeStore,
	config *Config,
	parent tally.Scope,
) {
	onceInitTaskLauncher.Do(func() {
		if taskLauncher != nil {
			log.Warning("Task launcher has already been initialized")
			return
		}

		taskLauncher = &launcher{
			resMgrClient:  resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(resMgrClientName)),
			hostMgrClient: hostsvc.NewInternalHostServiceYARPCClient(d.ClientConfig(hostMgrClientName)),
			rootCtx:       context.Background(),
			taskStore:     taskStore,
			volumeStore:   volumeStore,
			config:        config,
			metrics:       NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
		}
	})
}

// GetLauncher returns the task scheduler instance
func GetLauncher() Launcher {
	if taskLauncher == nil {
		log.Fatal("Task launcher is not initialized")
	}
	return taskLauncher
}

// Start starts Task Launcher
func (l *launcher) Start() error {
	if atomic.CompareAndSwapInt32(&l.started, 0, 1) {
		log.Info("Starting Task Launcher")
		go func() {
			for l.isRunning() {
				placements, err := l.getPlacements()
				if err != nil {
					log.Error("Failed to get placements")
					continue
				}

				if len(placements) == 0 {
					// log a debug to make it not verbose
					log.Debug("No placements")
					continue
				}

				// Getting and launching placements in different go routine
				l.processPlacements(context.Background(), placements)
			}
		}()
	}
	log.Info("Task Launcher started")
	return nil
}

func (l *launcher) getPlacements() ([]*resmgr.Placement, error) {
	ctx, cancelFunc := context.WithTimeout(l.rootCtx, _timeoutFunctionCall)
	defer cancelFunc()

	request := &resmgrsvc.GetPlacementsRequest{
		Limit:   uint32(l.config.PlacementDequeueLimit),
		Timeout: uint32(l.config.GetPlacementsTimeout),
	}

	callStart := time.Now()
	response, err := l.resMgrClient.GetPlacements(ctx, request)
	callDuration := time.Since(callStart)

	if err != nil {
		log.WithError(err).Error("GetPlacements failed")
		l.metrics.GetPlacementFail.Inc(1)
		return nil, err
	}

	if response.GetError() != nil {
		log.WithFields(log.Fields{
			"num_placements": len(response.Placements),
			"error":          response.Error.String(),
		}).Error("Failed to get placements")
		l.metrics.GetPlacementFail.Inc(1)
		return nil, err
	}

	if len(response.GetPlacements()) != 0 {
		log.WithFields(log.Fields{
			"num_placements": len(response.Placements),
			"duration":       callDuration.Seconds(),
		}).Info("GetPlacements")
	}

	// TODO: turn getplacement metric into gauge so we can
	//       get the current get_placements counts
	l.metrics.GetPlacement.Inc(int64(len(response.GetPlacements())))
	l.metrics.GetPlacementsCallDuration.Record(callDuration)
	return response.GetPlacements(), nil
}

func (l *launcher) isRunning() bool {
	shutdown := atomic.LoadInt32(&l.shutdown)
	return shutdown == 0
}

// LaunchTaskWithReservedResource launch a task to hostmgr directly as the resource
// is already reserved.
func (l *launcher) LaunchTaskWithReservedResource(ctx context.Context, taskInfo *task.TaskInfo) error {
	pelotonTaskID := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", taskInfo.GetJobId().GetValue(), taskInfo.GetInstanceId()),
	}
	taskRuntime := taskInfo.GetRuntime()
	launchableTasks, err := l.getLaunchableTasks(
		ctx, []*peloton.TaskID{pelotonTaskID},
		taskRuntime.GetHost(),
		nil)
	if err != nil {
		return err
	}
	runtimePorts := taskInfo.GetRuntime().GetPorts()
	var selectedPorts []uint32
	for _, port := range runtimePorts {
		selectedPorts = append(selectedPorts, port)
	}
	err = l.launchStatefulTasks(
		launchableTasks, taskRuntime.GetHost(), selectedPorts, false)
	return err
}

// launchTasks launches tasks to host manager
func (l *launcher) processPlacements(ctx context.Context, placements []*resmgr.Placement) error {
	log.WithField("placements", placements).Debug("Start processing placements")
	for _, placement := range placements {
		go func(placement *resmgr.Placement) {
			tasks, err := l.getLaunchableTasks(
				ctx,
				placement.GetTasks(),
				placement.GetHostname(),
				placement.GetPorts())
			if err != nil {
				log.WithError(err).WithFields(log.Fields{
					"placement": placement,
				}).Error("Failed to get launchable tasks")
				return
			}
			l.metrics.LauncherGoRoutines.Inc(1)
			err = l.launchTasks(tasks, placement)
			if err != nil {
				log.WithFields(log.Fields{
					"placement": placement,
					"error":     err.Error(),
				}).Error("Failed to launch tasks")
				// TODO: We need to enqueue tasks to resmgr after updating staus
				// TODO: to db for the ones which are failed to launch
			}
		}(placement)
	}
	return nil
}

func (l *launcher) getLaunchableTasks(
	ctx context.Context,
	tasks []*peloton.TaskID,
	hostname string,
	selectedPorts []uint32) ([]*hostsvc.LaunchableTask, error) {

	portsIndex := 0

	var tasksInfo []*task.TaskInfo
	getTaskInfoStart := time.Now()

	for _, taskID := range tasks {
		// TODO: we need to check goal state before we can launch tasks
		// TODO: We need to add batch api's for getting all tasks in one shot
		taskInfo, err := l.taskStore.GetTaskByID(ctx, taskID.GetValue())
		if err != nil {
			log.WithError(err).WithField("Task Id", taskID.Value).
				Error("Not able to get Task")
			continue
		}

		// Generate volume ID if not set for stateful task.
		if taskInfo.GetConfig().GetVolume() != nil {
			if taskInfo.GetRuntime().GetVolumeID() == nil || hostname != taskInfo.GetRuntime().GetHost() {
				// Generates volume ID if first time launch the stateful task,
				// OR task is being launched to a different host.
				taskInfo.GetRuntime().VolumeID = &peloton.VolumeID{
					Value: uuid.NewUUID().String(),
				}
			}
		}

		taskInfo.GetRuntime().Host = hostname
		taskInfo.GetRuntime().State = task.TaskState_LAUNCHING
		if selectedPorts != nil {
			// Reset runtime ports to get new ports assignment if placement has ports.
			taskInfo.GetRuntime().Ports = make(map[string]uint32)
			// Assign selected dynamic port to task per port config.
			for _, portConfig := range taskInfo.GetConfig().GetPorts() {
				if portConfig.GetValue() != 0 {
					// Skip static port.
					continue
				}
				if portsIndex >= len(selectedPorts) {
					// This should never happen.
					log.WithFields(log.Fields{
						"selected_ports": selectedPorts,
						"task_info":      taskInfo,
					}).Error("placement contains less selected ports than required.")
					return nil, errors.New("invalid placement")
				}
				taskInfo.GetRuntime().Ports[portConfig.GetName()] = selectedPorts[portsIndex]
				portsIndex++
			}
		}

		// Writes the hostname and ports information back to db.
		err = l.taskStore.UpdateTask(ctx, taskInfo)
		if err != nil {
			log.WithError(err).WithField("task_info", taskInfo).
				Error("Not able to update Task")
			continue
		}

		tasksInfo = append(tasksInfo, taskInfo)
	}

	if len(tasksInfo) == 0 {
		return nil, errEmptyTasks
	}

	getTaskInfoDuration := time.Since(getTaskInfoStart)

	log.WithFields(log.Fields{
		"num_tasks": len(tasks),
		"duration":  getTaskInfoDuration.Seconds(),
	}).Debug("GetTaskInfo")

	l.metrics.GetDBTaskInfo.Record(getTaskInfoDuration)

	return createLaunchableTasks(tasksInfo), nil
}

// createLaunchableTasks generates list of hostsvc.LaunchableTask from list of task.TaskInfo
func createLaunchableTasks(tasks []*task.TaskInfo) []*hostsvc.LaunchableTask {
	var launchableTasks []*hostsvc.LaunchableTask
	for _, task := range tasks {
		var launchableTask hostsvc.LaunchableTask
		// Add volume info into launchable task if task config has volume.
		if task.GetConfig().GetVolume() != nil {
			diskResource := util.NewMesosResourceBuilder().
				WithName("disk").
				WithValue(float64(task.GetConfig().GetVolume().GetSizeMB())).
				Build()
			launchableTask = hostsvc.LaunchableTask{
				TaskId: task.GetRuntime().GetMesosTaskId(),
				Config: task.GetConfig(),
				Ports:  task.GetRuntime().GetPorts(),
				Volume: &hostsvc.Volume{
					Id:            task.GetRuntime().GetVolumeID(),
					ContainerPath: task.GetConfig().GetVolume().GetContainerPath(),
					Resource:      diskResource,
				},
			}
		} else {
			launchableTask = hostsvc.LaunchableTask{
				TaskId: task.GetRuntime().GetMesosTaskId(),
				Config: task.GetConfig(),
				Ports:  task.GetRuntime().GetPorts(),
			}
		}
		launchableTasks = append(launchableTasks, &launchableTask)
	}
	return launchableTasks
}

// Stop stops Task Launcher process
func (l *launcher) Stop() error {
	defer l.Unlock()
	l.Lock()

	if !(l.isRunning()) {
		log.Warn("Task Launcher is already stopped, no action will be performed")
		return nil
	}

	log.Info("Stopping Task Launcher")
	atomic.StoreInt32(&l.shutdown, 1)
	// Wait for task launcher to be stopped
	for {
		if l.isRunning() {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	log.Info("Task Launcher Stopped")
	return nil
}

func (l *launcher) launchStatefulTasks(
	selectedTasks []*hostsvc.LaunchableTask,
	hostname string,
	selectedPorts []uint32,
	checkVolume bool) error {
	ctx, cancelFunc := context.WithTimeout(l.rootCtx, _rpcTimeout)
	defer cancelFunc()

	volumeID := selectedTasks[0].GetVolume().GetId().GetValue()
	createVolume := false
	if checkVolume {
		pv, err := l.volumeStore.GetPersistentVolume(ctx, volumeID)
		if err != nil {
			_, ok := err.(*storage.VolumeNotFoundError)
			if !ok {
				// volume store db read error.
				return err
			}
			// First time launch stateful task and create volume.
			createVolume = true
		} else if pv.GetState() != volume.VolumeState_CREATED {
			// Retry launch task but volume is not created.
			createVolume = true
		}
	}

	var err error
	var operations []*hostsvc.OfferOperation
	hostOperationFactory := NewHostOperationsFactory(selectedTasks, hostname, selectedPorts)
	if createVolume {
		// Reserve, Create and Launch the task.
		operations, err = hostOperationFactory.GetHostOperations(
			[]hostsvc.OfferOperation_Type{
				hostsvc.OfferOperation_RESERVE,
				hostsvc.OfferOperation_CREATE,
				hostsvc.OfferOperation_LAUNCH,
			})
	} else {
		// Launch using the volume.
		operations, err = hostOperationFactory.GetHostOperations(
			[]hostsvc.OfferOperation_Type{
				hostsvc.OfferOperation_LAUNCH,
			})
	}

	if err != nil {
		return err
	}

	var request = &hostsvc.OfferOperationsRequest{
		Hostname:   hostname,
		Operations: operations,
	}

	log.WithField("request", request).Debug("OfferOperations Called")

	response, err := l.hostMgrClient.OfferOperations(ctx, request)
	if err != nil {
		return err
	}
	if response.GetError() != nil {
		return errors.New(response.Error.String())
	}
	return nil
}

func (l *launcher) launchBatchTasks(
	selectedTasks []*hostsvc.LaunchableTask,
	placement *resmgr.Placement) error {
	ctx, cancelFunc := context.WithTimeout(l.rootCtx, _rpcTimeout)
	defer cancelFunc()
	var request = &hostsvc.LaunchTasksRequest{
		Hostname: placement.GetHostname(),
		Tasks:    selectedTasks,
		AgentId:  placement.GetAgentId(),
	}

	log.WithField("request", request).Debug("LaunchTasks Called")

	response, err := l.hostMgrClient.LaunchTasks(ctx, request)
	if err != nil {
		return err
	}
	if response.GetError() != nil {
		return errors.New(response.Error.String())
	}
	return nil
}

func (l *launcher) launchTasks(
	selectedTasks []*hostsvc.LaunchableTask,
	placement *resmgr.Placement) error {
	// TODO: Add retry Logic for tasks launching failure
	if len(selectedTasks) == 0 {
		log.Debug("No task is selected to launch")
		return errors.New("No task is selected to launch")
	}

	log.WithField("tasks", selectedTasks).Debug("Launching Tasks")

	callStart := time.Now()
	var err error
	if placement.Type != resmgr.TaskType_STATEFUL {
		err = l.launchBatchTasks(selectedTasks, placement)
	} else {
		err = l.launchStatefulTasks(
			selectedTasks,
			placement.GetHostname(),
			placement.GetPorts(),
			true,
		)
	}
	callDuration := time.Since(callStart)

	if err != nil {
		log.WithFields(log.Fields{
			"tasks":     len(selectedTasks),
			"placement": placement,
		}).WithError(err).Error("Failed to launch tasks")
		l.metrics.TaskLaunchFail.Inc(1)
		return err
	}

	l.metrics.TaskLaunch.Inc(int64(len(selectedTasks)))

	log.WithFields(log.Fields{
		"num_tasks": len(selectedTasks),
		"hostname":  placement.GetHostname(),
		"duration":  callDuration.Seconds(),
	}).Debug("Launched tasks")
	l.metrics.LaunchTasksCallDuration.Record(callDuration)
	return nil
}
