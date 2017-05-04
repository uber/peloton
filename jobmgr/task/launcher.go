package task

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	"peloton/api/task"
	"peloton/private/hostmgr/hostsvc"
	"peloton/private/resmgr"
	"peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/storage"
)

// LauncherConfig is Task launcher specific config
type LauncherConfig struct {
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
}

// launcher implements the Launcher interface
type launcher struct {
	sync.Mutex

	resMgrClient  json.Client
	hostMgrClient json.Client
	rootCtx       context.Context
	started       int32
	shutdown      int32
	taskStore     storage.TaskStore
	config        *LauncherConfig
	metrics       *Metrics
}

var taskLauncher *launcher
var onceInitTaskLauncher sync.Once

// InitTaskLauncher initializes a Task Launcher
func InitTaskLauncher(
	d yarpc.Dispatcher,
	resMgrClientName string,
	hostMgrClientName string,
	taskStore storage.TaskStore,
	config *LauncherConfig,
	parent tally.Scope,
) {
	onceInitTaskLauncher.Do(func() {
		if taskLauncher != nil {
			log.Warning("Task launcher has already been initialized")
			return
		}

		taskLauncher = &launcher{
			resMgrClient:  json.New(d.ClientConfig(resMgrClientName)),
			hostMgrClient: json.New(d.ClientConfig(hostMgrClientName)),
			rootCtx:       context.Background(),
			taskStore:     taskStore,
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

				// TODO: Implement this as a thread pool
				// Getting and launching placements in different go routine
				l.processPlacements(placements)
			}
		}()
	}
	log.Info("Task Launcher started")
	return nil
}

func (l *launcher) getPlacements() ([]*resmgr.Placement, error) {
	ctx, cancelFunc := context.WithTimeout(l.rootCtx, timeoutFunctionCall)
	defer cancelFunc()

	var response resmgrsvc.GetPlacementsResponse
	request := resmgrsvc.GetPlacementsRequest{
		Limit:   uint32(l.config.PlacementDequeueLimit),
		Timeout: uint32(l.config.GetPlacementsTimeout),
	}

	callStart := time.Now()
	_, err := l.resMgrClient.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("ResourceManagerService.GetPlacements"),
		request,
		&response,
	)
	callDuration := time.Since(callStart)

	if err != nil {
		log.WithError(err).Error("GetPlacements failed")
		l.metrics.GetPlacementFail.Inc(1)
		return nil, err
	}

	if response.Error != nil {
		log.WithFields(log.Fields{
			"num_placements": len(response.Placements),
			"error":          response.Error.String(),
		}).Error("Failed to get placements")
		l.metrics.GetPlacementFail.Inc(1)
		return nil, err
	}

	if len(response.Placements) != 0 {
		log.WithFields(log.Fields{
			"num_placements": len(response.Placements),
			"duration":       callDuration.Seconds(),
		}).Info("GetPlacements")
	}

	// TODO: turn getplacement metric into gauge so we can
	//       get the current get_placements counts
	l.metrics.GetPlacement.Inc(int64(len(response.Placements)))
	l.metrics.GetPlacementsCall.Record(callDuration)
	return response.Placements, nil
}

func (l *launcher) isRunning() bool {
	shutdown := atomic.LoadInt32(&l.shutdown)
	return shutdown == 0
}

// launchTasks launches tasks to host manager
func (l *launcher) processPlacements(placements []*resmgr.Placement) error {
	log.WithField("placements", placements).Debug("Start processing placements")
	for _, placement := range placements {
		go func(placement *resmgr.Placement) {
			tasks, err := l.getLaunchableTasks(placement)
			if err != nil {
				log.WithFields(log.Fields{
					"placement": placement,
					"error":     err.Error(),
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
	placement *resmgr.Placement) ([]*hostsvc.LaunchableTask, error) {

	tasks := placement.GetTasks()
	hostname := placement.GetHostname()
	selectedPorts := placement.GetPorts()
	portsIndex := 0

	var tasksInfo []*task.TaskInfo
	getTaskInfoStart := time.Now()
	watcher := l.metrics.GetDBTaskInfo.Start()

	for _, taskID := range tasks {
		// TODO: we need to check goal state before we can launch tasks
		// TODO: We need to add batch api's for getting all tasks in one shot
		taskInfo, err := l.taskStore.GetTaskByID(taskID.Value)
		if err != nil {
			log.WithField("Task Id", taskID.Value).Error("Not able to get Task")
			continue
		}

		taskInfo.GetRuntime().Host = hostname
		taskInfo.GetRuntime().State = task.TaskState_LAUNCHING
		// Assign selected dynamic port to task per port config.
		for _, portConfig := range taskInfo.GetConfig().GetPorts() {
			if portConfig.GetValue() != 0 {
				// Skip static port.
				continue
			}
			if portsIndex >= len(selectedPorts) {
				// This should never happen.
				log.WithField("placement", placement).
					Error("placement contains less ports than required.")
				return nil, errors.New("invalid placement")
			}
			// Make sure runtime ports is not nil.
			if taskInfo.GetRuntime().GetPorts() == nil {
				taskInfo.GetRuntime().Ports = make(map[string]uint32)
			}
			taskInfo.GetRuntime().Ports[portConfig.GetName()] = selectedPorts[portsIndex]
			portsIndex++
		}

		// Writes the hostname and ports information back to db.
		err = l.taskStore.UpdateTask(taskInfo)
		if err != nil {
			log.WithField("task_info", taskInfo).
				Error("Not able to update Task")
			continue
		}

		tasksInfo = append(tasksInfo, taskInfo)
	}

	getTaskInfoDuration := time.Since(getTaskInfoStart)

	log.WithFields(log.Fields{
		"num_tasks": len(tasks),
		"duration":  getTaskInfoDuration.Seconds(),
	}).Info("GetTaskInfo")

	d := watcher.Stop()
	l.metrics.GetDBTaskInfo.Record(d)

	launchableTasks := l.createLaunchableTasks(tasksInfo)
	return launchableTasks, nil
}

// createLaunchableTasks generates list of hostsvc.LaunchableTask from list of task.TaskInfo
func (l *launcher) createLaunchableTasks(tasks []*task.TaskInfo) []*hostsvc.LaunchableTask {
	var launchableTasks []*hostsvc.LaunchableTask
	for _, task := range tasks {
		launchableTask := hostsvc.LaunchableTask{
			TaskId: task.GetRuntime().GetTaskId(),
			Config: task.GetConfig(),
			Ports:  task.GetRuntime().GetPorts(),
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

func (l *launcher) launchTasks(selectedTasks []*hostsvc.LaunchableTask,
	placement *resmgr.Placement) error {
	// TODO: Add retry Logic for tasks launching failure
	if len(selectedTasks) == 0 {
		log.Debug("No task is selected to launch")
		return errors.New("No task is selected to launch")
	}

	log.WithField("tasks", selectedTasks).Debug("Launching Tasks")
	ctx, cancelFunc := context.WithTimeout(l.rootCtx, 10*time.Second)
	defer cancelFunc()
	var response hostsvc.LaunchTasksResponse
	var request = &hostsvc.LaunchTasksRequest{
		Hostname: placement.GetHostname(),
		Tasks:    selectedTasks,
		AgentId:  placement.GetAgentId(),
	}

	log.WithField("request", request).Debug("LaunchTasks Called")

	callStart := time.Now()
	_, err := l.hostMgrClient.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("InternalHostService.LaunchTasks"),
		request,
		&response,
	)
	callDuration := time.Since(callStart)

	if err != nil {
		log.WithFields(log.Fields{
			"tasks":     len(selectedTasks),
			"placement": placement,
		}).WithError(err).Error("Failed to launch tasks")
		l.metrics.TaskLaunchFail.Inc(1)
		return err
	}

	log.WithField("response", response).Debug("LaunchTasks returned")

	if response.Error != nil {
		log.WithFields(log.Fields{
			"tasks":     len(selectedTasks),
			"placement": placement,
			"error":     response.Error.String(),
		}).Error("Failed to launch tasks")
		l.metrics.TaskLaunchFail.Inc(1)
		return errors.New(response.Error.String())
	}
	l.metrics.TaskLaunch.Inc(int64(len(selectedTasks)))

	log.WithFields(log.Fields{
		"num_tasks": len(selectedTasks),
		"hostname":  placement.GetHostname(),
		"duration":  callDuration.Seconds(),
	}).Info("Launched tasks")
	l.metrics.LaunchTasksCall.Record(callDuration)
	return nil
}
