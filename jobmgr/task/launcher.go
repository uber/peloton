package task

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	"peloton/api/peloton"
	"peloton/api/task"
	"peloton/private/hostmgr/hostsvc"
	"peloton/private/resmgr"
	"peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/jobmgr"
	"code.uber.internal/infra/peloton/storage"
	"github.com/uber-go/tally"
)

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
	config        *jobmgr.Config
	metrics       *Metrics
}

var taskLauncher *launcher
var once sync.Once

// InitTaskLauncher initializes a Task Launcher
func InitTaskLauncher(
	d yarpc.Dispatcher,
	resMgrClientName string,
	hostMgrClientName string,
	taskStore storage.TaskStore,
	config *jobmgr.Config,
	parent tally.Scope,
) {
	once.Do(func() {
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
					log.WithError(err).Error("Fail to get placements")
					continue
				}
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

	log.WithFields(log.Fields{
		"num_placements": len(response.Placements),
		"duration":       callDuration.Seconds(),
	}).Info("GetPlacements")

	// TODO: turn getplacement metric into gauge so we can
	//       get the current get_placements counts
	l.metrics.GetPlacement.Inc(int64(len(response.Placements)))
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
		tasks := l.getLaunchableTasks(placement.Tasks)
		err := l.launchTasks(tasks, placement)
		if err != nil {
			log.WithFields(log.Fields{
				"placement": placement,
				"error":     err.Error(),
			}).Error("Failed to launch tasks")

			return err
		}
	}
	return nil
}

func (l *launcher) getLaunchableTasks(tasks []*peloton.TaskID) []*hostsvc.LaunchableTask {
	var tasksInfo []*task.TaskInfo
	log.WithField("Length of Tasks", len(tasks)).
		Debug("getTasks: Length for tasks for placement")

	getTaskInfoStart := time.Now()
	for _, taskID := range tasks {
		// TODO: we need to check if we can launch task to verify the goal state
		taskInfo, err := l.taskStore.GetTaskByID(taskID.Value)
		if err != nil {
			log.WithField("Task Id", taskID.Value).Error("Not able to get Task")
		}
		tasksInfo = append(tasksInfo, taskInfo)
	}
	getTaskInfoDuration := time.Since(getTaskInfoStart)

	log.WithFields(log.Fields{
		"num_tasks": len(tasks),
		"duration":  getTaskInfoDuration.Seconds(),
	}).Info("GetTaskInfo")

	launchableTasks := createLaunchableTasks(tasksInfo)
	return launchableTasks
}

// createLaunchableTasks generates list of hostsvc.LaunchableTask from list of task.TaskInfo
func createLaunchableTasks(tasks []*task.TaskInfo) []*hostsvc.LaunchableTask {
	var launchableTasks []*hostsvc.LaunchableTask
	for _, task := range tasks {
		launchableTask := hostsvc.LaunchableTask{
			TaskId: task.Runtime.TaskId,
			Config: task.GetConfig(),
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
	// TODO: Add retry Logic for tasks launching failure
	placement *resmgr.Placement) error {
	if len(selectedTasks) > 0 {
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
				"tasks": len(selectedTasks),
				"error": err.Error(),
			}).Error("Failed to launch tasks")
			l.metrics.TaskLaunchFail.Inc(1)
			return err
		}

		log.WithField("response", response).Debug("LaunchTasks returned")

		if response.Error != nil {
			log.WithFields(log.Fields{
				"tasks": len(selectedTasks),
				"error": response.Error.String(),
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
	} else {
		log.WithField("remaining_tasks", selectedTasks).Debug("No task is selected to launch")
		return errors.New("No task is selected to launch")
	}
	return nil
}
