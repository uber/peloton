package launcher

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"

	"go.uber.org/yarpc"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/common"

	"code.uber.internal/infra/peloton/common/backoff"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
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
	started       int32
	shutdown      int32
	jobStore      storage.JobStore
	taskStore     storage.TaskStore
	volumeStore   storage.PersistentVolumeStore
	config        *Config
	metrics       *Metrics
	retryPolicy   backoff.RetryPolicy
}

const (
	// Time out for the function to time out
	_timeoutFunctionCall = 120 * time.Second
	_rpcTimeout          = 10 * time.Second
)

var (
	errEmptyTasks         = errors.New("empty tasks infos")
	errLaunchInvalidOffer = errors.New("invalid offer to launch tasks")
)

var taskLauncher *launcher
var onceInitTaskLauncher sync.Once

// InitTaskLauncher initializes a Task Launcher
func InitTaskLauncher(
	d *yarpc.Dispatcher,
	resMgrClientName string,
	hostMgrClientName string,
	jobStore storage.JobStore,
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
			jobStore:      jobStore,
			taskStore:     taskStore,
			volumeStore:   volumeStore,
			config:        config,
			metrics:       NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
			// TODO: make launch retry policy config.
			retryPolicy: backoff.NewRetryPolicy(3, 15*time.Second),
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
					log.WithError(err).Error("jobmgr failed to dequeue placements")
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
	ctx, cancelFunc := context.WithTimeout(context.Background(), _timeoutFunctionCall)
	defer cancelFunc()

	request := &resmgrsvc.GetPlacementsRequest{
		Limit:   uint32(l.config.PlacementDequeueLimit),
		Timeout: uint32(l.config.GetPlacementsTimeout),
	}

	callStart := time.Now()
	response, err := l.resMgrClient.GetPlacements(ctx, request)
	callDuration := time.Since(callStart)

	if err != nil {
		l.metrics.GetPlacementFail.Inc(1)
		return nil, err
	}

	if response.GetError() != nil {
		l.metrics.GetPlacementFail.Inc(1)
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
	pelotonTaskID := util.BuildTaskID(taskInfo.GetJobId(), taskInfo.GetInstanceId())
	taskRuntime := taskInfo.GetRuntime()
	launchableTasks, _, err := l.getLaunchableTasks(
		ctx, []*peloton.TaskID{pelotonTaskID},
		taskRuntime.GetHost(),
		taskRuntime.GetAgentID(),
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
		ctx, launchableTasks, taskRuntime.GetHost(), selectedPorts, false)
	return err
}

func (l *launcher) isLauncherRetryableError(err error) bool {
	switch err {
	case errLaunchInvalidOffer:
		return false
	}

	log.WithError(err).Warn("task launch need to be retried")
	l.metrics.TaskLaunchRetry.Inc(1)
	// s.InternalServiceError
	return true
}

// launchTasks launches tasks to host manager
func (l *launcher) processPlacements(ctx context.Context, placements []*resmgr.Placement) error {
	log.WithField("placements", placements).Debug("Start processing placements")
	for _, placement := range placements {
		go func(placement *resmgr.Placement) {
			tasks, taskInfos, err := l.getLaunchableTasks(
				ctx,
				placement.GetTasks(),
				placement.GetHostname(),
				placement.GetAgentId(),
				placement.GetPorts())
			if err != nil {
				err = l.tryReturnOffers(ctx, err, placement)
				if err != nil {
					log.WithError(err).WithFields(log.Fields{
						"placement":   placement,
						"tasks_total": len(tasks),
					}).Error("Failed to get launchable tasks")
				}
				return
			}
			l.metrics.LauncherGoRoutines.Inc(1)
			err = l.launchTasks(ctx, tasks, placement)
			err = l.tryReturnOffers(ctx, err, placement)
			if err != nil {
				log.WithError(err).WithFields(log.Fields{
					"placement":   placement,
					"tasks_total": len(tasks),
				}).Error("failed to launch tasks to hostmgr")

				if err == errLaunchInvalidOffer {
					// enqueue tasks to resmgr upon launch failure due to invalid offer.
					l.metrics.TaskRequeuedOnLaunchFail.Inc(int64(len(tasks)))
					if err = l.enqueueTasks(ctx, taskInfos); err != nil {
						log.WithError(err).WithFields(log.Fields{
							"task_infos":  taskInfos,
							"tasks_total": len(taskInfos),
						}).Error("failed to enqueue tasks back to resmgr")
					}
				}
			}
		}(placement)
	}
	return nil
}

// enqueueTask enqueues given task to resmgr to launch again.
func (l *launcher) enqueueTasks(ctx context.Context, tasks []*task.TaskInfo) error {
	if len(tasks) == 0 {
		return nil
	}

	for _, t := range tasks {
		t.Runtime.State = task.TaskState_INITIALIZED
		util.RegenerateMesosTaskID(t.JobId, t.InstanceId, t.Runtime)
		err := l.taskStore.UpdateTaskRuntime(ctx, t.JobId, t.InstanceId, t.Runtime)
		if err != nil {
			return err
		}
	}
	jobConfig, err := l.jobStore.GetJobConfig(ctx, tasks[0].GetJobId())
	if err == nil {
		err = jobmgr_task.EnqueueGangs(ctx, tasks, jobConfig, l.resMgrClient)
	}
	return err
}

func (l *launcher) getLaunchableTasks(
	ctx context.Context,
	tasks []*peloton.TaskID,
	hostname string,
	agentID *mesos.AgentID,
	selectedPorts []uint32) ([]*hostsvc.LaunchableTask, []*task.TaskInfo, error) {
	portsIndex := 0

	var tasksInfo []*task.TaskInfo
	getTaskInfoStart := time.Now()

	for _, taskID := range tasks {
		for {
			// TODO: We need to add batch api's for getting all tasks in one shot
			taskInfo, err := l.taskStore.GetTaskByID(ctx, taskID)
			if err != nil {
				log.WithError(err).WithField("Task Id", taskID.Value).
					Error("Not able to get Task")
				break
			}

			if taskInfo.GetRuntime().GetGoalState() == task.TaskGoalState_KILL {
				log.WithField("task_id", taskID.Value).Info("skipping launch of killed task")
				break
			}

			// Generate volume ID if not set for stateful task.
			if taskInfo.GetConfig().GetVolume() != nil {
				if taskInfo.GetRuntime().GetVolumeID() == nil || hostname != taskInfo.GetRuntime().GetHost() {
					// Generates volume ID if first time launch the stateful task,
					// OR task is being launched to a different host.
					taskInfo.GetRuntime().VolumeID = &peloton.VolumeID{
						Value: uuid.New(),
					}
				}
			}

			taskInfo.GetRuntime().Host = hostname
			taskInfo.GetRuntime().AgentID = agentID
			taskInfo.GetRuntime().State = task.TaskState_LAUNCHED
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
						return nil, nil, errors.New("invalid placement")
					}
					taskInfo.GetRuntime().Ports[portConfig.GetName()] = selectedPorts[portsIndex]
					portsIndex++
				}
			}

			// Writes the hostname and ports information back to db.
			err = l.taskStore.UpdateTaskRuntime(ctx, taskInfo.JobId, taskInfo.InstanceId, taskInfo.Runtime)
			if err == nil {
				tasksInfo = append(tasksInfo, taskInfo)
				break
			} else if !common.IsTransientError(err) {
				log.WithError(err).WithField("task_info", taskInfo).
					Error("Not able to update Task")
				break
			}
		}
	}

	if len(tasksInfo) == 0 {
		return nil, nil, errEmptyTasks
	}

	getTaskInfoDuration := time.Since(getTaskInfoStart)

	log.WithFields(log.Fields{
		"num_tasks": len(tasks),
		"duration":  getTaskInfoDuration.Seconds(),
	}).Debug("GetTaskInfo")

	l.metrics.GetDBTaskInfo.Record(getTaskInfoDuration)

	launchableTasks := createLaunchableTasks(tasksInfo)
	return launchableTasks, tasksInfo, nil
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
	ctx context.Context,
	selectedTasks []*hostsvc.LaunchableTask,
	hostname string,
	selectedPorts []uint32,
	checkVolume bool) error {
	ctx, cancel := context.WithTimeout(ctx, _rpcTimeout)
	defer cancel()

	volumeID := &peloton.VolumeID{
		Value: selectedTasks[0].GetVolume().GetId().GetValue(),
	}
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
	ctx context.Context,
	selectedTasks []*hostsvc.LaunchableTask,
	placement *resmgr.Placement) error {
	ctx, cancel := context.WithTimeout(ctx, _rpcTimeout)
	defer cancel()
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
		log.WithFields(log.Fields{
			"response":  response,
			"placement": placement,
		}).Error("hostmgr launch tasks got error resp")
		if response.GetError().GetInvalidOffers() != nil {
			return errLaunchInvalidOffer
		}
		return errors.New(response.Error.String())
	}
	return nil
}

func (l *launcher) launchTasks(
	ctx context.Context,
	selectedTasks []*hostsvc.LaunchableTask,
	placement *resmgr.Placement) error {
	// TODO: Add retry Logic for tasks launching failure
	if len(selectedTasks) == 0 {
		return errEmptyTasks
	}

	log.WithField("tasks", selectedTasks).Debug("Launching Tasks")

	callStart := time.Now()
	var err error
	if placement.Type != resmgr.TaskType_STATEFUL {
		err = backoff.Retry(
			func() error {
				return l.launchBatchTasks(ctx, selectedTasks, placement)
			}, l.retryPolicy, l.isLauncherRetryableError)
	} else {
		err = l.launchStatefulTasks(
			ctx,
			selectedTasks,
			placement.GetHostname(),
			placement.GetPorts(),
			true,
		)
	}
	callDuration := time.Since(callStart)

	if err != nil {
		l.metrics.TaskLaunchFail.Inc(1)
		return err
	}

	l.metrics.TaskLaunch.Inc(int64(len(selectedTasks)))

	log.WithFields(log.Fields{
		"num_tasks": len(selectedTasks),
		"placement": placement,
		"hostname":  placement.GetHostname(),
		"duration":  callDuration.Seconds(),
	}).Debug("Launched tasks")
	l.metrics.LaunchTasksCallDuration.Record(callDuration)
	return nil
}

func (l *launcher) tryReturnOffers(ctx context.Context, err error, placement *resmgr.Placement) error {
	if err == errEmptyTasks {
		request := &hostsvc.ReleaseHostOffersRequest{
			HostOffers: []*hostsvc.HostOffer{{
				Hostname: placement.Hostname,
				AgentId:  placement.AgentId,
			}},
		}
		ctx, cancel := context.WithTimeout(ctx, _rpcTimeout)
		defer cancel()
		_, err = l.hostMgrClient.ReleaseHostOffers(ctx, request)
	}
	return err
}
