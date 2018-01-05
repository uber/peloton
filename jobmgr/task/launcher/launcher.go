package launcher

import (
	"context"
	"sync"
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

	"code.uber.internal/infra/peloton/common/backoff"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

// Launcher defines the interface of task launcher which launches
// tasks from the placed queues of resource pool
type Launcher interface {
	// ProcessPlacement launches tasks to hostmgr
	ProcessPlacement(ctx context.Context, tasks []*hostsvc.LaunchableTask, placement *resmgr.Placement) error
	// LaunchStatefulTasks launches stateful task with reserved resource to hostmgr directly.
	LaunchStatefulTasks(ctx context.Context, selectedTasks []*hostsvc.LaunchableTask, hostname string, selectedPorts []uint32, checkVolume bool) error
	// GetLaunchableTasks returns launchable tasks after updating their runtime state with the placement information
	GetLaunchableTasks(ctx context.Context, tasks []*peloton.TaskID, hostname string, agentID *mesos.AgentID, selectedPorts []uint32) (map[string]*task.TaskInfo, error)
	// TryReturnOffers returns the offers in the placement back to host manager
	TryReturnOffers(ctx context.Context, err error, placement *resmgr.Placement) error
}

// launcher implements the Launcher interface
type launcher struct {
	sync.Mutex
	hostMgrClient hostsvc.InternalHostServiceYARPCClient
	taskStore     storage.TaskStore
	volumeStore   storage.PersistentVolumeStore
	metrics       *Metrics
	retryPolicy   backoff.RetryPolicy
}

const (
	// Time out for the function to time out
	_rpcTimeout = 10 * time.Second
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
	hostMgrClientName string,
	taskStore storage.TaskStore,
	volumeStore storage.PersistentVolumeStore,
	parent tally.Scope,
) {
	onceInitTaskLauncher.Do(func() {
		if taskLauncher != nil {
			log.Warning("Task launcher has already been initialized")
			return
		}

		taskLauncher = &launcher{
			hostMgrClient: hostsvc.NewInternalHostServiceYARPCClient(d.ClientConfig(hostMgrClientName)),
			taskStore:     taskStore,
			volumeStore:   volumeStore,
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

// ProcessPlacements launches tasks to host manager
func (l *launcher) ProcessPlacement(ctx context.Context, tasks []*hostsvc.LaunchableTask, placement *resmgr.Placement) error {
	l.metrics.LauncherGoRoutines.Inc(1)
	err := l.launchTasks(ctx, tasks, placement)
	l.TryReturnOffers(ctx, err, placement)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"placement":   placement,
			"tasks_total": len(tasks),
		}).Error("failed to launch tasks to hostmgr")
		l.metrics.TaskRequeuedOnLaunchFail.Inc(int64(len(tasks)))
	}
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

func (l *launcher) GetLaunchableTasks(
	ctx context.Context,
	tasks []*peloton.TaskID,
	hostname string,
	agentID *mesos.AgentID,
	selectedPorts []uint32) (map[string]*task.TaskInfo, error) {
	portsIndex := 0

	tasksInfo := make(map[string]*task.TaskInfo)
	getTaskInfoStart := time.Now()

	for _, taskID := range tasks {
		// TODO: We need to add batch api's for getting all tasks in one shot
		taskInfo, err := l.taskStore.GetTaskByID(ctx, taskID.GetValue())
		if err != nil {
			log.WithError(err).WithField("Task Id", taskID.GetValue()).
				Error("Not able to get Task")
			continue
		}

		// Generate volume ID if not set for stateful task.
		if taskInfo.GetConfig().GetVolume() != nil {
			if taskInfo.GetRuntime().GetVolumeID() == nil || hostname != taskInfo.GetRuntime().GetHost() {
				newVolumeID := &peloton.VolumeID{
					Value: uuid.New(),
				}
				log.WithFields(log.Fields{
					"task":          taskInfo,
					"hostname":      hostname,
					"new_volume_id": newVolumeID,
				}).Info("generates new volume id for task")
				// Generates volume ID if first time launch the stateful task,
				// OR task is being launched to a different host.
				taskInfo.GetRuntime().VolumeID = newVolumeID
			}
		}

		if taskInfo.GetRuntime().GetGoalState() != task.TaskState_KILLED {
			taskInfo.GetRuntime().Host = hostname
			taskInfo.GetRuntime().AgentID = agentID
			taskInfo.GetRuntime().State = task.TaskState_LAUNCHED
		}

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

		taskInfo.GetRuntime().Message = "Add hostname and ports"
		taskInfo.GetRuntime().Reason = "REASON_UPDATE_OFFER"
		tasksInfo[taskID.GetValue()] = taskInfo
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

	return tasksInfo, nil
}

// CreateLaunchableTasks generates list of hostsvc.LaunchableTask from list of task.TaskInfo
func CreateLaunchableTasks(tasks map[string]*task.TaskInfo) []*hostsvc.LaunchableTask {
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

func (l *launcher) LaunchStatefulTasks(
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

	log.WithFields(log.Fields{
		"request":        request,
		"selected_tasks": selectedTasks,
	}).Info("OfferOperations called")

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
		err = l.LaunchStatefulTasks(
			ctx,
			selectedTasks,
			placement.GetHostname(),
			placement.GetPorts(),
			true, /* checkVolume */
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

func (l *launcher) TryReturnOffers(ctx context.Context, err error, placement *resmgr.Placement) error {
	if err != nil && err != errLaunchInvalidOffer {
		request := &hostsvc.ReleaseHostOffersRequest{
			HostOffers: []*hostsvc.HostOffer{{
				Hostname: placement.Hostname,
				AgentId:  placement.AgentId,
			}},
		}
		ctx, cancel := context.WithTimeout(ctx, _rpcTimeout)
		defer cancel()
		_, err = l.hostMgrClient.ReleaseHostOffers(ctx, request)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"hostname": placement.Hostname,
				"agentid":  placement.AgentId,
			}).Error("failed to release host offers")
		}
	}
	return err
}
