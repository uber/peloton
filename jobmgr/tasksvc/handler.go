package tasksvc

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	pb_errors "code.uber.internal/infra/peloton/.gen/peloton/api/errors"
	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/query"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/jobmgr/job"
	"code.uber.internal/infra/peloton/jobmgr/log_manager"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/jobmgr/task/launcher"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

const (
	_rpcTimeout        = 15 * time.Second
	_httpClientTimeout = 15 * time.Second
	_frameworkName     = "Peloton"
)

var (
	errEmptyFrameworkID = errors.New("framework id is empty")
)

// InitServiceHandler initializes the TaskManager
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	frameworkInfoStore storage.FrameworkInfoStore,
	volumeStore storage.PersistentVolumeStore,
	runtimeUpdater *job.RuntimeUpdater,
	mesosAgentWorkDir string) {

	handler := &serviceHandler{
		taskStore:          taskStore,
		jobStore:           jobStore,
		frameworkInfoStore: frameworkInfoStore,
		volumeStore:        volumeStore,
		runtimeUpdater:     runtimeUpdater,
		metrics:            NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
		hostmgrClient:      hostsvc.NewInternalHostServiceYARPCClient(d.ClientConfig(common.PelotonHostManager)),
		resmgrClient:       resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(common.PelotonResourceManager)),
		httpClient:         &http.Client{Timeout: _httpClientTimeout},
		taskLauncher:       launcher.GetLauncher(),
		mesosAgentWorkDir:  mesosAgentWorkDir,
	}
	d.Register(task.BuildTaskManagerYARPCProcedures(handler))
}

// serviceHandler implements peloton.api.task.TaskManager
type serviceHandler struct {
	taskStore          storage.TaskStore
	jobStore           storage.JobStore
	frameworkInfoStore storage.FrameworkInfoStore
	volumeStore        storage.PersistentVolumeStore
	runtimeUpdater     *job.RuntimeUpdater
	metrics            *Metrics
	hostmgrClient      hostsvc.InternalHostServiceYARPCClient
	resmgrClient       resmgrsvc.ResourceManagerServiceYARPCClient
	httpClient         *http.Client
	taskLauncher       launcher.Launcher
	mesosAgentWorkDir  string
}

func (m *serviceHandler) Get(
	ctx context.Context,
	body *task.GetRequest) (*task.GetResponse, error) {

	m.metrics.TaskAPIGet.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(ctx, body.JobId)
	if err != nil || jobConfig == nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		return &task.GetResponse{
			NotFound: &pb_errors.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("job %v not found, %v", body.JobId, err),
			},
		}, nil
	}

	result, err := m.taskStore.GetTaskForJob(ctx, body.JobId, body.InstanceId)
	for _, taskInfo := range result {
		log.Infof("found task %v", taskInfo)
		m.metrics.TaskGet.Inc(1)
		return &task.GetResponse{
			Result: taskInfo,
		}, nil
	}

	m.metrics.TaskGetFail.Inc(1)
	return &task.GetResponse{
		OutOfRange: &task.InstanceIdOutOfRange{
			JobId:         body.JobId,
			InstanceCount: jobConfig.InstanceCount,
		},
	}, nil
}

func (m *serviceHandler) List(
	ctx context.Context,
	body *task.ListRequest) (*task.ListResponse, error) {

	m.metrics.TaskAPIList.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(ctx, body.JobId)
	if err != nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		m.metrics.TaskListFail.Inc(1)
		return &task.ListResponse{
			NotFound: &pb_errors.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("Failed to find job with id %v, err=%v", body.JobId, err),
			},
		}, nil
	}
	var result map[uint32]*task.TaskInfo
	if body.Range == nil {
		result, err = m.taskStore.GetTasksForJob(ctx, body.JobId)
	} else {
		// Need to do this check as the CLI may send default instance Range (0, MaxUnit32)
		// and  C* store would error out if it cannot find a instance id. A separate
		// task is filed on the CLI side.
		if body.Range.To > jobConfig.InstanceCount {
			body.Range.To = jobConfig.InstanceCount
		}
		result, err = m.taskStore.GetTasksForJobByRange(ctx, body.JobId, body.Range)
	}
	if err != nil || len(result) == 0 {
		m.metrics.TaskListFail.Inc(1)
		return &task.ListResponse{
			NotFound: &pb_errors.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("err= %v", err),
			},
		}, nil
	}

	m.metrics.TaskList.Inc(1)
	return &task.ListResponse{
		Result: &task.ListResponse_Result{
			Value: result,
		},
	}, nil
}

// getTaskInfosByRangesFromDB get all the tasks infos for given job and ranges.
func (m *serviceHandler) getTaskInfosByRangesFromDB(
	ctx context.Context,
	jobID *peloton.JobID,
	ranges []*task.InstanceRange,
	jobConfig *pb_job.JobConfig) (map[uint32]*task.TaskInfo, error) {

	taskInfos := make(map[uint32]*task.TaskInfo)
	var err error
	if len(ranges) == 0 {
		// If no ranges specified, then start all instances in the given job.
		taskInfos, err = m.taskStore.GetTasksForJob(ctx, jobID)
	} else {
		var tmpTaskInfos map[uint32]*task.TaskInfo
		for _, taskRange := range ranges {
			// Need to do this check as the CLI may send default instance Range (0, MaxUnit32)
			// and  C* store would error out if it cannot find a instance id. A separate
			// task is filed on the CLI side.
			if taskRange.GetTo() > jobConfig.InstanceCount {
				taskRange.To = jobConfig.InstanceCount
			}
			tmpTaskInfos, err = m.taskStore.GetTasksForJobByRange(ctx, jobID, taskRange)
			if err != nil {
				return taskInfos, err
			}
			for inst := range tmpTaskInfos {
				taskInfos[inst] = tmpTaskInfos[inst]
			}
		}
	}

	return taskInfos, nil
}

// Start implements TaskManager.Start, tries to start terminal tasks in a given job.
func (m *serviceHandler) Start(
	ctx context.Context,
	body *task.StartRequest) (*task.StartResponse, error) {

	m.metrics.TaskAPIStart.Inc(1)
	ctx, cancelFunc := context.WithTimeout(
		ctx,
		_rpcTimeout,
	)
	defer cancelFunc()

	jobConfig, err := m.jobStore.GetJobConfig(ctx, body.GetJobId())
	if err != nil || jobConfig == nil {
		log.WithField("job", body.JobId).
			WithError(err).
			Error("failed to get job from db")
		return &task.StartResponse{
			Error: &task.StartResponse_Error{
				NotFound: &pb_errors.JobNotFound{
					Id:      body.JobId,
					Message: err.Error(),
				},
			},
		}, nil
	}

	taskInfos, err := m.getTaskInfosByRangesFromDB(
		ctx, body.GetJobId(), body.GetRanges(), jobConfig)
	if err != nil {
		log.WithField("job", body.JobId).
			WithError(err).
			Error("failed to get tasks for job in db")
		return &task.StartResponse{
			Error: &task.StartResponse_Error{
				OutOfRange: &task.InstanceIdOutOfRange{
					JobId:         body.JobId,
					InstanceCount: jobConfig.InstanceCount,
				},
			},
		}, nil
	}

	// tasksToStart includes tasks need to be enqueued to resmgr.
	var tasksToStart []*task.TaskInfo
	var statefulTasksToStart []*task.TaskInfo
	var startedInstanceIds []uint32
	for instID, taskInfo := range taskInfos {
		taskRuntime := taskInfo.GetRuntime()
		// Skip start task if current state is not terminal state.
		// TODO(mu): LAUNCHING state is exception here because task occasionally
		// stuck at LAUNCHING state but we don't have good retry module yet.
		if !util.IsPelotonStateTerminal(taskRuntime.GetState()) {
			if taskRuntime.GetState() != task.TaskState_LAUNCHING {
				continue
			}
			// for LAUNCHING state task, do not generate new mesos task
			// id but only update runtime state.
			taskInfo.GetRuntime().State = task.TaskState_INITIALIZED
		} else {
			// Only regenerate mesos task id for terminated task.
			util.RegenerateMesosTaskID(taskInfo)
		}

		// First change goalstate if it is KILLED.
		if taskInfo.GetRuntime().GoalState == task.TaskState_KILLED {
			if jobConfig.GetType() == pb_job.JobType_SERVICE {
				taskInfo.GetRuntime().GoalState = task.TaskState_RUNNING
			} else {
				taskInfo.GetRuntime().GoalState = task.TaskState_SUCCEEDED
			}
		}

		err = m.taskStore.UpdateTask(ctx, taskInfo)
		if err != nil {
			// Skip remaining tasks starting if db update error occurs.
			log.WithError(err).
				WithField("task", taskInfo).
				Error("Failed to update task to INITIALIZED state")
			break
		}

		startedInstanceIds = append(startedInstanceIds, instID)
		if taskInfo.GetConfig().GetVolume() != nil && len(taskInfo.GetRuntime().GetVolumeID().GetValue()) > 0 {
			pv, err := m.volumeStore.GetPersistentVolume(
				ctx, taskInfo.GetRuntime().GetVolumeID().GetValue())
			if err != nil {
				_, ok := err.(*storage.VolumeNotFoundError)
				if !ok {
					// volume store db read error.
					log.WithError(err).
						WithField("task", taskInfo).
						Error("failed to read volume information from db")
					break
				}
				// volume not exist so treat as normal task going through placement.
			} else if pv.GetState() == volume.VolumeState_CREATED {
				// volume is in CREATED state so that launch the task directly to hostmgr.
				statefulTasksToStart = append(statefulTasksToStart, taskInfo)
				continue
			}
		}
		tasksToStart = append(tasksToStart, taskInfo)
	}

	if len(tasksToStart) > 0 {
		// For batch/stateless tasks, enqueue them to resmgr to go through
		// placement to get offers/host to launch.
		err = jobmgr_task.EnqueueGangs(ctx, tasksToStart, jobConfig, m.resmgrClient)
		if err == nil {
			err = m.runtimeUpdater.UpdateJob(ctx, body.GetJobId())
		}
		if err != nil {
			log.WithError(err).
				WithField("tasks", tasksToStart).
				Error("failed to start tasks")
			m.metrics.TaskStartFail.Inc(1)
			return &task.StartResponse{
				Error: &task.StartResponse_Error{
					Failure: &task.TaskStartFailure{
						Message: fmt.Sprintf("tasks start failed for %v", err),
					},
				},
				StartedInstanceIds: startedInstanceIds,
			}, nil
		}
	}

	if len(statefulTasksToStart) > 0 {
		// for stateful task with volume created, we should launch task directly to hostmgr.
		for _, taskInfo := range statefulTasksToStart {
			err = m.taskLauncher.LaunchTaskWithReservedResource(ctx, taskInfo)
			if err != nil {
				log.WithError(err).WithFields(log.Fields{
					"task_info": taskInfo,
				}).Error("Failed to launch stateful tasks")
				m.metrics.TaskStartFail.Inc(1)
				return &task.StartResponse{
					Error: &task.StartResponse_Error{
						Failure: &task.TaskStartFailure{
							Message: fmt.Sprintf("stateful tasks start failed for %v", err),
						},
					},
					StartedInstanceIds: startedInstanceIds,
				}, nil
			}
		}
	}

	m.metrics.TaskStart.Inc(1)
	return &task.StartResponse{
		StartedInstanceIds: startedInstanceIds,
	}, nil
}

// Stop implements TaskManager.Stop, tries to stop tasks in a given job.
func (m *serviceHandler) Stop(
	ctx context.Context,
	body *task.StopRequest) (*task.StopResponse, error) {

	log.WithField("request", body).Info("TaskManager.Stop called")
	m.metrics.TaskAPIStop.Inc(1)
	ctx, cancelFunc := context.WithTimeout(
		ctx,
		_rpcTimeout,
	)
	defer cancelFunc()

	jobConfig, err := m.jobStore.GetJobConfig(ctx, body.GetJobId())
	if err != nil {
		log.WithField("job", body.JobId).
			WithError(err).
			Error("failed to get job from db")
		return &task.StopResponse{
			Error: &task.StopResponse_Error{
				NotFound: &pb_errors.JobNotFound{
					Id:      body.JobId,
					Message: err.Error(),
				},
			},
		}, nil
	}

	taskInfos, err := m.getTaskInfosByRangesFromDB(
		ctx, body.GetJobId(), body.GetRanges(), jobConfig)
	if err != nil {
		log.WithField("job", body.JobId).
			WithError(err).
			Error("failed to get tasks for job in db")
		return &task.StopResponse{
			Error: &task.StopResponse_Error{
				OutOfRange: &task.InstanceIdOutOfRange{
					JobId:         body.JobId,
					InstanceCount: jobConfig.InstanceCount,
				},
			},
		}, nil
	}

	if err != nil || len(taskInfos) == 0 {
		log.WithField("job", body.JobId).
			WithError(err).
			Error("Failed to get tasks to stop for job")
		return &task.StopResponse{
			Error: &task.StopResponse_Error{
				OutOfRange: &task.InstanceIdOutOfRange{
					JobId:         body.JobId,
					InstanceCount: jobConfig.InstanceCount,
				},
			},
		}, nil
	}

	// tasksToKill only includes taks ids whose goal state update succeeds.
	var tasksToKill []*mesos.TaskID
	var stoppedInstanceIds []uint32
	// Persist KILLED goalstate for tasks in db.
	for instID, taskInfo := range taskInfos {
		taskID := taskInfo.GetRuntime().GetMesosTaskId()
		// Skip update task goalstate if it is already KILLED.
		if taskInfo.GetRuntime().GoalState != task.TaskState_KILLED {
			taskInfo.GetRuntime().GoalState = task.TaskState_KILLED
			err = m.taskStore.UpdateTask(ctx, taskInfo)
			if err != nil {
				// Skip remaining tasks killing if db update error occurs.
				log.WithError(err).
					WithField("task_id", taskID).
					Error("Failed to update KILLED goalstate")
				break
			}
		}
		curState := taskInfo.GetRuntime().GetState()
		// Only kill tasks that is actively starting/running.
		switch curState {
		case task.TaskState_LAUNCHING, task.TaskState_RUNNING:
			tasksToKill = append(tasksToKill, taskID)
		}
		stoppedInstanceIds = append(stoppedInstanceIds, instID)
		m.metrics.TaskStop.Inc(1)
	}

	// TODO(mu): Notify RM to also remove these tasks from task queue.
	// TODO(mu): Add kill retry module since the kill msg to mesos could get lost.
	if len(tasksToKill) > 0 {
		log.WithField("tasks_to_kill", tasksToKill).Info("Call HM to kill tasks.")
		var request = &hostsvc.KillTasksRequest{
			TaskIds: tasksToKill,
		}
		_, err := m.hostmgrClient.KillTasks(ctx, request)
		if err != nil {
			log.WithError(err).
				WithField("tasks", tasksToKill).
				Error("Failed to kill tasks on host manager")
		}
	}

	if err != nil {
		return &task.StopResponse{
			Error: &task.StopResponse_Error{
				UpdateError: &task.TaskUpdateError{
					Message: fmt.Sprintf("Goalstate update failed for %v", err),
				},
			},
			StoppedInstanceIds: stoppedInstanceIds,
		}, nil
	}
	return &task.StopResponse{
		StoppedInstanceIds: stoppedInstanceIds,
	}, nil
}

func (m *serviceHandler) Restart(
	ctx context.Context,
	body *task.RestartRequest) (*task.RestartResponse, error) {

	m.metrics.TaskAPIRestart.Inc(1)
	m.metrics.TaskRestart.Inc(1)
	return &task.RestartResponse{}, nil
}

func (m *serviceHandler) Query(ctx context.Context, req *task.QueryRequest) (*task.QueryResponse, error) {
	m.metrics.TaskAPIQuery.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(ctx, req.JobId)
	if err != nil || jobConfig == nil {
		log.Errorf("Failed to find job with id %v, err=%v", req.JobId, err)
		m.metrics.TaskQueryFail.Inc(1)
		return &task.QueryResponse{
			Error: &task.QueryResponse_Error{
				NotFound: &pb_errors.JobNotFound{
					Id:      req.JobId,
					Message: fmt.Sprintf("Failed to find job with id %v, err=%v", req.JobId, err),
				},
			},
		}, nil
	}

	result, total, err := m.taskStore.QueryTasks(ctx, req.JobId, req.GetSpec())
	if err != nil {
		m.metrics.TaskQueryFail.Inc(1)
		return &task.QueryResponse{
			Error: &task.QueryResponse_Error{
				NotFound: &pb_errors.JobNotFound{
					Id:      req.JobId,
					Message: fmt.Sprintf("err= %v", err),
				},
			},
		}, nil
	}

	m.metrics.TaskQuery.Inc(1)
	return &task.QueryResponse{
		Records: result,
		Pagination: &query.Pagination{
			Offset: req.GetSpec().GetPagination().GetOffset(),
			Limit:  req.GetSpec().GetPagination().GetLimit(),
			Total:  total,
		},
	}, nil
}

func (m *serviceHandler) BrowseSandbox(
	ctx context.Context,
	req *task.BrowseSandboxRequest) (*task.BrowseSandboxResponse, error) {

	log.WithField("req", req).Info("taskmanager getlogurls called")
	m.metrics.TaskAPIListLogs.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(ctx, req.JobId)
	if err != nil || jobConfig == nil {
		log.WithField("job_id", req.JobId).
			WithError(err).
			Error("failed to find job with id")
		m.metrics.TaskListLogsFail.Inc(1)
		return &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				NotFound: &pb_errors.JobNotFound{
					Id:      req.JobId,
					Message: fmt.Sprintf("job %v not found, %v", req.JobId, err),
				},
			},
		}, nil
	}

	result, err := m.taskStore.GetTaskForJob(ctx, req.JobId, req.InstanceId)
	if err != nil || len(result) != 1 {
		log.WithField("req", req).
			WithField("result", result).
			WithError(err).
			Error("failed to get task for job")
		m.metrics.TaskListLogsFail.Inc(1)
		return &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				OutOfRange: &task.InstanceIdOutOfRange{
					JobId:         req.JobId,
					InstanceCount: jobConfig.InstanceCount,
				},
			},
		}, nil
	}

	var taskInfo *task.TaskInfo
	for _, t := range result {
		taskInfo = t
	}

	hostname, agentID := taskInfo.GetRuntime().GetHost(), taskInfo.GetRuntime().GetAgentID().GetValue()
	taskID := taskInfo.GetRuntime().GetMesosTaskId().GetValue()
	if len(agentID) == 0 {
		m.metrics.TaskListLogsFail.Inc(1)
		return &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				NotRunning: &task.TaskNotRunning{
					Message: "taskinfo does not have agentID",
				},
			},
		}, nil
	}

	// get framework ID.
	frameworkID, err := m.getFrameworkID(ctx)
	if err != nil {
		m.metrics.TaskListLogsFail.Inc(1)
		log.WithError(err).WithFields(log.Fields{
			"req":       req,
			"task_info": taskInfo,
		}).Error("failed to get framework id")
		return &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				Failure: &task.BrowseSandboxFailure{
					Message: err.Error(),
				},
			},
		}, nil
	}

	logManager := logmanager.NewLogManager(m.httpClient)
	var logPaths []string
	logPaths, err = logManager.ListSandboxFilesPaths(m.mesosAgentWorkDir, frameworkID, hostname, agentID, taskID)

	if err != nil {
		m.metrics.TaskListLogsFail.Inc(1)
		log.WithError(err).WithFields(log.Fields{
			"req":       req,
			"task_info": taskInfo,
		}).Error("failed to list slave logs files paths")
		return &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				Failure: &task.BrowseSandboxFailure{
					Message: fmt.Sprintf(
						"get slave log failed on host:%s due to: %v",
						hostname,
						err,
					),
				},
			},
		}, nil
	}

	m.metrics.TaskListLogs.Inc(1)
	return &task.BrowseSandboxResponse{
		Hostname: hostname,
		Port:     "5051",
		Paths:    logPaths,
	}, nil
}

// GetFrameworkID returns the frameworkID.
func (m *serviceHandler) getFrameworkID(ctx context.Context) (string, error) {
	frameworkIDVal, err := m.frameworkInfoStore.GetFrameworkID(ctx, _frameworkName)
	if err != nil {
		return frameworkIDVal, err
	}
	if frameworkIDVal == "" {
		return frameworkIDVal, errEmptyFrameworkID
	}
	return frameworkIDVal, nil
}
