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

	pb_errors "code.uber.internal/infra/peloton/.gen/peloton/api/errors"
	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/query"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/jobmgr/job"
	"code.uber.internal/infra/peloton/jobmgr/log_manager"
	"code.uber.internal/infra/peloton/jobmgr/task/launcher"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
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
	runtimeUpdater job.RuntimeUpdater,
	trackedManager tracked.Manager,
	mesosAgentWorkDir string) {

	handler := &serviceHandler{
		taskStore:          taskStore,
		jobStore:           jobStore,
		frameworkInfoStore: frameworkInfoStore,
		runtimeUpdater:     runtimeUpdater,
		metrics:            NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
		resmgrClient:       resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(common.PelotonResourceManager)),
		httpClient:         &http.Client{Timeout: _httpClientTimeout},
		taskLauncher:       launcher.GetLauncher(),
		trackedManager:     trackedManager,
		mesosAgentWorkDir:  mesosAgentWorkDir,
	}
	d.Register(task.BuildTaskManagerYARPCProcedures(handler))
}

// serviceHandler implements peloton.api.task.TaskManager
type serviceHandler struct {
	taskStore          storage.TaskStore
	jobStore           storage.JobStore
	frameworkInfoStore storage.FrameworkInfoStore
	runtimeUpdater     job.RuntimeUpdater
	metrics            *Metrics
	resmgrClient       resmgrsvc.ResourceManagerServiceYARPCClient
	httpClient         *http.Client
	taskLauncher       launcher.Launcher
	trackedManager     tracked.Manager
	mesosAgentWorkDir  string
}

func (m *serviceHandler) Get(
	ctx context.Context,
	body *task.GetRequest) (*task.GetResponse, error) {

	m.metrics.TaskAPIGet.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(ctx, body.JobId)
	if err != nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		m.metrics.TaskGetFail.Inc(1)
		return &task.GetResponse{
			NotFound: &pb_errors.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("job %v not found, %v", body.JobId, err),
			},
		}, nil
	}

	result, err := m.taskStore.GetTaskForJob(ctx, body.JobId, body.InstanceId)
	for _, taskInfo := range result {
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

	log.WithField("request", body).Debug("TaskSVC.List called")

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
	resp := &task.ListResponse{
		Result: &task.ListResponse_Result{
			Value: result,
		},
	}
	log.WithField("response", resp).Debug("TaskSVC.List returned")
	return resp, nil
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

	return taskInfos, err
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
	if err != nil {
		log.WithField("job", body.JobId).
			WithError(err).
			Error("failed to get job from db")
		m.metrics.TaskStartFail.Inc(1)
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
		m.metrics.TaskStartFail.Inc(1)
		return &task.StartResponse{
			Error: &task.StartResponse_Error{
				OutOfRange: &task.InstanceIdOutOfRange{
					JobId:         body.JobId,
					InstanceCount: jobConfig.InstanceCount,
				},
			},
		}, nil
	}

	var startedInstanceIds []uint32
	var failedInstanceIds []uint32
	for instID, taskInfo := range taskInfos {
		taskRuntime := taskInfo.GetRuntime()
		// Skip start task if current state is not terminal state.
		// TODO(mu): LAUNCHED state is exception here because task occasionally
		// stuck at LAUNCHED state but we don't have good retry module yet.
		if !util.IsPelotonStateTerminal(taskRuntime.GetState()) {
			if taskRuntime.GetState() != task.TaskState_LAUNCHED {
				continue
			}
			// for LAUNCHED state task, do not generate new mesos task
			// id but only update runtime state.
			taskInfo.GetRuntime().State = task.TaskState_INITIALIZED
		} else {
			// Only regenerate mesos task id for terminated task.
			util.RegenerateMesosTaskID(taskInfo.JobId, taskInfo.InstanceId, taskInfo.Runtime)
		}

		// Change the goalstate.
		if jobConfig.GetType() == pb_job.JobType_SERVICE {
			taskInfo.GetRuntime().GoalState = task.TaskState_RUNNING
		} else {
			taskInfo.GetRuntime().GoalState = task.TaskState_SUCCEEDED
		}
		taskInfo.GetRuntime().Message = "Task start API request"
		taskInfo.GetRuntime().Reason = ""

		err = m.trackedManager.UpdateTaskRuntime(ctx, taskInfo.GetJobId(), instID, taskRuntime)
		if err != nil {
			// If db update error occurs, add it to failedInstanceIds list and continue.
			log.WithError(err).
				WithField("task", taskInfo).
				Error("Failed to update task to INITIALIZED state")
			failedInstanceIds = append(failedInstanceIds, instID)
			m.metrics.TaskStartFail.Inc(1)
			continue
		}

		startedInstanceIds = append(startedInstanceIds, instID)
		m.metrics.TaskStart.Inc(1)
	}

	if err == nil && len(startedInstanceIds) > 0 {
		err = m.runtimeUpdater.UpdateJob(ctx, body.GetJobId())
	}

	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"request":              body,
			"tasks":                taskInfos,
			"started_instance_ids": startedInstanceIds,
		}).Error("failed to start tasks")
		m.metrics.TaskStartFail.Inc(1)
		return &task.StartResponse{
			Error: &task.StartResponse_Error{
				Failure: &task.TaskStartFailure{
					Message: fmt.Sprintf("task start failed for %v", err),
				},
			},
			StartedInstanceIds: startedInstanceIds,
			InvalidInstanceIds: failedInstanceIds,
		}, nil
	}

	return &task.StartResponse{
		StartedInstanceIds: startedInstanceIds,
		InvalidInstanceIds: failedInstanceIds,
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
		m.metrics.TaskStopFail.Inc(1)
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
		m.metrics.TaskStopFail.Inc(1)
		return &task.StopResponse{
			Error: &task.StopResponse_Error{
				OutOfRange: &task.InstanceIdOutOfRange{
					JobId:         body.JobId,
					InstanceCount: jobConfig.InstanceCount,
				},
			},
		}, nil
	}

	// tasksToKill only includes task ids whose goal state update succeeds.
	var stoppedInstanceIds []uint32
	var failedInstanceIds []uint32
	// Persist KILLED goalstate for tasks in db.
	for instID, taskInfo := range taskInfos {
		taskID := taskInfo.GetRuntime().GetMesosTaskId()
		// Skip update task goalstate if it is already KILLED.
		if taskInfo.GetRuntime().GoalState == task.TaskState_KILLED {
			continue
		}

		taskInfo.GetRuntime().GoalState = task.TaskState_KILLED
		taskInfo.GetRuntime().Message = "Task stop API request"
		taskInfo.GetRuntime().Reason = ""
		// TODO: We can retry here in case of conflict.
		err = m.trackedManager.UpdateTaskRuntime(ctx, taskInfo.GetJobId(), instID, taskInfo.GetRuntime())
		if err != nil {
			// If a db update error occurs, add it to failedInstanceIds list and continue.
			log.WithError(err).
				WithField("task_id", taskID).
				Error("Failed to update KILLED goalstate")
			m.metrics.TaskStopFail.Inc(1)
			failedInstanceIds = append(failedInstanceIds, instID)
			continue
		}

		stoppedInstanceIds = append(stoppedInstanceIds, instID)
		m.metrics.TaskStop.Inc(1)
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
		InvalidInstanceIds: failedInstanceIds,
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
	log.WithField("request", req).Info("TaskSVC.Query called")
	m.metrics.TaskAPIQuery.Inc(1)
	_, err := m.jobStore.GetJobConfig(ctx, req.JobId)
	if err != nil {
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
	resp := &task.QueryResponse{
		Records: result,
		Pagination: &query.Pagination{
			Offset: req.GetSpec().GetPagination().GetOffset(),
			Limit:  req.GetSpec().GetPagination().GetLimit(),
			Total:  total,
		},
	}
	log.WithField("response", resp).Debug("TaskSVC.Query returned")
	return resp, nil
}

func (m *serviceHandler) BrowseSandbox(
	ctx context.Context,
	req *task.BrowseSandboxRequest) (*task.BrowseSandboxResponse, error) {

	log.WithField("req", req).Info("TaskSVC.BrowseSandbox called")
	m.metrics.TaskAPIListLogs.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(ctx, req.JobId)
	if err != nil {
		log.WithField("job_id", req.JobId.Value).
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
	resp := &task.BrowseSandboxResponse{
		Hostname: hostname,
		Port:     "5051",
		Paths:    logPaths,
	}
	log.WithField("response", resp).Info("TaskSVC.BrowseSandbox returned")
	return resp, nil
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
