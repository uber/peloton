package task

import (
	"context"
	"fmt"
	"net/http"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/jobmgr/log_manager"
	"code.uber.internal/infra/peloton/storage"

	"code.uber.internal/infra/peloton/.gen/peloton/api/errors"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
)

const (
	stopRequestTimeoutSecs = 10
	httpClientTimeout      = 10 * time.Second
)

// InitServiceHandler initializes the TaskManager
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	hostmgrClientName string) {

	handler := &serviceHandler{
		taskStore:     taskStore,
		jobStore:      jobStore,
		metrics:       NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
		hostmgrClient: hostsvc.NewInternalHostServiceYarpcClient(d.ClientConfig(hostmgrClientName)),
		httpClient:    &http.Client{Timeout: httpClientTimeout},
	}

	d.Register(task.BuildTaskManagerYarpcProcedures(handler))
}

// serviceHandler implements peloton.api.task.TaskManager
type serviceHandler struct {
	taskStore     storage.TaskStore
	jobStore      storage.JobStore
	metrics       *Metrics
	hostmgrClient hostsvc.InternalHostServiceYarpcClient
	httpClient    *http.Client
}

func (m *serviceHandler) Get(
	ctx context.Context,
	body *task.GetRequest) (*task.GetResponse, error) {

	log.Infof("TaskManager.Get called: %v", body)
	m.metrics.TaskAPIGet.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(ctx, body.JobId)
	if err != nil || jobConfig == nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		return &task.GetResponse{
			NotFound: &errors.JobNotFound{
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

	log.Infof("TaskManager.List called: %v", body)
	m.metrics.TaskAPIList.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(ctx, body.JobId)
	if err != nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		m.metrics.TaskListFail.Inc(1)
		return &task.ListResponse{
			NotFound: &errors.JobNotFound{
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
			NotFound: &errors.JobNotFound{
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

func (m *serviceHandler) Start(
	ctx context.Context,
	body *task.StartRequest) (*task.StartResponse, error) {

	log.Infof("TaskManager.Start called: %v", body)
	m.metrics.TaskAPIStart.Inc(1)
	m.metrics.TaskStart.Inc(1)
	return &task.StartResponse{}, nil
}

// Stop implements TaskManager.Stop, tries to stop tasks in a given job.
func (m *serviceHandler) Stop(
	ctx context.Context,
	body *task.StopRequest) (*task.StopResponse, error) {

	log.WithField("request", body).Info("TaskManager.Stop called")
	m.metrics.TaskAPIStop.Inc(1)
	ctx, cancelFunc := context.WithTimeout(
		ctx,
		stopRequestTimeoutSecs*time.Second,
	)
	defer cancelFunc()

	jobConfig, err := m.jobStore.GetJobConfig(ctx, body.JobId)
	if err != nil || jobConfig == nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		return &task.StopResponse{
			Error: &task.StopResponse_Error{
				NotFound: &errors.JobNotFound{
					Id:      body.JobId,
					Message: fmt.Sprintf("job %v not found, %v", body.JobId, err),
				},
			},
		}, nil
	}

	var instanceIds []uint32
	for _, taskRange := range body.GetRanges() {
		// Need to do this check as the CLI may send default instance Range (0, MaxUnit32)
		// and  C* store would error out if it cannot find a instance id. A separate
		// task is filed on the CLI side.
		from := taskRange.GetFrom()
		for from < taskRange.GetTo() {
			instanceIds = append(instanceIds, from)
			from++
		}
	}
	taskInfos := make(map[uint32]*task.TaskInfo)
	var invalidInstanceIds []uint32
	// TODO: refactor the ranges code to peloton/range subpackage.
	if body.GetRanges() == nil {
		// If no ranges specified, then stop all instances in the given job.
		taskInfos, err = m.taskStore.GetTasksForJob(ctx, body.JobId)
	} else {
		for _, instance := range instanceIds {
			var tasks = make(map[uint32]*task.TaskInfo)
			tasks, err = m.taskStore.GetTaskForJob(ctx, body.JobId, instance)
			if err != nil || len(tasks) != 1 {
				// Do not continue if task db query got error.
				invalidInstanceIds = append(invalidInstanceIds, instance)
				for k := range taskInfos {
					delete(taskInfos, k)
				}
				break
			}
			for inst := range tasks {
				taskInfos[inst] = tasks[inst]
			}
		}
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
			InvalidInstanceIds: invalidInstanceIds,
		}, nil
	}

	// tasksToKill only includes taks ids whose goal state update succeeds.
	var tasksToKill []*mesos.TaskID
	var stoppedInstanceIds []uint32
	// Persist KILLED goalstate for tasks in db.
	for instID, taskInfo := range taskInfos {
		taskID := taskInfo.GetRuntime().GetTaskId()
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
			InvalidInstanceIds: invalidInstanceIds,
			StoppedInstanceIds: stoppedInstanceIds,
		}, nil
	}
	return &task.StopResponse{
		InvalidInstanceIds: invalidInstanceIds,
		StoppedInstanceIds: stoppedInstanceIds,
	}, nil
}

func (m *serviceHandler) Restart(
	ctx context.Context,
	body *task.RestartRequest) (*task.RestartResponse, error) {

	log.Infof("TaskManager.Restart called: %v", body)
	m.metrics.TaskAPIRestart.Inc(1)
	m.metrics.TaskRestart.Inc(1)
	return &task.RestartResponse{}, nil
}

func (m *serviceHandler) Query(
	ctx context.Context,
	body *task.QueryRequest) (*task.QueryResponse, error) {

	log.Infof("TaskManager.Query called: %v", body)
	m.metrics.TaskAPIQuery.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(ctx, body.JobId)
	if err != nil || jobConfig == nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		m.metrics.TaskQueryFail.Inc(1)
		return &task.QueryResponse{
			Error: &task.QueryResponse_Error{
				NotFound: &errors.JobNotFound{
					Id:      body.JobId,
					Message: fmt.Sprintf("Failed to find job with id %v, err=%v", body.JobId, err),
				},
			},
		}, nil
	}

	// TODO: Support filter and order arguments.
	result, total, err := m.taskStore.QueryTasks(ctx, body.JobId, body.Offset, body.Limit)
	if err != nil {
		m.metrics.TaskQueryFail.Inc(1)
		return &task.QueryResponse{
			Error: &task.QueryResponse_Error{
				NotFound: &errors.JobNotFound{
					Id:      body.JobId,
					Message: fmt.Sprintf("err= %v", err),
				},
			},
		}, nil
	}

	m.metrics.TaskQuery.Inc(1)
	return &task.QueryResponse{
		Records: result,
		Offset:  body.Offset,
		Limit:   body.Limit,
		Total:   total,
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
				NotFound: &errors.JobNotFound{
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
	hostname := taskInfo.GetRuntime().GetHost()
	taskID := taskInfo.GetRuntime().GetTaskId().GetValue()
	if len(hostname) == 0 {
		m.metrics.TaskListLogsFail.Inc(1)
		return &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				NotRunning: &task.TaskNotRunning{
					Message: "taskinfo does not have hostname",
				},
			},
		}, nil
	}

	logManager := logmanager.NewLogManager(m.httpClient)
	var logPaths []string
	logPaths, err = logManager.ListSandboxFilesPaths(hostname, taskID)

	if err != nil {
		m.metrics.TaskListLogsFail.Inc(1)
		log.WithField("req", req).
			WithField("hostname", hostname).
			WithField("task_id", taskID).
			WithError(err).
			Error("failed to list slave logs files paths")
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
