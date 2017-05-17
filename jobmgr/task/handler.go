package task

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/storage"
	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"

	"code.uber.internal/infra/peloton/.gen/peloton/api/errors"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
)

const (
	stopRequestTimeoutSecs = 10
)

// InitServiceHandler initializes the TaskManager
func InitServiceHandler(
	d yarpc.Dispatcher,
	parent tally.Scope,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	hostmgrClientName string) {

	handler := serviceHandler{
		taskStore:     taskStore,
		jobStore:      jobStore,
		metrics:       NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
		rootCtx:       context.Background(),
		hostmgrClient: json.New(d.ClientConfig(hostmgrClientName)),
	}
	json.Register(d, json.Procedure("TaskManager.Get", handler.Get))
	json.Register(d, json.Procedure("TaskManager.List", handler.List))
	json.Register(d, json.Procedure("TaskManager.Start", handler.Start))
	json.Register(d, json.Procedure("TaskManager.Stop", handler.Stop))
	json.Register(d, json.Procedure("TaskManager.Restart", handler.Restart))
	json.Register(d, json.Procedure("TaskManager.Query", handler.Query))
}

// serviceHandler implements peloton.api.task.TaskManager
type serviceHandler struct {
	taskStore     storage.TaskStore
	jobStore      storage.JobStore
	metrics       *Metrics
	rootCtx       context.Context
	hostmgrClient json.Client
}

func (m *serviceHandler) Get(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.GetRequest) (*task.GetResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Get called: %v", body)
	m.metrics.TaskAPIGet.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(body.JobId)
	if err != nil || jobConfig == nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		return &task.GetResponse{
			NotFound: &errors.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("job %v not found, %v", body.JobId, err),
			},
		}, nil, nil
	}

	result, err := m.taskStore.GetTaskForJob(body.JobId, body.InstanceId)
	for _, taskInfo := range result {
		log.Infof("found task %v", taskInfo)
		m.metrics.TaskGet.Inc(1)
		return &task.GetResponse{
			Result: taskInfo,
		}, nil, nil
	}

	m.metrics.TaskGetFail.Inc(1)
	return &task.GetResponse{
		OutOfRange: &task.InstanceIdOutOfRange{
			JobId:         body.JobId,
			InstanceCount: jobConfig.InstanceCount,
		},
	}, nil, nil
}

func (m *serviceHandler) List(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.ListRequest) (*task.ListResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.List called: %v", body)
	m.metrics.TaskAPIList.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(body.JobId)
	if err != nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		m.metrics.TaskListFail.Inc(1)
		return &task.ListResponse{
			NotFound: &errors.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("Failed to find job with id %v, err=%v", body.JobId, err),
			},
		}, nil, nil
	}
	var result map[uint32]*task.TaskInfo
	if body.Range == nil {
		result, err = m.taskStore.GetTasksForJob(body.JobId)
	} else {
		// Need to do this check as the CLI may send default instance Range (0, MaxUnit32)
		// and  C* store would error out if it cannot find a instance id. A separate
		// task is filed on the CLI side.
		if body.Range.To > jobConfig.InstanceCount {
			body.Range.To = jobConfig.InstanceCount
		}
		result, err = m.taskStore.GetTasksForJobByRange(body.JobId, body.Range)
	}
	if err != nil || len(result) == 0 {
		m.metrics.TaskListFail.Inc(1)
		return &task.ListResponse{
			NotFound: &errors.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("err= %v", err),
			},
		}, nil, nil
	}

	m.metrics.TaskList.Inc(1)
	return &task.ListResponse{
		Result: &task.ListResponse_Result{
			Value: result,
		},
	}, nil, nil
}

func (m *serviceHandler) Start(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.StartRequest) (*task.StartResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Start called: %v", body)
	m.metrics.TaskAPIStart.Inc(1)
	m.metrics.TaskStart.Inc(1)
	return &task.StartResponse{}, nil, nil
}

// Stop implements TaskManager.Stop, tries to stop tasks in a given job.
func (m *serviceHandler) Stop(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.StopRequest) (*task.StopResponse, yarpc.ResMeta, error) {

	log.WithField("request", body).Info("TaskManager.Stop called")
	m.metrics.TaskAPIStop.Inc(1)
	ctx, cancelFunc := context.WithTimeout(
		m.rootCtx,
		stopRequestTimeoutSecs*time.Second,
	)
	defer cancelFunc()

	jobConfig, err := m.jobStore.GetJobConfig(body.JobId)
	if err != nil || jobConfig == nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		return &task.StopResponse{
			Error: &task.StopResponse_Error{
				NotFound: &errors.JobNotFound{
					Id:      body.JobId,
					Message: fmt.Sprintf("job %v not found, %v", body.JobId, err),
				},
			},
		}, nil, nil
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
		taskInfos, err = m.taskStore.GetTasksForJob(body.JobId)
	} else {
		for _, instance := range instanceIds {
			var tasks = make(map[uint32]*task.TaskInfo)
			tasks, err = m.taskStore.GetTaskForJob(body.JobId, instance)
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
		}, nil, nil
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
			err = m.taskStore.UpdateTask(taskInfo)
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
		var response hostsvc.KillTasksResponse
		var request = &hostsvc.KillTasksRequest{
			TaskIds: tasksToKill,
		}
		_, killTasksInHostmgrErr := m.hostmgrClient.Call(
			ctx,
			yarpc.NewReqMeta().Procedure("InternalHostService.KillTasks"),
			request,
			&response,
		)
		if killTasksInHostmgrErr != nil {
			log.WithError(killTasksInHostmgrErr).
				WithField("tasks", tasksToKill).
				Error("Failed to kill tasks on host manager")
		}
	}

	if err != nil {
		return &task.StopResponse{
			Error: &task.StopResponse_Error{
				UpdateError: &task.TaskUpdateError{
					Message: fmt.Sprint("Goalstate update failed for %v", err),
				},
			},
			InvalidInstanceIds: invalidInstanceIds,
			StoppedInstanceIds: stoppedInstanceIds,
		}, nil, nil
	}
	return &task.StopResponse{
		InvalidInstanceIds: invalidInstanceIds,
		StoppedInstanceIds: stoppedInstanceIds,
	}, nil, nil
}

func (m *serviceHandler) Restart(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.RestartRequest) (*task.RestartResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Restart called: %v", body)
	m.metrics.TaskAPIRestart.Inc(1)
	m.metrics.TaskRestart.Inc(1)
	return &task.RestartResponse{}, nil, nil
}

func (m *serviceHandler) Query(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.QueryRequest) (*task.QueryResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Query called: %v", body)
	m.metrics.TaskAPIQuery.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(body.JobId)
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
		}, nil, nil
	}

	// TODO: Support filter and order arguments.
	result, total, err := m.taskStore.QueryTasks(body.JobId, body.Offset, body.Limit)
	if err != nil {
		m.metrics.TaskQueryFail.Inc(1)
		return &task.QueryResponse{
			Error: &task.QueryResponse_Error{
				NotFound: &errors.JobNotFound{
					Id:      body.JobId,
					Message: fmt.Sprintf("err= %v", err),
				},
			},
		}, nil, nil
	}

	m.metrics.TaskQuery.Inc(1)
	return &task.QueryResponse{
		Records: result,
		Offset:  body.Offset,
		Limit:   body.Limit,
		Total:   total,
	}, nil, nil
}
