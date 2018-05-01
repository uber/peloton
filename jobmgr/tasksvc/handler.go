package tasksvc

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"

	mesos_v1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	pb_errors "code.uber.internal/infra/peloton/.gen/peloton/api/errors"
	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/query"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/logmanager"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/jobmgr/task/event/statechanges"
	"code.uber.internal/infra/peloton/jobmgr/task/launcher"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

const (
	_rpcTimeout    = 15 * time.Second
	_frameworkName = "Peloton"
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
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	mesosAgentWorkDir string,
	hostMgrClientName string,
	logManager logmanager.LogManager) {

	handler := &serviceHandler{
		taskStore:          taskStore,
		jobStore:           jobStore,
		frameworkInfoStore: frameworkInfoStore,
		metrics:            NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
		resmgrClient:       resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(common.PelotonResourceManager)),
		taskLauncher:       launcher.GetLauncher(),
		jobFactory:         jobFactory,
		goalStateDriver:    goalStateDriver,
		mesosAgentWorkDir:  mesosAgentWorkDir,
		hostMgrClient:      hostsvc.NewInternalHostServiceYARPCClient(d.ClientConfig(hostMgrClientName)),
		logManager:         logManager,
	}
	d.Register(task.BuildTaskManagerYARPCProcedures(handler))
}

// serviceHandler implements peloton.api.task.TaskManager
type serviceHandler struct {
	taskStore          storage.TaskStore
	jobStore           storage.JobStore
	frameworkInfoStore storage.FrameworkInfoStore
	metrics            *Metrics
	resmgrClient       resmgrsvc.ResourceManagerServiceYARPCClient
	taskLauncher       launcher.Launcher
	jobFactory         cached.JobFactory
	goalStateDriver    goalstate.Driver
	mesosAgentWorkDir  string
	hostMgrClient      hostsvc.InternalHostServiceYARPCClient
	logManager         logmanager.LogManager
}

func (m *serviceHandler) getTerminalEvents(eventList []*task.TaskEvent, lastTaskInfo *task.TaskInfo) []*task.TaskInfo {
	var taskInfos []*task.TaskInfo

	sortEventsList := getEventsResponseResult(eventList)
	for _, events := range sortEventsList {
		var terminalEvent *task.TaskEvent

		for i := len(events.GetEvent()) - 1; i >= 0; i-- {
			event := events.GetEvent()[i]
			if util.IsPelotonStateTerminal(event.GetState()) {
				terminalEvent = event
				break
			}
			if i == 0 {
				terminalEvent = events.GetEvent()[len(events.GetEvent())-1]
			}
		}

		mesosID := terminalEvent.GetTaskId().GetValue()
		prevMesosID := terminalEvent.GetPrevTaskId().GetValue()
		agentID := terminalEvent.GetAgentId()
		taskInfos = append(taskInfos, &task.TaskInfo{
			InstanceId: lastTaskInfo.GetInstanceId(),
			JobId:      lastTaskInfo.GetJobId(),
			Config:     lastTaskInfo.GetConfig(),
			Runtime: &task.RuntimeInfo{
				State: terminalEvent.GetState(),
				MesosTaskId: &mesos_v1.TaskID{
					Value: &mesosID,
				},
				Host: terminalEvent.GetHostname(),
				AgentID: &mesos_v1.AgentID{
					Value: &agentID,
				},
				Message: terminalEvent.GetMessage(),
				Reason:  terminalEvent.GetReason(),
				PrevMesosTaskId: &mesos_v1.TaskID{
					Value: &prevMesosID,
				},
			},
		})
	}

	return taskInfos
}

func (m *serviceHandler) Get(
	ctx context.Context,
	body *task.GetRequest) (*task.GetResponse, error) {

	m.metrics.TaskAPIGet.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(ctx, body.JobId)
	if err != nil {
		log.Debug("Failed to find job with id %v, err=%v", body.JobId, err)
		m.metrics.TaskGetFail.Inc(1)
		return &task.GetResponse{
			NotFound: &pb_errors.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("job %v not found, %v", body.JobId, err),
			},
		}, nil
	}

	var lastTaskInfo *task.TaskInfo
	result, err := m.taskStore.GetTaskForJob(ctx, body.JobId, body.InstanceId)
	for _, taskInfo := range result {
		lastTaskInfo = taskInfo
		break
	}

	if lastTaskInfo == nil {
		m.metrics.TaskGetFail.Inc(1)
		return &task.GetResponse{
			OutOfRange: &task.InstanceIdOutOfRange{
				JobId:         body.JobId,
				InstanceCount: jobConfig.InstanceCount,
			},
		}, nil
	}

	eventList, err := m.taskStore.GetTaskEvents(ctx, body.GetJobId(), body.GetInstanceId())
	if err != nil {
		m.metrics.TaskGetFail.Inc(1)
		return &task.GetResponse{
			OutOfRange: &task.InstanceIdOutOfRange{
				JobId:         body.JobId,
				InstanceCount: jobConfig.InstanceCount,
			},
		}, nil
	}

	taskInfos := m.getTerminalEvents(eventList, lastTaskInfo)

	m.metrics.TaskGet.Inc(1)
	return &task.GetResponse{
		Result:  lastTaskInfo,
		Results: taskInfos,
	}, nil
}

func (m *serviceHandler) GetEvents(
	ctx context.Context,
	body *task.GetEventsRequest) (*task.GetEventsResponse, error) {
	m.metrics.TaskAPIGetEvents.Inc(1)
	result, err := m.taskStore.GetTaskEvents(ctx, body.GetJobId(), body.GetInstanceId())
	if err != nil {
		log.WithError(err).
			WithField("job_id", body.GetJobId()).
			Debug("Failed to get task state changes")
		m.metrics.TaskGetEventsFail.Inc(1)
		return &task.GetEventsResponse{
			Error: &task.GetEventsResponse_Error{
				EventError: &task.TaskEventsError{
					Message: fmt.Sprintf("error: %v", err),
				},
			},
		}, nil
	}

	m.metrics.TaskGetEvents.Inc(1)
	return &task.GetEventsResponse{
		Result: getEventsResponseResult(result),
	}, nil
}

func (m *serviceHandler) List(
	ctx context.Context,
	body *task.ListRequest) (*task.ListResponse, error) {

	log.WithField("request", body).Debug("TaskSVC.List called")

	m.metrics.TaskAPIList.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(ctx, body.JobId)
	if err != nil {
		log.Debug("Failed to find job with id %v, err=%v", body.JobId, err)
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

	m.fillTaskInfoFromResourceManager(ctx, body.GetJobId(), convertTaskMapToSlice(result))
	m.metrics.TaskList.Inc(1)
	resp := &task.ListResponse{
		Result: &task.ListResponse_Result{
			Value: result,
		},
	}
	log.WithField("response", resp).Debug("TaskSVC.List returned")
	return resp, nil
}

// Refresh loads the task runtime state from DB, updates the cache,
// and enqueues it to goal state for evaluation.
func (m *serviceHandler) Refresh(ctx context.Context, req *task.RefreshRequest) (*task.RefreshResponse, error) {
	log.WithField("request", req).Debug("TaskSVC.Refresh called")

	m.metrics.TaskAPIRefresh.Inc(1)
	jobConfig, err := m.jobStore.GetJobConfig(ctx, req.GetJobId())
	if err != nil {
		log.WithError(err).
			WithField("job_id", req.GetJobId().GetValue()).
			Error("failed to load job config in executing task")
		m.metrics.TaskRefreshFail.Inc(1)
		return &task.RefreshResponse{}, yarpcerrors.NotFoundErrorf("job not found")
	}

	reqRange := req.GetRange()
	if reqRange == nil {
		reqRange = &task.InstanceRange{
			From: 0,
			To:   jobConfig.InstanceCount,
		}
	}

	if reqRange.To > jobConfig.InstanceCount {
		reqRange.To = jobConfig.InstanceCount
	}

	runtimes, err := m.taskStore.GetTaskRuntimesForJobByRange(ctx, req.GetJobId(), reqRange)
	if err != nil {
		log.WithError(err).
			WithField("job_id", req.GetJobId().GetValue()).
			WithField("range", reqRange).
			Error("failed to load task runtimes in executing task")
		m.metrics.TaskRefreshFail.Inc(1)
		return &task.RefreshResponse{}, yarpcerrors.NotFoundErrorf("tasks not found")
	}
	if len(runtimes) == 0 {
		log.WithError(err).
			WithField("job_id", req.GetJobId().GetValue()).
			WithField("range", reqRange).
			Error("no task runtimes found while executing task")
		m.metrics.TaskRefreshFail.Inc(1)
		return &task.RefreshResponse{}, yarpcerrors.NotFoundErrorf("tasks not found")
	}

	cachedJob := m.jobFactory.AddJob(req.GetJobId())
	cachedJob.UpdateTasks(ctx, runtimes, cached.UpdateCacheOnly)
	for instID := range runtimes {
		m.goalStateDriver.EnqueueTask(req.GetJobId(), instID, time.Now())
	}

	m.metrics.TaskRefresh.Inc(1)
	return &task.RefreshResponse{}, nil
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

	jobRuntime, err := m.jobStore.GetJobRuntime(ctx, body.GetJobId())
	if err != nil {
		log.WithField("job", body.JobId).
			WithError(err).
			Error("failed to get job runtime from db")
		m.metrics.TaskStartFail.Inc(1)
		return &task.StartResponse{
			Error: &task.StartResponse_Error{
				Failure: &task.TaskStartFailure{
					Message: fmt.Sprintf("task start failed while getting job status %v", err),
				},
			},
		}, nil
	}

	if jobConfig.GetType() == pb_job.JobType_SERVICE {
		jobRuntime.GoalState = pb_job.JobState_RUNNING
	} else {
		jobRuntime.GoalState = pb_job.JobState_SUCCEEDED
	}
	jobRuntime.State = pb_job.JobState_PENDING

	// Since no action needs to be taken at a job level, only update the DB
	// and not the cache. Whenever, a job action needs to be taken, the
	// cache is always updated at that time.
	err = m.jobStore.UpdateJobRuntime(ctx, body.GetJobId(), jobRuntime)
	if err != nil {
		log.WithField("job", body.JobId).
			WithError(err).
			Error("failed to set job runtime in db")
		m.metrics.TaskStartFail.Inc(1)
		return &task.StartResponse{
			Error: &task.StartResponse_Error{
				Failure: &task.TaskStartFailure{
					Message: fmt.Sprintf("task start failed while updating job status %v", err),
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
	var instanceIds []uint32
	runtimes := make(map[uint32]*task.RuntimeInfo)
	for _, taskInfo := range taskInfos {
		taskState := taskInfo.GetRuntime().GetState()

		if taskState == task.TaskState_INITIALIZED || taskState == task.TaskState_PENDING ||
			(taskInfo.GetConfig().GetVolume() != nil && len(taskInfo.GetRuntime().GetVolumeID().GetValue()) != 0) {
			// Do not regenerate mesos task ID if task is known that not in mesos yet OR stateful task.
			taskInfo.GetRuntime().State = task.TaskState_INITIALIZED
		} else {
			util.RegenerateMesosTaskID(taskInfo.JobId, taskInfo.InstanceId, taskInfo.Runtime)
		}

		// Change the goalstate.
		taskInfo.GetRuntime().GoalState = jobmgr_task.GetDefaultTaskGoalState(jobConfig.GetType())
		taskInfo.GetRuntime().Message = "Task start API request"
		taskInfo.GetRuntime().Reason = ""
		runtimes[taskInfo.InstanceId] = taskInfo.GetRuntime()

		instanceIds = append(instanceIds, taskInfo.InstanceId)
	}

	cachedJob := m.jobFactory.AddJob(body.GetJobId())
	err = cachedJob.UpdateTasks(ctx, runtimes, cached.UpdateCacheAndDB)
	if err != nil {
		log.WithError(err).
			WithField("runtimes", runtimes).
			WithField("job_id", body.GetJobId().GetValue()).
			Error("failed to update task to initialized state")
		m.metrics.TaskStartFail.Inc(1)
		failedInstanceIds = instanceIds
	} else {
		startedInstanceIds = instanceIds
		m.metrics.TaskStart.Inc(1)
	}

	for _, instID := range startedInstanceIds {
		m.goalStateDriver.EnqueueTask(body.GetJobId(), instID, time.Now())
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

func (m *serviceHandler) stopJob(
	ctx context.Context,
	jobID *peloton.JobID,
	jobConfig *pb_job.JobConfig) (*task.StopResponse, error) {

	var instanceList []uint32
	instanceCount := jobConfig.GetInstanceCount()

	for i := uint32(0); i < instanceCount; i++ {
		instanceList = append(instanceList, i)
	}

	jobRuntime, err := m.jobStore.GetJobRuntime(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			Error("failed to get job run time")
		m.metrics.TaskStopFail.Inc(int64(instanceCount))
		return &task.StopResponse{
			Error: &task.StopResponse_Error{
				UpdateError: &task.TaskUpdateError{
					Message: fmt.Sprintf("Job state fetch failed for %v", err),
				},
			},
			InvalidInstanceIds: instanceList,
		}, nil
	}

	if jobRuntime.GoalState == pb_job.JobState_KILLED {
		return &task.StopResponse{
			StoppedInstanceIds: instanceList,
		}, nil
	}

	jobRuntime.GoalState = pb_job.JobState_KILLED
	jobInfo := &pb_job.JobInfo{
		Id:      jobID,
		Config:  jobConfig,
		Runtime: jobRuntime,
	}
	cachedJob := m.jobFactory.AddJob(jobID)

	err = cachedJob.Update(ctx, jobInfo, cached.UpdateCacheAndDB)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			Error("failed to update job run time")
		m.metrics.TaskStopFail.Inc(int64(instanceCount))
		return &task.StopResponse{
			Error: &task.StopResponse_Error{
				UpdateError: &task.TaskUpdateError{
					Message: fmt.Sprintf("Job state update failed for %v", err),
				},
			},
			InvalidInstanceIds: instanceList,
		}, nil
	}

	m.goalStateDriver.EnqueueJob(jobID, time.Now())

	m.metrics.TaskStop.Inc(int64(instanceCount))
	return &task.StopResponse{
		StoppedInstanceIds: instanceList,
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

	taskRange := body.GetRanges()
	if len(taskRange) == 0 || (len(taskRange) == 1 && taskRange[0].From == 0 && taskRange[0].To >= jobConfig.InstanceCount) {
		// Stop all tasks in a job, stop entire job instead of task by task
		log.WithField("job_id", body.GetJobId().GetValue()).
			Info("stopping all tasks in the job")
		return m.stopJob(ctx, body.GetJobId(), jobConfig)
	}

	taskInfos, err := m.getTaskInfosByRangesFromDB(
		ctx, body.GetJobId(), taskRange, jobConfig)
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
	var instanceIds []uint32
	runtimes := make(map[uint32]*task.RuntimeInfo)
	// Persist KILLED goalstate for tasks in db.
	for _, taskInfo := range taskInfos {
		// Skip update task goalstate if it is already KILLED.
		if taskInfo.GetRuntime().GoalState == task.TaskState_KILLED {
			continue
		}

		taskInfo.GetRuntime().GoalState = task.TaskState_KILLED
		taskInfo.GetRuntime().Message = "Task stop API request"
		taskInfo.GetRuntime().Reason = ""
		runtimes[taskInfo.InstanceId] = taskInfo.GetRuntime()
		instanceIds = append(instanceIds, taskInfo.InstanceId)
	}

	cachedJob := m.jobFactory.AddJob(body.GetJobId())
	err = cachedJob.UpdateTasks(ctx, runtimes, cached.UpdateCacheAndDB)
	if err != nil {
		log.WithError(err).
			WithField("runtimes", runtimes).
			WithField("job_id", body.GetJobId().GetValue()).
			Error("failed to updated killed goalstate")
		failedInstanceIds = instanceIds
		m.metrics.TaskStopFail.Inc(1)
	} else {
		stoppedInstanceIds = instanceIds
		m.metrics.TaskStop.Inc(1)
	}

	for _, instID := range stoppedInstanceIds {
		m.goalStateDriver.EnqueueTask(body.GetJobId(), instID, time.Now())
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
	callStart := time.Now()

	_, err := m.jobStore.GetJobConfig(ctx, req.JobId)
	if err != nil {
		log.Debug("Failed to find job with id %v, err=%v", req.JobId, err)
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

	m.fillTaskInfoFromResourceManager(ctx, req.GetJobId(), result)
	m.metrics.TaskQuery.Inc(1)
	resp := &task.QueryResponse{
		Records: result,
		Pagination: &query.Pagination{
			Offset: req.GetSpec().GetPagination().GetOffset(),
			Limit:  req.GetSpec().GetPagination().GetLimit(),
			Total:  total,
		},
	}
	callDuration := time.Since(callStart)
	m.metrics.TaskQueryHandlerDuration.Record(callDuration)
	log.WithField("response", resp).Debug("TaskSVC.Query returned")
	return resp, nil
}

func (m *serviceHandler) getHostInfoWithTaskID(
	ctx context.Context,
	jobID *peloton.JobID,
	instanceID uint32,
	taskID string) (hostname string, agentID string, err error) {
	var taskEventsList []*task.TaskEvent

	events, err := m.taskStore.GetTaskEvents(ctx, jobID, instanceID)
	if err != nil {
		return "", "", err
	}

	for _, event := range events {
		if event.TaskId.GetValue() == taskID {
			taskEventsList = append(taskEventsList, event)
		}
	}

	var terminalEvent *task.TaskEvent
	sort.Sort(statechanges.TaskEventByTime(taskEventsList))

	for i := len(taskEventsList) - 1; i >= 0; i-- {
		event := taskEventsList[i]
		if util.IsPelotonStateTerminal(event.GetState()) {
			terminalEvent = event
			break
		}
		if i == 0 {
			terminalEvent = taskEventsList[len(taskEventsList)-1]
		}
	}

	hostname = terminalEvent.GetHostname()
	agentID = terminalEvent.GetAgentId()
	return hostname, agentID, nil
}

func (m *serviceHandler) getHostInfoCurrentTask(
	ctx context.Context,
	jobID *peloton.JobID,
	instanceID uint32) (hostname string, agentID string, taskID string, err error) {
	result, err := m.taskStore.GetTaskForJob(ctx, jobID, instanceID)

	if err != nil {
		return "", "", "", err
	}

	if len(result) != 1 {
		return "", "", "", yarpcerrors.NotFoundErrorf("task not found")
	}

	var taskInfo *task.TaskInfo
	for _, t := range result {
		taskInfo = t
	}

	hostname, agentID = taskInfo.GetRuntime().GetHost(), taskInfo.GetRuntime().GetAgentID().GetValue()
	taskID = taskInfo.GetRuntime().GetMesosTaskId().GetValue()
	return hostname, agentID, taskID, nil
}

// getSandboxPathInfo - return details such as hostname, agentID, frameworkID and taskID to create sandbox path.
func (m *serviceHandler) getSandboxPathInfo(ctx context.Context,
	instanceCount uint32,
	req *task.BrowseSandboxRequest) (hostname, agentID, taskID, frameworkID string, resp *task.BrowseSandboxResponse) {
	var host string
	var agentid string
	taskid := req.GetTaskId()

	var err error
	if len(taskid) > 0 {
		host, agentid, err = m.getHostInfoWithTaskID(ctx, req.JobId, req.InstanceId, taskid)
	} else {
		host, agentid, taskid, err = m.getHostInfoCurrentTask(ctx, req.JobId, req.InstanceId)
	}

	if err != nil {
		m.metrics.TaskListLogsFail.Inc(1)
		return "", "", "", "", &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				OutOfRange: &task.InstanceIdOutOfRange{
					JobId:         req.JobId,
					InstanceCount: instanceCount,
				},
			},
		}
	}

	if len(host) == 0 || len(agentid) == 0 {
		m.metrics.TaskListLogsFail.Inc(1)
		return "", "", "", "", &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				NotRunning: &task.TaskNotRunning{
					Message: "taskinfo does not have hostname or agentID",
				},
			},
		}
	}

	// get framework ID.
	frameworkid, err := m.getFrameworkID(ctx)
	if err != nil {
		m.metrics.TaskListLogsFail.Inc(1)
		log.WithError(err).WithFields(log.Fields{
			"req": req,
		}).Error("failed to get framework id")
		return "", "", "", "", &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				Failure: &task.BrowseSandboxFailure{
					Message: err.Error(),
				},
			},
		}
	}
	return host, agentid, taskid, frameworkid, nil
}

// BrowseSandbox returns the list of sandbox files path, with agent name, agent id and mesos master name & port.
func (m *serviceHandler) BrowseSandbox(
	ctx context.Context,
	req *task.BrowseSandboxRequest) (*task.BrowseSandboxResponse, error) {
	log.WithField("req", req).Debug("TaskSVC.BrowseSandbox called")
	m.metrics.TaskAPIListLogs.Inc(1)

	jobConfig, err := m.jobStore.GetJobConfig(ctx, req.JobId)
	if err != nil {
		log.WithField("job_id", req.JobId.Value).
			WithError(err).
			Debug("failed to find job with id")
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

	hostname, agentID, taskID, frameworkID, resp := m.getSandboxPathInfo(ctx, jobConfig.InstanceCount, req)
	if resp != nil {
		return resp, nil
	}

	fmt.Println(hostname, agentID, taskID, frameworkID)

	var logPaths []string
	logPaths, err = m.logManager.ListSandboxFilesPaths(m.mesosAgentWorkDir, frameworkID, hostname, agentID, taskID)

	if err != nil {
		m.metrics.TaskListLogsFail.Inc(1)
		log.WithError(err).WithFields(log.Fields{
			"req":          req,
			"hostname":     hostname,
			"framework_id": frameworkID,
			"agent_id":     agentID,
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

	mesosMasterHostPortRespose, err := m.hostMgrClient.GetMesosMasterHostPort(ctx, &hostsvc.MesosMasterHostPortRequest{})
	if err != nil {
		m.metrics.TaskListLogsFail.Inc(1)
		log.WithError(err).WithFields(log.Fields{
			"req":          req,
			"hostname":     hostname,
			"framework_id": frameworkID,
			"agent_id":     agentID,
		}).Error("failed to list slave logs files paths")
		return &task.BrowseSandboxResponse{
			Error: &task.BrowseSandboxResponse_Error{
				Failure: &task.BrowseSandboxFailure{
					Message: fmt.Sprintf(
						"%v",
						err,
					),
				},
			},
		}, nil
	}

	m.metrics.TaskListLogs.Inc(1)
	resp = &task.BrowseSandboxResponse{
		Hostname:            hostname,
		Port:                "5051",
		Paths:               logPaths,
		MesosMasterHostname: mesosMasterHostPortRespose.Hostname,
		MesosMasterPort:     mesosMasterHostPortRespose.Port,
	}
	log.WithField("response", resp).Info("TaskSVC.BrowseSandbox returned")
	return resp, nil
}

// TODO: remove this function once eventstream is enabled in RM
// fillTaskInfoFromResourceManager takes a map of taskinfo and update it
// with result from ResourceManager. All the task in the input should belong
// to the same jobID passed in
func (m *serviceHandler) fillTaskInfoFromResourceManager(
	ctx context.Context,
	jobID *peloton.JobID,
	taskMap []*task.TaskInfo) {
	// only need to consult ResourceManager for PENDING tasks,
	// because only tasks with PENDING states are being processed by ResourceManager
	pendingTasks := make(map[string]*task.TaskInfo)
	for _, taskInfo := range taskMap {
		if taskInfo.GetRuntime().GetState() == task.TaskState_PENDING {
			taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), taskInfo.GetInstanceId())
			pendingTasks[taskID] = taskInfo
		}
	}
	if len(pendingTasks) == 0 {
		return
	}

	// TODO: resmdrsvc.GetActiveTasksRequest.States takes a slice of TaskState instead of string
	rmResp, err := m.resmgrClient.GetActiveTasks(
		ctx,
		&resmgrsvc.GetActiveTasksRequest{
			JobID:  jobID.GetValue(),
			States: getResourceManagerProcessingStates(),
		})
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			Error("failed to get active tasks from ResourceManager")
		return
	}

	// iterate through the result, and fill in the TaskInfo.Runtime.Reason for PENDING tasks
	for _, taskEntries := range rmResp.GetTasksByState() {
		for _, taskEntry := range taskEntries.GetTaskEntry() {
			taskID := taskEntry.GetTaskID()
			if pendingTaskInfo, ok := pendingTasks[taskID]; ok {
				pendingTaskInfo.GetRuntime().Reason = taskEntry.GetReason()
			}
		}
	}
	return
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

// getEventsResponseResult returns []*GetEventsResponse_Events which is a list
// of event lists for each mesos task sorted by time
func getEventsResponseResult(records []*task.TaskEvent) []*task.GetEventsResponse_Events {
	// Get a map of <mesos task id : []*TaskEvent>
	resMap := make(map[string]*task.GetEventsResponse_Events)
	for _, record := range records {
		taskID := record.GetTaskId().GetValue()
		_, ok := resMap[taskID]
		if ok {
			resMap[taskID].Event = append(resMap[taskID].Event, record)
		} else {
			resMap[taskID] = &task.GetEventsResponse_Events{Event: []*task.TaskEvent{record}}
		}
	}
	var eventsListRes []*task.GetEventsResponse_Events
	for _, eventList := range resMap {
		sort.Sort(statechanges.TaskEventByTime(eventList.GetEvent()))
		eventsListRes = append(eventsListRes, eventList)
	}
	sort.Sort(statechanges.TaskEventListByTime(eventsListRes))
	return eventsListRes
}

// TODO: remove this function once eventstream is enabled in RM
func convertTaskMapToSlice(taskMaps map[uint32]*task.TaskInfo) []*task.TaskInfo {
	result := make([]*task.TaskInfo, 0, len(taskMaps))
	for _, taskInfo := range taskMaps {
		result = append(result, taskInfo)
	}
	return result
}

// TODO: remove this function once eventstream is enabled in RM
// getResourceManagerProcessingStates returns a slice of state name which
// indicates a task is still being processed in ResourceManager
func getResourceManagerProcessingStates() []string {
	return []string{
		task.TaskState_PENDING.String(),
		task.TaskState_READY.String(),
		task.TaskState_PLACING.String(),
		task.TaskState_PLACED.String(),
		task.TaskState_LAUNCHING.String()}
}
