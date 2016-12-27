package task

import (
	"context"
	"fmt"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/storage"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	"peloton/job"
	"peloton/task"
)

// InitManager initializes the TaskManager
func InitManager(d yarpc.Dispatcher, jobStore storage.JobStore, taskStore storage.TaskStore, metricScope tally.Scope) {
	successScope := metricScope.Tagged(map[string]string{"type": "success"})
	failScope := metricScope.Tagged(map[string]string{"type": "fail"})
	apiScope := metricScope.SubScope("api")

	handler := taskManager{
		taskStore: taskStore,
		jobStore:  jobStore,
		metrics: taskMetrics{
			apiGet:      apiScope.Counter("get"),
			get:         successScope.Counter("get"),
			getFail:     failScope.Counter("get"),
			apiList:     apiScope.Counter("list"),
			list:        successScope.Counter("list"),
			listFail:    failScope.Counter("list"),
			apiStart:    apiScope.Counter("start"),
			start:       successScope.Counter("start"),
			startFail:   failScope.Counter("start"),
			apiStop:     apiScope.Counter("stop"),
			stop:        successScope.Counter("stop"),
			stopFail:    failScope.Counter("stop"),
			apiRestart:  apiScope.Counter("restart"),
			restart:     successScope.Counter("restart"),
			restartFail: failScope.Counter("restart"),
		},
	}
	json.Register(d, json.Procedure("TaskManager.Get", handler.Get))
	json.Register(d, json.Procedure("TaskManager.List", handler.List))
	json.Register(d, json.Procedure("TaskManager.Start", handler.Start))
	json.Register(d, json.Procedure("TaskManager.Stop", handler.Stop))
	json.Register(d, json.Procedure("TaskManager.Restart", handler.Restart))
}

type taskManager struct {
	taskStore storage.TaskStore
	jobStore  storage.JobStore
	metrics   taskMetrics
}

type taskMetrics struct {
	apiGet      tally.Counter
	get         tally.Counter
	getFail     tally.Counter
	apiList     tally.Counter
	list        tally.Counter
	listFail    tally.Counter
	apiStart    tally.Counter
	start       tally.Counter
	startFail   tally.Counter
	apiStop     tally.Counter
	stop        tally.Counter
	stopFail    tally.Counter
	apiRestart  tally.Counter
	restart     tally.Counter
	restartFail tally.Counter
}

func (m *taskManager) Get(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.GetRequest) (*task.GetResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Get called: %v", body)
	m.metrics.apiGet.Inc(1)
	jobConfig, err := m.jobStore.GetJob(body.JobId)
	if err != nil || jobConfig == nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		return &task.GetResponse{
			NotFound: &job.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("job %v not found, %v", body.JobId, err),
			},
		}, nil, nil
	}

	result, err := m.taskStore.GetTaskForJob(body.JobId, body.InstanceId)
	for _, taskInfo := range result {
		log.Infof("found task %v", taskInfo)
		m.metrics.get.Inc(1)
		return &task.GetResponse{
			Result: taskInfo,
		}, nil, nil
	}

	m.metrics.getFail.Inc(1)
	return &task.GetResponse{
		OutOfRange: &task.InstanceIdOutOfRange{
			JobId:         body.JobId,
			InstanceCount: jobConfig.InstanceCount,
		},
	}, nil, nil
}

func (m *taskManager) List(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.ListRequest) (*task.ListResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.List called: %v", body)
	m.metrics.apiList.Inc(1)
	_, err := m.jobStore.GetJob(body.JobId)
	if err != nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		m.metrics.listFail.Inc(1)
		return &task.ListResponse{
			NotFound: &job.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("Failed to find job with id %v, err=%v", body.JobId, err),
			},
		}, nil, nil
	}
	var result map[uint32]*task.TaskInfo
	if body.Range == nil {
		result, err = m.taskStore.GetTasksForJob(body.JobId)
	} else {
		result, err = m.taskStore.GetTasksForJobByRange(body.JobId, body.Range)
	}
	if err != nil || len(result) == 0 {
		m.metrics.listFail.Inc(1)
		return &task.ListResponse{
			NotFound: &job.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("err= %v", err),
			},
		}, nil, nil
	}

	m.metrics.list.Inc(1)
	return &task.ListResponse{
		Result: &task.ListResponse_Result{
			Value: result,
		},
	}, nil, nil
}

func (m *taskManager) Start(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.StartRequest) (*task.StartResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Start called: %v", body)
	m.metrics.apiStart.Inc(1)
	m.metrics.start.Inc(1)
	return &task.StartResponse{}, nil, nil
}

func (m *taskManager) Stop(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.StopRequest) (*task.StopResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Stop called: %v", body)
	m.metrics.apiStop.Inc(1)
	m.metrics.stop.Inc(1)
	return &task.StopResponse{}, nil, nil
}

func (m *taskManager) Restart(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.RestartRequest) (*task.RestartResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Restart called: %v", body)
	m.metrics.apiRestart.Inc(1)
	m.metrics.restart.Inc(1)
	return &task.RestartResponse{}, nil, nil
}
