package task

import (
	"fmt"
	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/encoding/json"
	"golang.org/x/net/context"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/storage"
	"peloton/job"
	"peloton/task"
)

func InitManager(d yarpc.Dispatcher, jobStore storage.JobStore, taskStore storage.TaskStore) {
	handler := taskManager{TaskStore: taskStore, JobStore: jobStore}
	json.Register(d, json.Procedure("TaskManager.Get", handler.Get))
	json.Register(d, json.Procedure("TaskManager.List", handler.List))
	json.Register(d, json.Procedure("TaskManager.Start", handler.Start))
	json.Register(d, json.Procedure("TaskManager.Stop", handler.Stop))
	json.Register(d, json.Procedure("TaskManager.Restart", handler.Restart))
}

type taskManager struct {
	TaskStore storage.TaskStore
	JobStore  storage.JobStore
}

func (m *taskManager) Get(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.GetRequest) (*task.GetResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Get called: %v", body)
	jobConfig, err := m.JobStore.GetJob(body.JobId)
	if err != nil || jobConfig == nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		return &task.GetResponse{
			NotFound: &job.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("job %v not found, %v", body.JobId, err),
			},
		}, nil, nil
	}

	result, err := m.TaskStore.GetTaskForJob(body.JobId, body.InstanceId)
	for _, taskInfo := range result {
		log.Infof("found task %v", taskInfo)
		return &task.GetResponse{
			Result: taskInfo,
		}, nil, nil
	}

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
	_, err := m.JobStore.GetJob(body.JobId)
	if err != nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		return &task.ListResponse{
			NotFound: &job.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("Failed to find job with id %v, err=%v", body.JobId, err),
			},
		}, nil, nil
	}
	result, err := m.TaskStore.GetTasksForJobByRange(body.JobId, body.Range)
	if err != nil || len(result) == 0 {
		return &task.ListResponse{
			NotFound: &job.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("err= %v", err),
			},
		}, nil, nil
	}
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
	return &task.StartResponse{}, nil, nil
}

func (m *taskManager) Stop(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.StopRequest) (*task.StopResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Stop called: %v", body)
	return &task.StopResponse{}, nil, nil
}

func (m *taskManager) Restart(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.RestartRequest) (*task.RestartResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Restart called: %v", body)
	return &task.RestartResponse{}, nil, nil
}
