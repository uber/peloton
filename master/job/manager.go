package job

import (
	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/encoding/json"
	"golang.org/x/net/context"

	"code.uber.internal/go-common.git/x/log"
	"peloton/job"
	"code.uber.internal/infra/peloton/storage"
	"peloton/task"
)

func InitManager(d yarpc.Dispatcher, store storage.JobStore, taskStore storage.TaskStore) {
	handler := jobManager{JobStore:store, TaskStore:taskStore}
	json.Register(d, json.Procedure("JobManager.Create", handler.Create))
	json.Register(d, json.Procedure("JobManager.Get", handler.Get))
	json.Register(d, json.Procedure("JobManager.Query", handler.Query))
	json.Register(d, json.Procedure("JobManager.Delete", handler.Delete))
}

type jobManager struct {
	JobStore storage.JobStore
	TaskStore storage.TaskStore
}

func (m *jobManager) Create(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *job.CreateRequest) (*job.CreateResponse, yarpc.ResMeta, error) {

	log.Infof("JobManager.Create called: %s", body)
	jobId := body.Id
	jobConfig := body.Config
	err := m.JobStore.CreateJob(jobId, jobConfig, "peloton")
	if err != nil {
		return &job.CreateResponse{
			Response : &job.CreateResponse_AlreadyExists{
				AlreadyExists: &job.JobAlreadyExists{
					Id : body.Id,
					Message: err.Error(),
				},
			},
		}, nil, nil
	}
	// Create tasks for the job
	for i := 0; i < int(body.Config.InstanceCount); i++ {
		taskInfo := task.TaskInfo{
			Runtime: &task.RuntimeInfo{
					State:  task.RuntimeInfo_INITIALIZED,
				},
				JobConfig:  jobConfig,
				InstanceId: uint32(i),
				JobId:      jobId,
		}
		log.Infof("Creating %v =th task for job %v", i, jobId)
		err := m.TaskStore.CreateTask(jobId, i, &taskInfo, "peloton")
		if err != nil {
			log.Errorf("Creating %v =th task for job %v failed with err=%v", i, jobId, err)
			// TODO : decide how to handle the case that some tasks failed to be added (rare)
			// 1. Rely on job level healthcheck to alert on # of instances mismatch, and re-try creating the task later
			// 2. revert te job creation altogether
		}
	}
	return &job.CreateResponse{
		Response : &job.CreateResponse_Result {
			Result : jobId,
		},
	}, nil, nil
}

func (m *jobManager) Get(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *job.GetRequest) (*job.GetResponse, yarpc.ResMeta, error) {
	log.Infof("JobManager.Get called: %s", body)

	jobConfig, err := m.JobStore.GetJob(body.Id)
	if err != nil {
		log.Errorf("GetJob failed with error %v", err)
	}
	return &job.GetResponse{Result:jobConfig}, nil, nil
}

func (m *jobManager) Query(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *job.QueryRequest) (*job.QueryResponse, yarpc.ResMeta, error) {
	log.Infof("JobManager.Query called: %s", body)

	jobConfigs, err := m.JobStore.Query(body.Labels)
	if err != nil {
		log.Errorf("Query job failed with error %v", err)
	}
	return &job.QueryResponse{Result:jobConfigs}, nil, nil
}

func (m *jobManager) Delete(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *job.DeleteRequest) (*job.DeleteResponse, yarpc.ResMeta, error) {

	log.Infof("JobManager.Delete called: %s", body)
	err := m.JobStore.DeleteJob(body.Id)
	if err != nil {
		log.Errorf("Delete job failed with error %v", err)
	}
	return &job.DeleteResponse{}, nil, nil
}

