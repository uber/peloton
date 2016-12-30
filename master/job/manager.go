package job

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/master"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
	mesos_v1 "mesos/v1"
	"peloton/job"
	"peloton/master/taskqueue"
	"peloton/task"
)

// InitManager initalizes the job manager
func InitManager(d yarpc.Dispatcher, store storage.JobStore, taskStore storage.TaskStore, metrics *master.Metrics) {
	handler := jobManager{
		JobStore:  store,
		TaskStore: taskStore,
		client:    json.New(d.ClientConfig("peloton-master")),
		rootCtx:   context.Background(),
		metrics:   metrics,
	}
	json.Register(d, json.Procedure("JobManager.Create", handler.Create))
	json.Register(d, json.Procedure("JobManager.Get", handler.Get))
	json.Register(d, json.Procedure("JobManager.Query", handler.Query))
	json.Register(d, json.Procedure("JobManager.Delete", handler.Delete))
}

type jobManager struct {
	JobStore  storage.JobStore
	TaskStore storage.TaskStore
	TaskQueue util.TaskQueue
	client    json.Client
	rootCtx   context.Context
	metrics   *master.Metrics
}

func (m *jobManager) Create(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *job.CreateRequest) (*job.CreateResponse, yarpc.ResMeta, error) {

	jobID := body.Id
	jobConfig := body.Config

	log.WithField("config", jobConfig).Infof("JobManager.Create called: %v", body)
	m.metrics.JobAPICreate.Inc(1)

	err := m.JobStore.CreateJob(jobID, jobConfig, "peloton")
	if err != nil {
		m.metrics.JobCreateFail.Inc(1)
		return &job.CreateResponse{
			AlreadyExists: &job.JobAlreadyExists{
				Id:      body.Id,
				Message: err.Error(),
			},
		}, nil, nil
	}
	m.metrics.JobCreate.Inc(1)

	// Create tasks for the job
	for i := 0; i < int(body.Config.InstanceCount); i++ {
		taskID := fmt.Sprintf("%s-%d", jobID.Value, i)
		taskInfo := task.TaskInfo{
			Runtime: &task.RuntimeInfo{
				State: task.RuntimeInfo_INITIALIZED,
				TaskId: &mesos_v1.TaskID{
					Value: &taskID,
				},
			},
			JobConfig:  jobConfig,
			InstanceId: uint32(i),
			JobId:      jobID,
		}
		// FIXME: we should be tracking task creates with metrics here, using the same metric scope as in TaskManager
		// https://code.uberinternal.com/T674463
		log.Debugf("Creating %v =th task for job %v", i, jobID)
		err := m.TaskStore.CreateTask(jobID, i, &taskInfo, "peloton")
		if err != nil {
			m.metrics.TaskCreateFail.Inc(1)
			log.Errorf("Creating %v =th task for job %v failed with err=%v", i, jobID, err)
			continue
			// TODO : decide how to handle the case that some tasks
			// failed to be added (rare)

			// 1. Rely on job level healthcheck to alert on # of
			// instances mismatch, and re-try creating the task later

			// 2. revert te job creation altogether
		}
		// Put the task into the taskQueue. Scheduler will pick the
		// task up and schedule them
		// TODO: batch the tasks for each Enqueue request
		m.metrics.TaskCreate.Inc(1)
		m.putTasks([]*task.TaskInfo{&taskInfo})
	}
	return &job.CreateResponse{
		Result: jobID,
	}, nil, nil
}

func (m *jobManager) Get(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *job.GetRequest) (*job.GetResponse, yarpc.ResMeta, error) {
	log.Infof("JobManager.Get called: %v", body)
	m.metrics.JobAPIGet.Inc(1)

	jobConfig, err := m.JobStore.GetJob(body.Id)
	if err != nil {
		m.metrics.JobGetFail.Inc(1)
		log.Errorf("GetJob failed with error %v", err)
		return nil, nil, err
	}
	m.metrics.JobGet.Inc(1)
	return &job.GetResponse{Result: jobConfig}, nil, nil
}

func (m *jobManager) Query(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *job.QueryRequest) (*job.QueryResponse, yarpc.ResMeta, error) {
	log.Infof("JobManager.Query called: %v", body)
	m.metrics.JobAPIQuery.Inc(1)

	jobConfigs, err := m.JobStore.Query(body.Labels)
	if err != nil {
		m.metrics.JobQueryFail.Inc(1)
		log.Errorf("Query job failed with error %v", err)
		return nil, nil, err
	}
	m.metrics.JobQuery.Inc(1)
	return &job.QueryResponse{Result: jobConfigs}, nil, nil
}

func (m *jobManager) Delete(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *job.DeleteRequest) (*job.DeleteResponse, yarpc.ResMeta, error) {
	log.Infof("JobManager.Delete called: %v", body)
	m.metrics.JobAPIDelete.Inc(1)

	err := m.JobStore.DeleteJob(body.Id)
	if err != nil {
		m.metrics.JobDeleteFail.Inc(1)
		log.Errorf("Delete job failed with error %v", err)
		return nil, nil, err
	}
	m.metrics.JobDelete.Inc(1)
	return &job.DeleteResponse{}, nil, nil
}

func (m *jobManager) putTasks(tasks []*task.TaskInfo) error {
	ctx, cancelFunc := context.WithTimeout(m.rootCtx, 10*time.Second)
	defer cancelFunc()
	var response taskqueue.EnqueueResponse
	var request = &taskqueue.EnqueueRequest{
		Tasks: tasks,
	}
	_, err := m.client.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("TaskQueue.Enqueue"),
		request,
		&response,
	)
	if err != nil {
		log.Errorf("Deque failed with err=%v", err)
		return err
	}
	log.Debugf("Enqueued %d tasks to leader", len(tasks))
	return nil
}
