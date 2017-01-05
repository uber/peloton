package job

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/master/metrics"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
	mesos_v1 "mesos/v1"
	"peloton/job"
	"peloton/master/taskqueue"
	"peloton/task"
	"sync"
)

// InitManager initalizes the job manager
func InitManager(d yarpc.Dispatcher, store storage.JobStore, taskStore storage.TaskStore, metrics *metrics.Metrics) {
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
	metrics   *metrics.Metrics
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
	// NOTE: temp work to make task creation concurrent for mysql store.
	// mysql store will be deprecated soon and we are moving on the C* store
	// As of now, only create one job with many tasks at a time
	maxDBConcurrency := 100
	instances := int(body.Config.InstanceCount)
	batches := instances/maxDBConcurrency + 1
	startAddTaskTime := time.Now()
	go func() {
		for j := 0; j < batches; j++ {
			start := j * maxDBConcurrency
			end := (j + 1) * maxDBConcurrency
			if start >= instances {
				break
			}
			if end > instances {
				end = instances
			}
			wg := new(sync.WaitGroup)
			wg.Add(end - start)
			for i := start; i < end; i++ {
				log.Debugf("Creating task %v for job %v", i, jobID.Value)
				instanceID := i
				go func() {
					defer wg.Done()
					taskID := fmt.Sprintf("%s-%d", jobID.Value, instanceID)
					taskInfo := task.TaskInfo{
						Runtime: &task.RuntimeInfo{
							State: task.RuntimeInfo_INITIALIZED,
							TaskId: &mesos_v1.TaskID{
								Value: &taskID,
							},
						},
						JobConfig:  jobConfig,
						InstanceId: uint32(instanceID),
						JobId:      jobID,
					}
					// FIXME: we should be tracking task creates with metrics here, using the same metric scope as in TaskManager
					// https://code.uberinternal.com/T674463

					maxTaskCreateAttempts := 100
					for j := 0; j < maxTaskCreateAttempts; j++ {
						err := m.TaskStore.CreateTask(jobID, instanceID, &taskInfo, "peloton")
						if err != nil {
							m.metrics.TaskCreateFail.Inc(1)
							log.Errorf("Creating task %v for job %v failed with err=%v", instanceID, jobID.Value, err)
							time.Sleep(1 * time.Second)
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
						return
					}
					log.Errorf("Failed to create task %d for job %v after %d attempts", instanceID, jobID.Value, maxTaskCreateAttempts)
					// TODO: fire alerts, or make job enter certain state (task missing)
				}()
			}
			wg.Wait()
		}
		log.Infof("Job %v all %v tasks created, time spent: %v", jobID.Value, instances, time.Now().Sub(startAddTaskTime))
	}()
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
