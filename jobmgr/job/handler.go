package job

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/jobmgr"
	"code.uber.internal/infra/peloton/master/metrics"
	pmt "code.uber.internal/infra/peloton/master/task"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
	mesos "mesos/v1"

	"peloton/api/job"
	"peloton/api/task"
	"peloton/private/resmgr/taskqueue"
	"strings"
)

// InitServiceHandler initalizes the job manager
func InitServiceHandler(d yarpc.Dispatcher,
	config *jobmgr.Config,
	js storage.JobStore,
	ts storage.TaskStore,
	metrics *metrics.Metrics,
	clientName string) {

	handler := serviceHandler{
		JobStore:  js,
		TaskStore: ts,
		client:    json.New(d.ClientConfig(clientName)),
		rootCtx:   context.Background(),
		metrics:   metrics,
		config:    config,
	}
	json.Register(d, json.Procedure("JobManager.Create", handler.Create))
	json.Register(d, json.Procedure("JobManager.Get", handler.Get))
	json.Register(d, json.Procedure("JobManager.Query", handler.Query))
	json.Register(d, json.Procedure("JobManager.Delete", handler.Delete))
}

// serviceHandler implements peloton.api.job.JobManager
type serviceHandler struct {
	JobStore  storage.JobStore
	TaskStore storage.TaskStore
	TaskQueue util.TaskQueue
	client    json.Client
	rootCtx   context.Context
	metrics   *metrics.Metrics
	config    *jobmgr.Config
}

// Create creates a job object for a given job configuration and
// enqueues the tasks for scheduling
func (m *serviceHandler) Create(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *job.CreateRequest) (*job.CreateResponse, yarpc.ResMeta, error) {

	jobID := req.Id
	if strings.Index(jobID.Value, "-") > 0 {
		// TODO: define another CreateResponse for naming error
		m.metrics.JobCreateFail.Inc(1)
		return nil, nil, fmt.Errorf("Invalid jobId %v that contains '-'", jobID.Value)
	}
	jobConfig := req.Config

	log.WithField("config", jobConfig).Infof("JobManager.Create called")
	m.metrics.JobAPICreate.Inc(1)

	// Validate job config with default task configs
	err := pmt.ValidateTaskConfig(jobConfig)
	if err != nil {
		return &job.CreateResponse{
			InvalidConfig: &job.InvalidJobConfig{
				Id:      req.Id,
				Message: err.Error(),
			},
		}, nil, nil
	}

	err = m.JobStore.CreateJob(jobID, jobConfig, "peloton")
	if err != nil {
		m.metrics.JobCreateFail.Inc(1)
		return &job.CreateResponse{
			AlreadyExists: &job.JobAlreadyExists{
				Id:      req.Id,
				Message: err.Error(),
			},
		}, nil, nil
	}
	m.metrics.JobCreate.Inc(1)
	// NOTE: temp work to make task creation concurrent for mysql store.
	// mysql store will be deprecated soon and we are moving on the C* store
	// As of now, only create one job with many tasks at a time
	instances := req.Config.InstanceCount
	startAddTaskTime := time.Now()

	tasks := make([]*task.TaskInfo, instances)
	for i := uint32(0); i < instances; i++ {
		// Populate taskInfos
		instance := i
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.Value, instance,
			uuid.NewUUID().String())
		taskConfig, err := pmt.GetTaskConfig(jobConfig, i)
		if err != nil {
			log.Errorf("Failed to get task config (%d) for job %v: %v",
				i, jobID.Value, err)
			m.metrics.JobCreateFail.Inc(1)
			return nil, nil, err
		}
		t := task.TaskInfo{
			Runtime: &task.RuntimeInfo{
				State: task.RuntimeInfo_INITIALIZED,
				TaskId: &mesos.TaskID{
					Value: &mesosTaskID,
				},
			},
			Config:     taskConfig,
			InstanceId: uint32(instance),
			JobId:      jobID,
		}
		tasks[i] = &t
	}
	// TODO: use the username of current session for createBy param
	err = m.TaskStore.CreateTasks(jobID, tasks, "peloton")
	nTasks := int64(len(tasks))
	if err != nil {
		log.Errorf("Failed to create tasks (%d) for job %v: %v",
			nTasks, jobID.Value, err)
		m.metrics.TaskCreateFail.Inc(nTasks)
		return nil, nil, err
	}
	m.metrics.TaskCreate.Inc(nTasks)
	m.putTasks(tasks)

	log.Infof("Job %v all %v tasks created, time spent: %v",
		jobID.Value, instances, time.Since(startAddTaskTime))

	return &job.CreateResponse{
		Result: jobID,
	}, nil, nil
}

func (m *serviceHandler) Get(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *job.GetRequest) (*job.GetResponse, yarpc.ResMeta, error) {
	log.Infof("JobManager.Get called: %v", req)
	m.metrics.JobAPIGet.Inc(1)

	jobConfig, err := m.JobStore.GetJob(req.Id)
	if err != nil {
		m.metrics.JobGetFail.Inc(1)
		log.Errorf("GetJob failed with error %v", err)
		return nil, nil, err
	}
	m.metrics.JobGet.Inc(1)
	return &job.GetResponse{Result: jobConfig}, nil, nil
}

func (m *serviceHandler) Query(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *job.QueryRequest) (*job.QueryResponse, yarpc.ResMeta, error) {
	log.Infof("JobManager.Query called: %v", req)
	m.metrics.JobAPIQuery.Inc(1)

	jobConfigs, err := m.JobStore.Query(req.Labels)
	if err != nil {
		m.metrics.JobQueryFail.Inc(1)
		log.Errorf("Query job failed with error %v", err)
		return nil, nil, err
	}
	m.metrics.JobQuery.Inc(1)
	return &job.QueryResponse{Result: jobConfigs}, nil, nil
}

func (m *serviceHandler) Delete(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *job.DeleteRequest) (*job.DeleteResponse, yarpc.ResMeta, error) {

	log.Infof("JobManager.Delete called: %v", req)
	m.metrics.JobAPIDelete.Inc(1)

	err := m.JobStore.DeleteJob(req.Id)
	if err != nil {
		m.metrics.JobDeleteFail.Inc(1)
		log.Errorf("Delete job failed with error %v", err)
		return nil, nil, err
	}
	m.metrics.JobDelete.Inc(1)
	return &job.DeleteResponse{}, nil, nil
}

func (m *serviceHandler) putTasks(tasks []*task.TaskInfo) error {
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
		log.Errorf("Enqueue failed with err=%v", err)
		return err
	}
	log.Debugf("Enqueued %d tasks to leader", len(tasks))
	return nil
}
