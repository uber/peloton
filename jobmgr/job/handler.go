package job

import (
	"context"
	"fmt"
	"time"

	mesos "mesos/v1"

	jm_task "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/storage"
	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	"peloton/api/job"
	"peloton/api/task"
	"peloton/private/resmgr/taskqueue"
	"strings"
)

// InitServiceHandler initalizes the job manager
func InitServiceHandler(
	d yarpc.Dispatcher,
	parent tally.Scope,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	clientName string) {

	handler := serviceHandler{
		jobStore:  jobStore,
		taskStore: taskStore,
		client:    json.New(d.ClientConfig(clientName)),
		rootCtx:   context.Background(),
		metrics:   NewMetrics(parent.SubScope("jobmgr").SubScope("job")),
	}
	json.Register(d, json.Procedure("JobManager.Create", handler.Create))
	json.Register(d, json.Procedure("JobManager.Get", handler.Get))
	json.Register(d, json.Procedure("JobManager.Query", handler.Query))
	json.Register(d, json.Procedure("JobManager.Delete", handler.Delete))
}

// serviceHandler implements peloton.api.job.JobManager
type serviceHandler struct {
	jobStore  storage.JobStore
	taskStore storage.TaskStore
	client    json.Client
	rootCtx   context.Context
	metrics   *Metrics
}

// Create creates a job object for a given job configuration and
// enqueues the tasks for scheduling
func (h *serviceHandler) Create(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *job.CreateRequest) (*job.CreateResponse, yarpc.ResMeta, error) {

	jobID := req.Id
	if strings.Index(jobID.Value, "-") > 0 {
		h.metrics.JobCreateFail.Inc(1)
		err := fmt.Errorf("Invalid jobId %v that contains '-'", jobID.Value)
		log.WithError(err).Info("Invalid jobID value")
		return &job.CreateResponse{
			InvalidConfig: &job.InvalidJobConfig{
				Id:      req.Id,
				Message: err.Error(),
			},
		}, nil, nil
	}
	jobConfig := req.Config

	log.WithField("config", jobConfig).Infof("JobManager.Create called")
	h.metrics.JobAPICreate.Inc(1)

	// Validate job config with default task configs
	err := jm_task.ValidateTaskConfig(jobConfig)
	if err != nil {
		return &job.CreateResponse{
			InvalidConfig: &job.InvalidJobConfig{
				Id:      req.Id,
				Message: err.Error(),
			},
		}, nil, nil
	}

	err = h.jobStore.CreateJob(jobID, jobConfig, "peloton")
	if err != nil {
		h.metrics.JobCreateFail.Inc(1)
		return &job.CreateResponse{
			AlreadyExists: &job.JobAlreadyExists{
				Id:      req.Id,
				Message: err.Error(),
			},
		}, nil, nil
	}
	h.metrics.JobCreate.Inc(1)
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
		taskConfig, err := jm_task.GetTaskConfig(jobConfig, i)
		if err != nil {
			log.Errorf("Failed to get task config (%d) for job %v: %v",
				i, jobID.Value, err)
			h.metrics.JobCreateFail.Inc(1)
			return nil, nil, err
		}
		t := task.TaskInfo{
			Runtime: &task.RuntimeInfo{
				State: task.RuntimeInfo_INITIALIZED,
				// New task is by default treated as batch task and get SUCCEEDED goalstate.
				// TODO(mu): Long running tasks need RUNNING as default goalstate.
				GoalState: task.RuntimeInfo_SUCCEEDED,
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
	err = h.taskStore.CreateTasks(jobID, tasks, "peloton")
	nTasks := int64(len(tasks))
	if err != nil {
		log.Errorf("Failed to create tasks (%d) for job %v: %v",
			nTasks, jobID.Value, err)
		h.metrics.TaskCreateFail.Inc(nTasks)
		return nil, nil, err
	}
	h.metrics.TaskCreate.Inc(nTasks)
	h.putTasks(tasks)

	log.Infof("Job %v all %v tasks created, time spent: %v",
		jobID.Value, instances, time.Since(startAddTaskTime))

	return &job.CreateResponse{
		Result: jobID,
	}, nil, nil
}

// Get returns a job config for a given job ID
func (h *serviceHandler) Get(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *job.GetRequest) (*job.GetResponse, yarpc.ResMeta, error) {

	log.Infof("JobManager.Get called: %v", req)
	h.metrics.JobAPIGet.Inc(1)

	jobConfig, err := h.jobStore.GetJob(req.Id)
	if err != nil {
		h.metrics.JobGetFail.Inc(1)
		log.Errorf("GetJob failed with error %v", err)
		return nil, nil, err
	}
	h.metrics.JobGet.Inc(1)
	return &job.GetResponse{Result: jobConfig}, nil, nil
}

// Query returns a list of jobs matching the given query
func (h *serviceHandler) Query(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *job.QueryRequest) (*job.QueryResponse, yarpc.ResMeta, error) {

	log.Infof("JobManager.Query called: %v", req)
	h.metrics.JobAPIQuery.Inc(1)

	jobConfigs, err := h.jobStore.Query(req.Labels)
	if err != nil {
		h.metrics.JobQueryFail.Inc(1)
		log.Errorf("Query job failed with error %v", err)
		return nil, nil, err
	}
	h.metrics.JobQuery.Inc(1)
	return &job.QueryResponse{Result: jobConfigs}, nil, nil
}

// Delete kills all running tasks in a job
func (h *serviceHandler) Delete(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *job.DeleteRequest) (*job.DeleteResponse, yarpc.ResMeta, error) {

	log.Infof("JobManager.Delete called: %v", req)
	h.metrics.JobAPIDelete.Inc(1)

	err := h.jobStore.DeleteJob(req.Id)
	if err != nil {
		h.metrics.JobDeleteFail.Inc(1)
		log.Errorf("Delete job failed with error %v", err)
		return nil, nil, err
	}
	h.metrics.JobDelete.Inc(1)
	return &job.DeleteResponse{}, nil, nil
}

// putTasks enqueues all tasks to a task queue in resmgr.
func (h *serviceHandler) putTasks(tasks []*task.TaskInfo) error {

	// TODO: switch to use the ResourceManagerService.EnqueueTasks API
	ctx, cancelFunc := context.WithTimeout(h.rootCtx, 10*time.Second)
	defer cancelFunc()
	var response taskqueue.EnqueueResponse
	var request = &taskqueue.EnqueueRequest{
		Tasks: tasks,
	}
	_, err := h.client.Call(
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
