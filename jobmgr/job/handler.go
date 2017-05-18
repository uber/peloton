package job

import (
	"context"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	er "github.com/pkg/errors"
	"github.com/uber-go/tally"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/errors"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr/taskqueue"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	jm_task "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
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
	// It is possible that jobId is nil since protobuf doesn't enforce it
	if jobID == nil {
		jobID = &peloton.JobID{Value: ""}
	}

	if len(jobID.Value) == 0 {
		jobID.Value = uuid.NewUUID().String()
		log.WithField("jobID", jobID).Info("Genarating UUID ID for empty job ID")
	} else {
		if uuid.Parse(jobID.Value) == nil {
			log.WithField("job_id", jobID.Value).Warn("JobID is not valid UUID")
			return &job.CreateResponse{
				Error: &job.CreateResponse_Error{
					InvalidJobId: &job.InvalidJobId{
						Id:      req.Id,
						Message: "JobID must be valid UUID",
					},
				},
			}, nil, nil
		}
	}
	jobConfig := req.Config

	err := h.validateResourcePool(jobConfig.RespoolID)
	if err != nil {
		return &job.CreateResponse{
			Error: &job.CreateResponse_Error{
				InvalidConfig: &job.InvalidJobConfig{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil, nil
	}

	log.WithField("config", jobConfig).Infof("JobManager.Create called")
	h.metrics.JobAPICreate.Inc(1)

	// Validate job config with default task configs
	err = jm_task.ValidateTaskConfig(jobConfig)
	if err != nil {
		return &job.CreateResponse{
			Error: &job.CreateResponse_Error{
				InvalidConfig: &job.InvalidJobConfig{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil, nil
	}

	err = h.jobStore.CreateJob(jobID, jobConfig, "peloton")
	if err != nil {
		h.metrics.JobCreateFail.Inc(1)
		return &job.CreateResponse{
			Error: &job.CreateResponse_Error{
				AlreadyExists: &job.JobAlreadyExists{
					Id:      req.Id,
					Message: err.Error(),
				},
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
		taskConfig, err := jm_task.GetTaskConfig(jobID, jobConfig, i)
		if err != nil {
			log.Errorf("Failed to get task config (%d) for job %v: %v",
				i, jobID.Value, err)
			h.metrics.JobCreateFail.Inc(1)
			return &job.CreateResponse{
				Error: &job.CreateResponse_Error{
					InvalidConfig: &job.InvalidJobConfig{
						Id:      jobID,
						Message: err.Error(),
					},
				},
			}, nil, nil
		}
		t := task.TaskInfo{
			Runtime: &task.RuntimeInfo{
				State: task.TaskState_INITIALIZED,
				// New task is by default treated as batch task and get SUCCEEDED goalstate.
				// TODO(mu): Long running tasks need RUNNING as default goalstate.
				GoalState: task.TaskState_SUCCEEDED,
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
		// FIXME: Add a new Error type for this
		return nil, nil, err
	}
	h.metrics.TaskCreate.Inc(nTasks)

	err = h.enqueueTasks(tasks, jobConfig)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to enqueue tasks to RM")
		h.metrics.JobCreateFail.Inc(1)
		return nil, nil, err
	}

	jobRuntime, err := h.jobStore.GetJobRuntime(jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to GetJobRuntime")
		h.metrics.JobCreateFail.Inc(1)
		return nil, nil, err
	}
	jobRuntime.State = job.JobState_PENDING
	err = h.jobStore.UpdateJobRuntime(jobID, jobRuntime)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to UpdateJobRuntime")
		h.metrics.JobCreateFail.Inc(1)
		return nil, nil, err
	}

	log.Infof("Job %v all %v tasks created, time spent: %v",
		jobID.Value, instances, time.Since(startAddTaskTime))

	return &job.CreateResponse{
		JobId: jobID,
	}, nil, nil
}

// Get returns a job config for a given job ID
func (h *serviceHandler) Get(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *job.GetRequest) (*job.GetResponse, yarpc.ResMeta, error) {

	log.Infof("JobManager.Get called: %v", req)
	h.metrics.JobAPIGet.Inc(1)

	jobConfig, err := h.jobStore.GetJobConfig(req.Id)
	if err != nil {
		h.metrics.JobGetFail.Inc(1)
		log.WithError(err).
			WithField("job_id", req.Id).
			Errorf("GetJobConfig failed")
		return &job.GetResponse{
			Error: &job.GetResponse_Error{
				NotFound: &errors.JobNotFound{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil, nil
	}
	jobRuntime, err := h.jobStore.GetJobRuntime(req.Id)
	if err != nil {
		h.metrics.JobGetFail.Inc(1)
		log.WithError(err).
			WithField("job_id", req.Id).
			Error("Get jobRuntime failed")
		return &job.GetResponse{
			Error: &job.GetResponse_Error{
				GetRuntimeFail: &errors.JobGetRuntimeFail{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil, nil
	}
	h.metrics.JobGet.Inc(1)
	return &job.GetResponse{
		Config:  jobConfig,
		Runtime: jobRuntime,
	}, nil, nil
}

// Query returns a list of jobs matching the given query
func (h *serviceHandler) Query(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *job.QueryRequest) (*job.QueryResponse, yarpc.ResMeta, error) {

	log.WithField("request", req).Info("JobManager.Query called")
	h.metrics.JobAPIQuery.Inc(1)
	var jobConfigs map[string]*job.JobConfig
	var err error
	if (req.GetLabels().GetLabels() != nil && len(req.GetLabels().GetLabels()) > 0) ||
		(req.GetKeywords() != nil && len(req.GetKeywords()) > 0) {
		jobConfigs, err = h.jobStore.Query(req.Labels, req.GetKeywords())
		if err != nil {
			h.metrics.JobQueryFail.Inc(1)
			log.WithError(err).Error("Query job failed with error")
			return &job.QueryResponse{
				Error: &job.QueryResponse_Error{
					Err: &errors.UnknownError{
						Message: err.Error(),
					},
				},
			}, nil, nil
		}
		// if both labels and respool ID is specified, query then filter by respool id
		if req.GetRespoolID() != nil {
			for jobID, jobConfig := range jobConfigs {
				if jobConfig.RespoolID.GetValue() != req.GetRespoolID().Value {
					delete(jobConfigs, jobID)
				}
			}
		}
	} else {
		// Query by respool id directly
		jobConfigs, err = h.jobStore.GetJobsByRespoolID(req.GetRespoolID())
		if err != nil {
			h.metrics.JobQueryFail.Inc(1)
			log.WithError(err).Error("Query job failed with error")
			return &job.QueryResponse{
				Error: &job.QueryResponse_Error{
					Err: &errors.UnknownError{
						Message: err.Error(),
					},
				},
			}, nil, nil
		}
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
		return &job.DeleteResponse{
			Error: &job.DeleteResponse_Error{
				NotFound: &errors.JobNotFound{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil, nil
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

// submitTasksToResmgr enqueues all tasks to respool in resmgr.
func (h *serviceHandler) enqueueTasks(
	tasks []*task.TaskInfo,
	config *job.JobConfig,
) error {
	return EnqueueTasks(tasks, config, h.client)
}

// EnqueueTasks enqueues all tasks to respool in resmgr.
func EnqueueTasks(tasks []*task.TaskInfo, config *job.JobConfig, client json.Client) error {
	rootCtx := context.Background()
	ctx, cancelFunc := context.WithTimeout(rootCtx, 10*time.Second)
	defer cancelFunc()
	resmgrTasks := convertToResMgrTask(tasks, config)
	var response resmgrsvc.EnqueueTasksResponse
	var request = &resmgrsvc.EnqueueTasksRequest{
		Tasks:   resmgrTasks,
		ResPool: config.RespoolID,
	}
	_, err := client.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("ResourceManagerService.EnqueueTasks"),
		request,
		&response,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"tasks": tasks,
		}).Error("Resource Manager Enqueue Failed")
		return err
	}
	if response.Error != nil {
		log.WithFields(log.Fields{
			"error": response.Error,
			"tasks": tasks,
		}).Error("Resource Manager Enqueue Failed")
		return er.New(response.Error.String())
	}
	log.WithField("Count", len(resmgrTasks)).Debug("Enqueued tasks to " +
		"Resource Manager")
	return nil
}

// validateResourcePool validates the resource pool before submitting job
func (h *serviceHandler) validateResourcePool(
	respoolID *respool.ResourcePoolID,
) error {
	ctx, cancelFunc := context.WithTimeout(h.rootCtx, 10*time.Second)
	defer cancelFunc()
	if respoolID == nil {
		return er.New("Resource Pool Id is null")
	}

	var response respool.GetResponse
	var request = &respool.GetRequest{
		Id: respoolID,
	}
	_, err := h.client.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("ResourceManager.GetResourcePool"),
		request,
		&response,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error":     err,
			"respoolID": respoolID.Value,
		}).Error("Failed to get Resource Pool")
		return err
	}
	if response.Error != nil {
		log.WithFields(log.Fields{
			"error":     err,
			"respoolID": respoolID.Value,
		}).Info("Resource Pool Not Found")
		return er.New(response.Error.String())
	}

	if response.GetPoolinfo() != nil && response.GetPoolinfo().Id != nil {
		if response.GetPoolinfo().Id.Value != respoolID.Value {
			return er.New("Resource Pool Not Found")
		}
	} else {
		return er.New("Resource Pool Not Found")
	}

	return nil
}

// convertToResMgrTask converts taskinfo to resmgr task
func convertToResMgrTask(
	tasks []*task.TaskInfo,
	config *job.JobConfig) []*resmgr.Task {

	var resmgrtasks []*resmgr.Task
	for _, t := range tasks {
		resmgrtasks = append(
			resmgrtasks,
			util.ConvertTaskToResMgrTask(t, config),
		)
	}
	return resmgrtasks
}
