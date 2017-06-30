package jobsvc

import (
	"context"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	er "github.com/pkg/errors"
	"github.com/uber-go/tally"

	"go.uber.org/yarpc"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/errors"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/query"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	jobmgr_job "code.uber.internal/infra/peloton/jobmgr/job"
	"code.uber.internal/infra/peloton/jobmgr/job/updater"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	task_config "code.uber.internal/infra/peloton/jobmgr/task/config"
	"code.uber.internal/infra/peloton/storage"
)

// InitServiceHandler initalizes the job manager
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	runtimeUpdater *jobmgr_job.RuntimeUpdater,
	clientName string) {

	handler := &serviceHandler{
		jobStore:       jobStore,
		taskStore:      taskStore,
		respoolClient:  respool.NewResourceManagerYarpcClient(d.ClientConfig(clientName)),
		resmgrClient:   resmgrsvc.NewResourceManagerServiceYarpcClient(d.ClientConfig(clientName)),
		rootCtx:        context.Background(),
		runtimeUpdater: runtimeUpdater,
		metrics:        NewMetrics(parent.SubScope("jobmgr").SubScope("job")),
	}

	d.Register(job.BuildJobManagerYarpcProcedures(handler))
}

// serviceHandler implements peloton.api.job.JobManager
type serviceHandler struct {
	jobStore       storage.JobStore
	taskStore      storage.TaskStore
	respoolClient  respool.ResourceManagerYarpcClient
	resmgrClient   resmgrsvc.ResourceManagerServiceYarpcClient
	runtimeUpdater *jobmgr_job.RuntimeUpdater
	rootCtx        context.Context
	metrics        *Metrics
}

// Create creates a job object for a given job configuration and
// enqueues the tasks for scheduling
func (h *serviceHandler) Create(
	ctx context.Context,
	req *job.CreateRequest) (*job.CreateResponse, error) {

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
			}, nil
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
		}, nil
	}

	log.WithField("config", jobConfig).Infof("JobManager.Create called")
	h.metrics.JobAPICreate.Inc(1)

	// Validate job config with default task configs
	err = task_config.ValidateTaskConfig(jobConfig)
	if err != nil {
		return &job.CreateResponse{
			Error: &job.CreateResponse_Error{
				InvalidConfig: &job.InvalidJobConfig{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil
	}

	err = h.jobStore.CreateJob(ctx, jobID, jobConfig, "peloton")
	if err != nil {
		h.metrics.JobCreateFail.Inc(1)
		return &job.CreateResponse{
			Error: &job.CreateResponse_Error{
				AlreadyExists: &job.JobAlreadyExists{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil
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
		instanceID := i
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.Value, instanceID,
			uuid.NewUUID().String())
		taskConfig, err := task_config.GetTaskConfig(jobID, jobConfig, i)
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
			}, nil
		}
		t := getTaskInfo(mesosTaskID, taskConfig, instanceID, jobID)
		tasks[i] = &t
	}
	// TODO: use the username of current session for createBy param
	err = h.taskStore.CreateTasks(ctx, jobID, tasks, "peloton")
	nTasks := int64(len(tasks))
	if err != nil {
		log.Errorf("Failed to create tasks (%d) for job %v: %v",
			nTasks, jobID.Value, err)
		h.metrics.TaskCreateFail.Inc(nTasks)
		// FIXME: Add a new Error type for this
		return nil, err
	}
	h.metrics.TaskCreate.Inc(nTasks)

	err = jobmgr_task.EnqueueGangs(h.rootCtx, tasks, jobConfig, h.resmgrClient)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to enqueue tasks to RM")
		h.metrics.JobCreateFail.Inc(1)
		return nil, err
	}

	jobRuntime, err := h.jobStore.GetJobRuntime(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to GetJobRuntime")
		h.metrics.JobCreateFail.Inc(1)
		return nil, err
	}
	jobRuntime.State = job.JobState_PENDING
	err = h.jobStore.UpdateJobRuntime(ctx, jobID, jobRuntime)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to UpdateJobRuntime")
		h.metrics.JobCreateFail.Inc(1)
		return nil, err
	}

	log.Infof("Job %v all %v tasks created, time spent: %v",
		jobID.Value, instances, time.Since(startAddTaskTime))

	return &job.CreateResponse{
		JobId: jobID,
	}, nil
}

// Update updates a job object for a given job configuration and
// performs the appropriate action based on the change
func (h *serviceHandler) Update(
	ctx context.Context,
	req *job.UpdateRequest) (*job.UpdateResponse, error) {

	jobID := req.Id
	jobRuntime, err := h.jobStore.GetJobRuntime(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.Value).
			Error("Failed to GetJobRuntime")
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	if !jobmgr_job.NonTerminatedStates[jobRuntime.State] {
		msg := fmt.Sprintf("Job is in a terminal state:%s", jobRuntime.State)
		h.metrics.JobUpdateFail.Inc(1)
		return &job.UpdateResponse{
			Error: &job.UpdateResponse_Error{
				InvalidJobId: &job.InvalidJobId{
					Id:      req.Id,
					Message: msg,
				},
			},
		}, nil
	}

	newConfig := req.Config
	oldConfig, err := h.jobStore.GetJobConfig(ctx, jobID)

	if newConfig.RespoolID == nil {
		newConfig.RespoolID = oldConfig.RespoolID
	}

	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.Value).
			Error("Failed to GetJobConfig")
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	diff, err := updater.CalculateJobDiff(jobID, oldConfig, newConfig)
	if err != nil {
		h.metrics.JobUpdateFail.Inc(1)
		return &job.UpdateResponse{
			Error: &job.UpdateResponse_Error{
				InvalidConfig: &job.InvalidJobConfig{
					Id:      jobID,
					Message: err.Error(),
				},
			},
		}, nil
	}

	if diff.IsNoop() {
		log.WithField("job_id", jobID).
			Info("update is a noop")
		return nil, nil
	}

	err = h.jobStore.UpdateJobConfig(ctx, jobID, newConfig)
	if err != nil {
		h.metrics.JobUpdateFail.Inc(1)
		return &job.UpdateResponse{
			Error: &job.UpdateResponse_Error{
				JobNotFound: &job.JobNotFound{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil
	}

	log.WithField("job_id", jobID.Value).
		Infof("adding %d instances", len(diff.InstancesToAdd))

	var tasks []*task.TaskInfo
	for instanceID, taskConfig := range diff.InstancesToAdd {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.Value, instanceID,
			uuid.NewUUID().String())
		t := getTaskInfo(mesosTaskID, taskConfig, instanceID, jobID)
		tasks = append(tasks, &t)
	}

	err = h.taskStore.CreateTasks(ctx, jobID, tasks, "peloton")
	nTasks := int64(len(tasks))
	if err != nil {
		log.Errorf("Failed to create tasks (%d) for job %v: %v",
			nTasks, jobID.Value, err)
		h.metrics.TaskCreateFail.Inc(nTasks)
		// FIXME: Add a new Error type for this
		return nil, err
	}
	h.metrics.TaskCreate.Inc(nTasks)

	err = jobmgr_task.EnqueueGangs(h.rootCtx, tasks, newConfig, h.resmgrClient)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to enqueue tasks to RM")
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	err = h.runtimeUpdater.UpdateJob(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to update job runtime")
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	msg := fmt.Sprintf("added %d instances", len(diff.InstancesToAdd))
	return &job.UpdateResponse{
		Id:      jobID,
		Message: msg,
	}, nil
}

func getTaskInfo(mesosTaskID string, taskConfig *task.TaskConfig, instanceID uint32, jobID *peloton.JobID) task.TaskInfo {
	t := task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			State: task.TaskState_INITIALIZED,
			// New task is by default treated as batch task and get SUCCEEDED goalstate.
			// TODO(mu): Long running tasks need RUNNING as default goalstate.
			GoalState: task.TaskState_SUCCEEDED,
			MesosTaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
		},
		Config:     taskConfig,
		InstanceId: instanceID,
		JobId:      jobID,
	}
	return t
}

// Get returns a job config for a given job ID
func (h *serviceHandler) Get(
	ctx context.Context,
	req *job.GetRequest) (*job.GetResponse, error) {

	h.metrics.JobAPIGet.Inc(1)

	jobConfig, err := h.jobStore.GetJobConfig(ctx, req.Id)
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
		}, nil
	}
	jobRuntime, err := h.jobStore.GetJobRuntime(ctx, req.Id)
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
		}, nil
	}
	h.metrics.JobGet.Inc(1)
	return &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id:      req.GetId(),
			Config:  jobConfig,
			Runtime: jobRuntime,
		},
	}, nil
}

// Query returns a list of jobs matching the given query
func (h *serviceHandler) Query(ctx context.Context, req *job.QueryRequest) (*job.QueryResponse, error) {
	log.WithField("request", req).Info("JobManager.Query called")
	h.metrics.JobAPIQuery.Inc(1)

	jobConfigs, total, err := h.jobStore.QueryJobs(ctx, req.GetRespoolID(), req.GetSpec())
	if err != nil {
		h.metrics.JobQueryFail.Inc(1)
		log.WithError(err).Error("Query job failed with error")
		return &job.QueryResponse{
			Error: &job.QueryResponse_Error{
				Err: &errors.UnknownError{
					Message: err.Error(),
				},
			},
		}, nil
	}

	h.metrics.JobQuery.Inc(1)
	return &job.QueryResponse{
		Records: jobConfigs,
		Pagination: &query.Pagination{
			Offset: req.GetSpec().GetPagination().GetOffset(),
			Limit:  req.GetSpec().GetPagination().GetLimit(),
			Total:  total,
		},
	}, nil
}

// Delete kills all running tasks in a job
func (h *serviceHandler) Delete(
	ctx context.Context,
	req *job.DeleteRequest) (*job.DeleteResponse, error) {

	h.metrics.JobAPIDelete.Inc(1)

	jobRuntime, err := h.jobStore.GetJobRuntime(ctx, req.GetId())
	if err != nil {
		log.WithError(err).
			WithField("job_id", req.GetId().GetValue()).
			Error("Failed to GetJobRuntime")
		h.metrics.JobUpdateFail.Inc(1)
		return nil, err
	}

	if jobmgr_job.NonTerminatedStates[jobRuntime.State] {
		h.metrics.JobUpdateFail.Inc(1)
		return nil, fmt.Errorf("Job is not in a terminal state: %s", jobRuntime.State)
	}

	if err := h.jobStore.DeleteJob(ctx, req.Id); err != nil {
		h.metrics.JobDeleteFail.Inc(1)
		log.Errorf("Delete job failed with error %v", err)
		return &job.DeleteResponse{
			Error: &job.DeleteResponse_Error{
				NotFound: &errors.JobNotFound{
					Id:      req.Id,
					Message: err.Error(),
				},
			},
		}, nil
	}
	h.metrics.JobDelete.Inc(1)
	return &job.DeleteResponse{}, nil
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

	var request = &respool.GetRequest{
		Id: respoolID,
	}
	response, err := h.respoolClient.GetResourcePool(ctx, request)
	if err != nil {
		log.WithFields(log.Fields{
			"error":     err,
			"respoolID": respoolID.Value,
		}).Error("Failed to get Resource Pool")
		return err
	}
	if response.GetError() != nil {
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
