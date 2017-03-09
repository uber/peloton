package taskqueue

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	jm_task "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	mesos "mesos/v1"
	"peloton/api/job"
	"peloton/api/task"
	tq "peloton/private/resmgr/taskqueue"
)

// ServiceHandler defines the interface for taskqueue service handler
// to be called by leader election callbacks.
type ServiceHandler interface {
	LoadFromDB() error
	Reset()
}

// serviceHandler implements peloton.private.resmgr.taskqueue.TaskQueue
// TODO: need to handle the case if dequeue RPC fails / follower is
// down which can lead to some tasks are dequeued and lost. We can
// find those tasks by reconcilation, and put those tasks back
type serviceHandler struct {
	tqValue atomic.Value
	// TODO: need to handle the case if dequeue RPC fails / follower
	// is down which can lead to some tasks are dequeued and lost. We
	// can find those tasks by reconcilation, and put those tasks back
	metrics   *Metrics
	jobStore  storage.JobStore
	taskStore storage.TaskStore
}

// Singleton service handler for TaskQueue
var handler *serviceHandler

// InitServiceHandler initialize the ServiceHandler for TaskQueue
func InitServiceHandler(
	d yarpc.Dispatcher,
	parent tally.Scope,
	jobStore storage.JobStore,
	taskStore storage.TaskStore) {

	if handler != nil {
		log.Warning("TaskQueue service handler has already been initialized")
		return
	}

	handler = &serviceHandler{
		metrics:   NewMetrics(parent.SubScope("taskqueue")),
		jobStore:  jobStore,
		taskStore: taskStore,
	}
	handler.tqValue.Store(util.NewMemLocalTaskQueue("sourceTaskQueue"))

	json.Register(d, json.Procedure("TaskQueue.Enqueue", handler.Enqueue))
	json.Register(d, json.Procedure("TaskQueue.Dequeue", handler.Dequeue))
}

// GetServiceHandler returns the handler for TaskQueue service. This
// function assumes the handler has been initialized as part of the
// InitEventHandler function.
func GetServiceHandler() ServiceHandler {
	if handler == nil {
		log.Fatalf("TaskQueue handler is not initialized")
	}
	return handler
}

// Enqueue enqueues tasks into the queue
func (h *serviceHandler) Enqueue(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *tq.EnqueueRequest) (*tq.EnqueueResponse, yarpc.ResMeta, error) {

	tasks := req.Tasks
	log.Debug("TaskQueue.Enqueue called")
	h.metrics.APIEnqueue.Inc(1)
	for _, task := range tasks {
		h.tqValue.Load().(util.TaskQueue).PutTask(task)
		h.metrics.Enqueue.Inc(1)
	}
	return &tq.EnqueueResponse{}, nil, nil
}

// Dequeue dequeues tasks from the queue
func (h *serviceHandler) Dequeue(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *tq.DequeueRequest) (*tq.DequeueResponse, yarpc.ResMeta, error) {

	limit := req.Limit
	h.metrics.APIDequeue.Inc(1)
	var tasks []*task.TaskInfo
	for i := 0; i < int(limit); i++ {
		task := h.tqValue.Load().(util.TaskQueue).GetTask(1 * time.Millisecond)
		h.metrics.Dequeue.Inc(1)
		if task != nil {
			tasks = append(tasks, task)
		} else {
			break
		}
	}

	// TODO: Add metrics to profile timing for dequeue
	return &tq.DequeueResponse{
		Tasks: tasks,
	}, nil, nil
}

// LoadFromDB would scan all non-finished jobs and put all
// non-finished tasks into the queue This is called then the peloton
// master becomes leader
// TODO:
//  1. make this async and cancelable;
//  2. optimize by: select total task count, and task count in each
//     state that need retry so that we don't need to scan though all
//     tasks
func (h *serviceHandler) LoadFromDB() error {

	jobs, err := h.jobStore.GetAllJobs()
	if err != nil {
		log.Errorf("Fail to get all jobs from DB, err=%v", err)
		return err
	}
	log.Infof("jobs : %v", jobs)
	for jobID, jobConfig := range jobs {
		err = h.requeueJob(jobID, jobConfig)
	}
	return err
}

// requeueJob scan the tasks batch by batch, update / create task
// infos, also put those task records into the queue
func (h *serviceHandler) requeueJob(
	jobID string,
	jobConfig *job.JobConfig) error {

	log.Infof("Requeue job %v to task queue", jobID)

	// TODO: add getTaskCount(jobID, taskState) in task store to help
	// optimize this function
	var err error
	for i := uint32(0); i <= jobConfig.InstanceCount/RequeueBatchSize; i++ {
		from := i * RequeueBatchSize
		to := util.Min((i+1)*RequeueBatchSize, jobConfig.InstanceCount)
		err = h.requeueTasks(jobID, jobConfig, from, to)
		if err != nil {
			log.Errorf("Failed to requeue tasks for job %v in [%v, %v)",
				jobID, from, to)

		}
	}
	return err
}

// requeueTasks loads the tasks in a given range from storage and
// enqueue them to task queue
func (h *serviceHandler) requeueTasks(
	jobID string,
	jobConfig *job.JobConfig,
	from, to uint32) error {

	log.Infof("Checking job %v instance range [%v, %v)", jobID, from, to)

	if from > to {
		return fmt.Errorf("Invalid job instance range [%v, %v)", from, to)
	} else if from == to {
		return nil
	}

	pbJobID := &job.JobID{Value: jobID}
	tasks, err := h.taskStore.GetTasksForJobByRange(
		pbJobID,
		&task.InstanceRange{
			From: from,
			To:   to,
		})
	if err != nil {
		return err
	}

	taskIDs := make(map[string]bool)
	for _, t := range tasks {
		switch t.Runtime.State {
		case task.RuntimeInfo_INITIALIZED,
			task.RuntimeInfo_LOST,
			task.RuntimeInfo_ASSIGNED,
			task.RuntimeInfo_SCHEDULING,
			task.RuntimeInfo_FAILED:

			// Requeue the tasks with these states into the queue again
			t.Runtime.State = task.RuntimeInfo_INITIALIZED
			h.taskStore.UpdateTask(t)
			h.tqValue.Load().(util.TaskQueue).PutTask(t)
		default:
			// Pass
		}
		taskID := fmt.Sprintf("%s-%d", jobID, t.InstanceId)
		taskIDs[taskID] = true
	}

	for i := from; i < to; i++ {
		taskID := fmt.Sprintf("%s-%d", jobID, i)
		if exist, _ := taskIDs[taskID]; !exist {
			log.Infof("Creating missing task %d for job %v", i, jobID)
			taskConfig, _ := jm_task.GetTaskConfig(jobConfig, i)
			t := &task.TaskInfo{
				Runtime: &task.RuntimeInfo{
					State:  task.RuntimeInfo_INITIALIZED,
					TaskId: &mesos.TaskID{Value: &taskID},
				},
				Config:     taskConfig,
				InstanceId: uint32(i),
				JobId:      pbJobID,
			}
			h.taskStore.CreateTask(pbJobID, i, t, "peloton")
			h.tqValue.Load().(util.TaskQueue).PutTask(t)
		}
	}
	return nil
}

// Reset would discard the queue content. Called when the current
// peloton master is no longer the leader
func (h *serviceHandler) Reset() {
	h.tqValue.Store(util.NewMemLocalTaskQueue("sourceTaskQueue"))
}
