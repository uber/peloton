package task

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"code.uber.internal/infra/peloton/master/metrics"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	mesos "mesos/v1"
	"peloton/api/job"
	"peloton/api/task"
	tq "peloton/private/resmgr/taskqueue"
)

// InitTaskQueue inits the TaskQueue
func InitTaskQueue(
	d yarpc.Dispatcher,
	metrics *metrics.Metrics,
	js storage.JobStore,
	ts storage.TaskStore) *Queue {

	tq := Queue{metrics: metrics, jobStore: js, taskStore: ts}
	tq.tqValue.Store(util.NewMemLocalTaskQueue("sourceTaskQueue"))
	json.Register(d, json.Procedure("TaskQueue.Enqueue", tq.Enqueue))
	json.Register(d, json.Procedure("TaskQueue.Dequeue", tq.Dequeue))
	return &tq
}

// Queue is the distributed task queue for peloton
// TODO: need to handle the case if dequeue RPC fails / follower is
// down which can lead to some tasks are dequeued and lost. We can
// find those tasks by reconcilation, and put those tasks back
type Queue struct {
	tqValue atomic.Value
	// TODO: need to handle the case if dequeue RPC fails / follower is down
	// which can lead to some tasks are dequeued and lost. We can find those tasks
	// by reconcilation, and put those tasks back
	metrics   *metrics.Metrics
	jobStore  storage.JobStore
	taskStore storage.TaskStore
}

// Enqueue enqueues tasks into the queue
func (q *Queue) Enqueue(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *tq.EnqueueRequest) (*tq.EnqueueResponse, yarpc.ResMeta, error) {

	tasks := req.Tasks
	log.Debug("TaskQueue.Enqueue called")
	q.metrics.QueueAPIEnqueue.Inc(1)
	for _, task := range tasks {
		q.tqValue.Load().(util.TaskQueue).PutTask(task)
		q.metrics.QueueEnqueue.Inc(1)
	}
	return &tq.EnqueueResponse{}, nil, nil
}

// Dequeue dequeues tasks from the queue
func (q *Queue) Dequeue(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *tq.DequeueRequest) (*tq.DequeueResponse, yarpc.ResMeta, error) {

	limit := req.Limit
	q.metrics.QueueAPIDequeue.Inc(1)
	var tasks []*task.TaskInfo
	for i := 0; i < int(limit); i++ {
		task := q.tqValue.Load().(util.TaskQueue).GetTask(1 * time.Millisecond)
		q.metrics.QueueDequeue.Inc(1)
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
func (q *Queue) LoadFromDB() error {

	jobs, err := q.jobStore.GetAllJobs()
	if err != nil {
		log.Errorf("Fail to get all jobs from DB, err=%v", err)
		return err
	}
	log.Infof("jobs : %v", jobs)
	for jobID, jobConfig := range jobs {
		err = q.requeueJob(jobID, jobConfig)
	}
	return err
}

// requeueJob scan the tasks batch by batch, update / create task
// infos, also put those task records into the queue
func (q *Queue) requeueJob(jobID string, jobConfig *job.JobConfig) error {

	log.Infof("Requeue job %v to task queue", jobID)

	// TODO: add getTaskCount(jobID, taskState) in task store to help
	// optimize this function
	var err error
	for i := uint32(0); i <= jobConfig.InstanceCount/RequeueBatchSize; i++ {
		from := i * RequeueBatchSize
		to := util.Min((i+1)*RequeueBatchSize, jobConfig.InstanceCount)
		err = q.requeueTasks(jobID, jobConfig, from, to)
		if err != nil {
			log.Errorf("Failed to requeue tasks for job %v in [%v, %v)",
				jobID, from, to)

		}
	}
	return err
}

// requeueTasks loads the tasks in a given range from storage and
// enqueue them to task queue
func (q *Queue) requeueTasks(
	jobID string, jobConfig *job.JobConfig, from, to uint32) error {

	log.Infof("Checking job %v instance range [%v, %v)", jobID, from, to)

	if from > to {
		return fmt.Errorf("Invalid job instance range [%v, %v)", from, to)
	} else if from == to {
		return nil
	}

	pbJobID := &job.JobID{Value: jobID}
	tasks, err := q.taskStore.GetTasksForJobByRange(
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
			q.taskStore.UpdateTask(t)
			q.tqValue.Load().(util.TaskQueue).PutTask(t)
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
			taskConfig, _ := GetTaskConfig(jobConfig, i)
			t := &task.TaskInfo{
				Runtime: &task.RuntimeInfo{
					State:  task.RuntimeInfo_INITIALIZED,
					TaskId: &mesos.TaskID{Value: &taskID},
				},
				Config:     taskConfig,
				InstanceId: uint32(i),
				JobId:      pbJobID,
			}
			q.taskStore.CreateTask(pbJobID, i, t, "peloton")
			q.tqValue.Load().(util.TaskQueue).PutTask(t)
		}
	}
	return nil
}

// Reset would discard the queue content. Called when the current
// peloton master is no longer the leader
func (q *Queue) Reset() {
	q.tqValue.Store(util.NewMemLocalTaskQueue("sourceTaskQueue"))
}
