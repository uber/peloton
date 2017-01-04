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
	"peloton/job"
	"peloton/master/taskqueue"
	"peloton/task"
)

// InitTaskQueue inits the TaskQueue
func InitTaskQueue(d yarpc.Dispatcher, metrics *metrics.Metrics) *Queue {
	tq := Queue{
		metrics: metrics,
	}
	tq.tqValue.Store(util.NewMemLocalTaskQueue("sourceTaskQueue"))
	json.Register(d, json.Procedure("TaskQueue.Enqueue", tq.Enqueue))
	json.Register(d, json.Procedure("TaskQueue.Dequeue", tq.Dequeue))
	return &tq
}

// Queue is the distributed task queue for peloton
type Queue struct {
	tqValue atomic.Value
	// TODO: need to handle the case if dequeue RPC fails / follower is down
	// which can lead to some tasks are dequeued and lost. We can find those tasks
	// by reconcilation, and put those tasks back
	metrics *metrics.Metrics
}

// Enqueue enqueues tasks into the queue
func (q *Queue) Enqueue(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *taskqueue.EnqueueRequest) (*taskqueue.EnqueueResponse, yarpc.ResMeta, error) {

	tasks := body.Tasks
	log.WithField("tasks", tasks).Debug("TaskQueue.Enqueue called")
	q.metrics.QueueAPIEnqueue.Inc(1)
	for _, task := range tasks {
		q.tqValue.Load().(util.TaskQueue).PutTask(task)
		q.metrics.QueueEnqueue.Inc(1)
	}
	return &taskqueue.EnqueueResponse{}, nil, nil
}

// Dequeue dequeues tasks from the queue
func (q *Queue) Dequeue(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *taskqueue.DequeueRequest) (*taskqueue.DequeueResponse, yarpc.ResMeta, error) {

	limit := body.Limit
	log.WithField("limit", limit).Debug("TaskQueue.Dequeue called")
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
	log.WithField("tasks", tasks).Debug("TaskQueue.Dequeue returned")

	return &taskqueue.DequeueResponse{
		Tasks: tasks,
	}, nil, nil
}

// LoadFromDB would scan all non-finished jobs and put all non-finished tasks into the queue
// This is called then the peloton master becomes leader
// TODO: 1. make this async and cancelable; 2. optimize by: select total task count, and task count in each state that need retry
// so that we don't need to scan though all tasks
func (q *Queue) LoadFromDB(jobStore storage.JobStore, taskStore storage.TaskStore) error {
	jobs, err := jobStore.GetAllJobs()
	if err != nil {
		log.Errorf("Fail to get all jobs from DB, err=%v", err)
		return err
	}
	log.Infof("jobs : %v", jobs)
	for jobID, jobConf := range jobs {
		err = q.refillQueue(jobID, jobConf, taskStore)
	}
	return err
}

// refillQueue scan the tasks batch by batch, update / create task infos, also put those task records into the queue
func (q *Queue) refillQueue(jobID string, jobConf *job.JobConfig, taskStore storage.TaskStore) error {
	log.Infof("Refill task queue for job %v", jobID)
	var batchSize = uint32(1000)

	var e error
	var pJobID = &job.JobID{
		Value: jobID,
	}
	// TODO: add getTaskCount(jobId, taskState) in task store to help optimize this function
	for i := 0; i <= int(jobConf.InstanceCount/batchSize); i++ {
		from := uint32(i) * batchSize
		to := uint32(i+1)*batchSize - 1
		if to >= jobConf.InstanceCount-1 {
			to = jobConf.InstanceCount - 1
		}
		if from < to {
			log.Infof("Checking job %v instance range %v - %v", pJobID, from, to)
			taskBatch, err := taskStore.GetTasksForJobByRange(
				pJobID,
				&task.InstanceRange{
					From: from,
					To:   to,
				})
			if err != nil {
				log.Errorf("err=%v", err)
				e = err
				continue
			}
			var taskIds = make(map[string]bool)
			for _, taskInfo := range taskBatch {
				if taskInfo.Runtime.State == task.RuntimeInfo_INITIALIZED ||
					taskInfo.Runtime.State == task.RuntimeInfo_LOST ||
					taskInfo.Runtime.State == task.RuntimeInfo_ASSIGNED ||
					taskInfo.Runtime.State == task.RuntimeInfo_SCHEDULING ||
					taskInfo.Runtime.State == task.RuntimeInfo_FAILED {
					// refill the tasks with these states into the queue again
					taskInfo.Runtime.State = task.RuntimeInfo_INITIALIZED
					taskStore.UpdateTask(taskInfo)
					q.tqValue.Load().(util.TaskQueue).PutTask(taskInfo)
				}
				taskIds[*(taskInfo.Runtime.TaskId.Value)] = true
			}
			for i := int(from); i <= int(to); i++ {
				taskID := fmt.Sprintf("%s-%d", jobID, i)
				if ok, _ := taskIds[taskID]; !ok {
					log.Infof("Creating missing task %d for job %v", i, jobID)
					taskInfo := &task.TaskInfo{
						Runtime: &task.RuntimeInfo{
							State: task.RuntimeInfo_INITIALIZED,
							TaskId: &mesos.TaskID{
								Value: &taskID,
							},
						},
						JobConfig:  jobConf,
						InstanceId: uint32(i),
						JobId:      pJobID,
					}
					taskStore.CreateTask(pJobID, i, taskInfo, "peloton")
					q.tqValue.Load().(util.TaskQueue).PutTask(taskInfo)
				}
			}
		}
	}
	return e
}

// Reset would discard the queue content. Called when the current peloton master is no longer the leader
func (q *Queue) Reset() {
	q.tqValue.Store(util.NewMemLocalTaskQueue("sourceTaskQueue"))
}
