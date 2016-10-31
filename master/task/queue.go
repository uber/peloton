package task

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	"fmt"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
	"golang.org/x/net/context"
	mesos "mesos/v1"
	"peloton/job"
	"peloton/master/taskqueue"
	"peloton/task"
	"time"
)

func InitTaskQueue(d yarpc.Dispatcher) {
	tq := TaskQueue{
		tQueue: util.NewMemLocalTaskQueue("sourceTaskQueue"),
	}
	json.Register(d, json.Procedure("TaskQueue.Enqueue", tq.Enqueue))
	json.Register(d, json.Procedure("TaskQueue.Dequeue", tq.Dequeue))
}

type TaskQueue struct {
	tQueue util.TaskQueue
	// TODO: need to handle the case if dequeue RPC fails / follower is down
	// which can lead to some tasks are dequeued and lost. We can find those tasks
	// by reconcilation, and put those tasks back
}

// Enqueue enqueues tasks into the queue
func (q *TaskQueue) Enqueue(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *taskqueue.EnqueueRequest) (*taskqueue.EnqueueResponse, yarpc.ResMeta, error) {

	tasks := body.Tasks
	log.WithField("tasks", tasks).Debug("TaskQueue.Enqueue called")
	for _, task := range tasks {
		q.tQueue.PutTask(task)
	}
	return &taskqueue.EnqueueResponse{}, nil, nil
}

// Dequeue dequeues tasks from the queue
func (q *TaskQueue) Dequeue(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *taskqueue.DequeueRequest) (*taskqueue.DequeueResponse, yarpc.ResMeta, error) {

	limit := body.Limit
	log.WithField("limit", limit).Debug("TaskQueue.Dequeue called")
	var tasks []*task.TaskInfo
	for i := 0; i < int(limit); i++ {
		task := q.tQueue.GetTask(1 * time.Millisecond)
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
func (q *TaskQueue) LoadFromDB(jobStore storage.JobStore, taskStore storage.TaskStore) error {
	jobs, err := jobStore.GetAllJobs()
	if err != nil {
		log.Errorf("Fail to get all jobs from DB, err=%v", err)
		return err
	}
	log.Infof("jobs : %v", jobs)
	for jobId, jobConf := range jobs {
		err = q.refillQueue(jobId, jobConf, taskStore)
	}
	return err
}

// refillQueue scan the tasks batch by batch, update / create task infos, also put those task records into the queue
func (q *TaskQueue) refillQueue(jobId string, jobConf *job.JobConfig, taskStore storage.TaskStore) error {
	log.Infof("Refill task queue for job %v", jobId)
	var batchSize = uint32(1000)

	var e error
	var jobID = &job.JobID{
		Value: jobId,
	}
	// TODO: add getTaskCount(jobId, taskState) in task store to help optimize this function
	for i := 0; i <= int(jobConf.InstanceCount/batchSize); i++ {
		from := uint32(i) * batchSize
		to := uint32(i+1)*batchSize - 1
		if to >= jobConf.InstanceCount-1 {
			to = jobConf.InstanceCount - 1
		}
		if from < to {
			log.Infof("Checking job %v instance range %v - %v", jobId, from, to)
			taskBatch, err := taskStore.GetTasksForJobByRange(
				jobID,
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
					q.tQueue.PutTask(taskInfo)
				}
				taskIds[*(taskInfo.Runtime.TaskId.Value)] = true
			}
			for i := int(from); i <= int(to); i++ {
				taskId := fmt.Sprintf("%s-%d", jobId, i)
				if ok, _ := taskIds[taskId]; !ok {
					log.Infof("Creating missing task %d for job %v", i, jobId)
					taskInfo := &task.TaskInfo{
						Runtime: &task.RuntimeInfo{
							State: task.RuntimeInfo_INITIALIZED,
							TaskId: &mesos.TaskID{
								Value: &taskId,
							},
						},
						JobConfig:  jobConf,
						InstanceId: uint32(i),
						JobId:      jobID,
					}
					taskStore.CreateTask(jobID, i, taskInfo, "peloton")
					q.tQueue.PutTask(taskInfo)
				}
			}
		}
	}
	return e
}
