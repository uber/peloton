package task

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/util"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
	"golang.org/x/net/context"
	"peloton/master/taskqueue"
	"peloton/task"
	"time"
)

func InitTaskQueue(d yarpc.Dispatcher) {
	tq := taskQueue{
		tQueue: util.NewMemLocalTaskQueue("sourceTaskQueue"),
	}
	json.Register(d, json.Procedure("TaskQueue.Enqueue", tq.Enqueue))
	json.Register(d, json.Procedure("TaskQueue.Dequeue", tq.Dequeue))
}

type taskQueue struct {
	tQueue util.TaskQueue
	// TODO: need to handle the case if dequeue RPC fails / follower is down
	// which can lead to some tasks are dequeued and lost. We can find those tasks
	// by reconcilation, and put those tasks back
}

// Enqueue enqueues tasks into the queue
func (q *taskQueue) Enqueue(
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
func (q *taskQueue) Dequeue(
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
