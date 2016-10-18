package task

import (
	"code.uber.internal/go-common.git/x/log"
	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/encoding/json"
	"golang.org/x/net/context"

	"peloton/taskqueue"
)

func InitTaskQueue(d yarpc.Dispatcher) {
	tq := taskQueue{}
	json.Register(d, json.Procedure("TaskQueue.Enqueue", tq.Enqueue))
	json.Register(d, json.Procedure("TaskQueue.Dequeue", tq.Dequeue))
}


type taskQueue struct {
}

func (q *taskQueue) Enqueue(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *taskqueue.EnqueueRequest) (*taskqueue.EnqueueResponse, yarpc.ResMeta, error) {

	tasks := body.Tasks
	log.WithField("tasks", tasks).Debug("TaskQueue.Enqueue called")

	return &taskqueue.EnqueueResponse{}, nil, nil
}

func (q *taskQueue) Dequeue(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *taskqueue.DequeueRequest) (*taskqueue.DequeueResponse, yarpc.ResMeta, error) {

	limit := body.Limit
	log.WithField("limit", limit).Debug("TaskQueue.Dequeue called")

	return &taskqueue.DequeueResponse{}, nil, nil
}
