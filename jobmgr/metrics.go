package jobmgr

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state of the job manager process
type Metrics struct {
	JobAPICreate  tally.Counter
	JobCreate     tally.Counter
	JobCreateFail tally.Counter
	JobAPIGet     tally.Counter
	JobGet        tally.Counter
	JobGetFail    tally.Counter
	JobAPIDelete  tally.Counter
	JobDelete     tally.Counter
	JobDeleteFail tally.Counter
	JobAPIQuery   tally.Counter
	JobQuery      tally.Counter
	JobQueryFail  tally.Counter

	TaskAPIGet      tally.Counter
	TaskGet         tally.Counter
	TaskGetFail     tally.Counter
	TaskCreate      tally.Counter
	TaskCreateFail  tally.Counter
	TaskAPIList     tally.Counter
	TaskList        tally.Counter
	TaskListFail    tally.Counter
	TaskAPIStart    tally.Counter
	TaskStart       tally.Counter
	TaskStartFail   tally.Counter
	TaskAPIStop     tally.Counter
	TaskStop        tally.Counter
	TaskStopFail    tally.Counter
	TaskAPIRestart  tally.Counter
	TaskRestart     tally.Counter
	TaskRestartFail tally.Counter

	QueueAPIEnqueue  tally.Counter
	QueueEnqueue     tally.Counter
	QueueEnqueueFail tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) Metrics {
	taskScope := scope.SubScope("task")
	jobScope := scope.SubScope("job")
	queueScope := scope.SubScope("queue")

	taskSuccessScope := taskScope.Tagged(map[string]string{"type": "success"})
	taskFailScope := taskScope.Tagged(map[string]string{"type": "fail"})
	taskAPIScope := taskScope.SubScope("api")

	jobSuccessScope := jobScope.Tagged(map[string]string{"type": "success"})
	jobFailScope := jobScope.Tagged(map[string]string{"type": "fail"})
	jobAPIScope := jobScope.SubScope("api")

	queueSuccessScope := queueScope.Tagged(map[string]string{"type": "success"})
	queueFailScope := queueScope.Tagged(map[string]string{"type": "fail"})
	queueAPIScope := queueScope.SubScope("api")

	m := Metrics{
		JobAPICreate:  jobAPIScope.Counter("create"),
		JobCreate:     jobSuccessScope.Counter("create"),
		JobCreateFail: jobFailScope.Counter("create"),
		JobAPIGet:     jobAPIScope.Counter("get"),
		JobGet:        jobSuccessScope.Counter("get"),
		JobGetFail:    jobFailScope.Counter("get"),
		JobAPIDelete:  jobAPIScope.Counter("delete"),
		JobDelete:     jobSuccessScope.Counter("delete"),
		JobDeleteFail: jobFailScope.Counter("delete"),
		JobAPIQuery:   jobAPIScope.Counter("query"),
		JobQuery:      jobSuccessScope.Counter("query"),
		JobQueryFail:  jobFailScope.Counter("query"),

		TaskAPIGet:      taskAPIScope.Counter("get"),
		TaskGet:         taskSuccessScope.Counter("get"),
		TaskGetFail:     taskFailScope.Counter("get"),
		TaskCreate:      taskSuccessScope.Counter("create"),
		TaskCreateFail:  taskFailScope.Counter("create"),
		TaskAPIList:     taskAPIScope.Counter("list"),
		TaskList:        taskSuccessScope.Counter("list"),
		TaskListFail:    taskFailScope.Counter("list"),
		TaskAPIStart:    taskAPIScope.Counter("start"),
		TaskStart:       taskSuccessScope.Counter("start"),
		TaskStartFail:   taskFailScope.Counter("start"),
		TaskAPIStop:     taskAPIScope.Counter("stop"),
		TaskStop:        taskSuccessScope.Counter("stop"),
		TaskStopFail:    taskFailScope.Counter("stop"),
		TaskAPIRestart:  taskAPIScope.Counter("restart"),
		TaskRestart:     taskSuccessScope.Counter("restart"),
		TaskRestartFail: taskFailScope.Counter("restart"),

		QueueAPIEnqueue:  queueAPIScope.Counter("enqueue"),
		QueueEnqueue:     queueSuccessScope.Counter("enqueue"),
		QueueEnqueueFail: queueFailScope.Counter("enqueue"),
	}
	return m
}
