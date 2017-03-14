package task

const (
	// maxReadyQueueSize is the max size of the task ready queue.
	maxReadyQueueSize = 100 * 1000
	// NotStarted state of task scheduler
	runningStateNotStarted = 0
	// Running state of task scheduler
	runningStateRunning = 1
	// dequeueTaskLimit is the max number of pending tasks to dequeue
	dequeueTaskLimit = 1000
)
