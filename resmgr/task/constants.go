package task

const (
	// maxReadyQueueSize is the max size of the task ready queue.
	maxReadyQueueSize = 100 * 1000
	// dequeueGangLimit is the max number of pending gangs to dequeue
	dequeueGangLimit = 1000
)
