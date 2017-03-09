package taskqueue

const (
	// RequeueBatchSize defines the batch size in tasks to requeue a
	// job upon leader fail-over
	RequeueBatchSize = uint32(1000)

	// MaxTaskQueueSize defines the max number of tasks in TaskQueue
	MaxTaskQueueSize = uint32(1000 * 1000)
)
