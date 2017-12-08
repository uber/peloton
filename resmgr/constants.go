package resmgr

const (
	// maxPlacementQueueSize is the max size of the placement queue.
	maxPlacementQueueSize = 1000 * 1000

	// RequeueTaskBatchSize defines the batch size of tasks to recover a
	// job upon leader fail-over
	RequeueTaskBatchSize = uint32(1000)

	// RequeueJobBatchSize defines the batch size of jobs to recover upon
	// leader fail-over
	RequeueJobBatchSize = uint32(10)
)
