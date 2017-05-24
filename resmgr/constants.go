package resmgr

const (
	// maxPlacementQueueSize is the max size of the placement queue.
	maxPlacementQueueSize = 1000 * 1000

	// RequeueBatchSize defines the batch size in tasks to requeue a
	// job upon leader fail-over
	RequeueBatchSize = uint32(1000)
)
