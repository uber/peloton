package respool

import (
	"peloton/api/respool"
)

const (
	// RootResPoolID is the ID for Root node
	RootResPoolID = "root"
	// RequeueBatchSize defines the batch size in tasks to requeue a
	// job upon leader fail-over
	RequeueBatchSize = uint32(1000)

	// ResourcePoolPathDelimiter is the delimiter for the resource pool path
	ResourcePoolPathDelimiter = "/"

	// DefaultResPoolSchedulingPolicy is the default scheduling policy for respool
	DefaultResPoolSchedulingPolicy = respool.SchedulingPolicy_PriorityFIFO
)
