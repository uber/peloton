package respool

import (
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
)

const (
	// RequeueBatchSize defines the batch size in tasks to requeue a
	// job upon leader fail-over
	RequeueBatchSize = uint32(1000)

	// ResourcePoolPathDelimiter is the delimiter for the resource pool path
	ResourcePoolPathDelimiter = "/"

	// DefaultResPoolSchedulingPolicy is the default scheduling policy for respool
	DefaultResPoolSchedulingPolicy = respool.SchedulingPolicy_PriorityFIFO
)
