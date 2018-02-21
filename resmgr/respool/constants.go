package respool

import (
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
)

const (
	// ResourcePoolPathDelimiter is the delimiter for the resource pool path
	ResourcePoolPathDelimiter = "/"

	// DefaultResPoolSchedulingPolicy is the default scheduling policy for respool
	DefaultResPoolSchedulingPolicy = respool.SchedulingPolicy_PriorityFIFO
)
