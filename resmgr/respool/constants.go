package respool

import (
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
)

const (
	// ResourcePoolPathDelimiter is the delimiter for the resource pool path
	ResourcePoolPathDelimiter = "/"

	// DefaultResPoolSchedulingPolicy is the default scheduling policy for respool
	DefaultResPoolSchedulingPolicy = respool.SchedulingPolicy_PriorityFIFO
)
