package atop

import (
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
)

// NewUpdateSpec creates a new UpdateSpec.
func NewUpdateSpec(s *api.JobUpdateSettings) *stateless.UpdateSpec {
	return &stateless.UpdateSpec{
		BatchSize:                    uint32(s.GetUpdateGroupSize()),
		RollbackOnFailure:            s.GetRollbackOnFailure(),
		MaxInstanceRetries:           uint32(s.GetMaxPerInstanceFailures()),
		MaxTolerableInstanceFailures: uint32(s.GetMaxFailedInstances()),

		// Peloton does not support pulsed updates, so if block if no pulse is
		// set, then we start the update in a paused state such that it must
		// be manually continued.
		StartPaused: s.GetBlockIfNoPulsesAfterMs() > 0,
	}
}
