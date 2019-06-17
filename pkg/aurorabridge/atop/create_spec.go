package atop

import (
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
)

// NewCreateSpec creates a new CreateSpec.
func NewCreateSpec(s *api.JobUpdateSettings) *stateless.CreateSpec {
	u := NewUpdateSpec(s, false)
	return &stateless.CreateSpec{
		BatchSize:                    u.BatchSize,
		MaxInstanceRetries:           u.MaxInstanceRetries,
		MaxTolerableInstanceFailures: u.MaxTolerableInstanceFailures,
		StartPaused:                  u.StartPaused,
	}
}
