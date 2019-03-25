package noop

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNoopSecurityManager(t *testing.T) {
	m := NewNoopSecurityManager()
	u, err := m.Authenticate(nil)
	assert.NoError(t, err)

	assert.True(t, u.IsPermitted("peloton.api.v1alpha.job.stateless.svc.JobService::GetJob"))
	assert.True(t, u.IsPermitted("peloton.api.v1alpha.job.stateless.svc.JobService::CreateJob"))
	// even if the procedure name is not valid, still should pass permit check
	assert.True(t, u.IsPermitted(""))
}
