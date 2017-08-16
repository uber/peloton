package backoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type RetryPolicyTestSuite struct {
	suite.Suite
}

func TestRetryPolicyTestSuite(t *testing.T) {
	suite.Run(t, new(RetryPolicyTestSuite))
}

func (s *RetryTestSuite) TestRetryNextBackOff() {
	policy := NewRetryPolicy(5, 5*time.Millisecond)
	r := NewRetrier(policy)
	var next time.Duration
	for i := 0; i < 4; i++ {
		next = r.NextBackOff()
		s.Equal(next, 5*time.Millisecond)
	}
}

func (s *RetryTestSuite) TestRetryMaxAttempts() {
	policy := NewRetryPolicy(5, 5*time.Millisecond)
	r := NewRetrier(policy)
	var next time.Duration
	for i := 0; i < 6; i++ {
		next = r.NextBackOff()
	}
	s.Equal(next, done)
}
