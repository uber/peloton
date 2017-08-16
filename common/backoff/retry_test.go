package backoff

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

var (
	errTest = errors.New("test error")
)

type RetryTestSuite struct {
	suite.Suite
}

func TestRetryTestSuite(t *testing.T) {
	suite.Run(t, new(RetryTestSuite))
}

func (s *RetryTestSuite) TestRetrySuccess() {
	i := 0
	op := func() error {
		i++

		if i == 5 {
			return nil
		}

		return errTest
	}
	policy := NewRetryPolicy(5, 5*time.Millisecond)
	err := Retry(op, policy)
	s.NoError(err)
	s.Equal(5, i)
}

func (s *RetryTestSuite) TestRetryFailed() {
	i := 0
	op := func() error {
		i++

		if i == 5 {
			return nil
		}

		return errTest
	}
	policy := NewRetryPolicy(4, 5*time.Millisecond)
	err := Retry(op, policy)
	s.Equal(err, errTest)
}
