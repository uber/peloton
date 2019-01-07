// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	err := Retry(op, policy, nil)
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
	err := Retry(op, policy, nil)
	s.Equal(err, errTest)
	s.Equal(i, 4)
}

func (s *RetryTestSuite) TestRetryExitWithNotRetryable() {
	i := 0
	op := func() error {
		i++

		if i == 5 {
			return nil
		}

		return errTest
	}
	isRetryable := func(err error) bool {
		switch err {
		case errTest:
			return false
		}
		return true
	}
	policy := NewRetryPolicy(4, 5*time.Millisecond)
	err := Retry(op, policy, isRetryable)
	s.Equal(err, errTest)
	s.Equal(i, 1)
}

func (s *RetryTestSuite) TestCheckRetry() {
	expectedCount := 5
	count := 0
	policy := NewRetryPolicy(expectedCount, 5*time.Millisecond)
	r := NewRetrier(policy)
	for {
		count++
		if !CheckRetry(r) {
			s.Equal(count, expectedCount)
			return
		}
		// Prevent infinite loop in case of failure
		if count > (expectedCount + 1) {
			s.Equal(count, expectedCount)
			return
		}
	}
}
