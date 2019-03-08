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
