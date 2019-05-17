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

package task

import (
	"github.com/stretchr/testify/suite"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"testing"
	"time"
)

type BackoffPolicyTestSuite struct {
	suite.Suite
}

func TestBackoffPolicy(t *testing.T) {
	suite.Run(t, new(BackoffPolicyTestSuite))
}

func (suite *BackoffPolicyTestSuite) TestGetNextDuration() {
	InitPolicyFactory()

	config := &Config{
		LaunchingTimeout:          1 * time.Minute,
		PlacingTimeout:            1 * time.Minute,
		PolicyName:                ExponentialBackOffPolicy,
		PlacementAttemptsPerCycle: 3,
		PlacementRetryBackoff:     30 * time.Second,
	}
	policy, err := GetFactory().CreateBackOffPolicy(config)
	suite.NoError(err)
	suite.NotNil(policy)
	timeout := policy.GetNextBackoffDuration(nil, config)
	suite.EqualValues(timeout, 0)
	timeout = policy.GetNextBackoffDuration(&resmgr.Task{
		PlacementAttemptCount:   1,
		PlacementTimeoutSeconds: 60,
	}, nil)
	suite.EqualValues(timeout, 0)

	timeout = policy.GetNextBackoffDuration(&resmgr.Task{
		PlacementRetryCount:     1,
		PlacementTimeoutSeconds: 60,
	}, &Config{
		LaunchingTimeout:          1 * time.Minute,
		PlacingTimeout:            1 * time.Minute,
		PolicyName:                ExponentialBackOffPolicy,
		PlacementAttemptsPerCycle: 0,
		PlacementRetryBackoff:     30 * time.Second,
	})
	suite.EqualValues(timeout, 0)

	timeout = policy.GetNextBackoffDuration(&resmgr.Task{
		PlacementRetryCount:     1,
		PlacementTimeoutSeconds: 60,
	}, &Config{
		LaunchingTimeout:          1 * time.Minute,
		PlacingTimeout:            1 * time.Minute,
		PolicyName:                ExponentialBackOffPolicy,
		PlacementAttemptsPerCycle: 3,
		PlacementRetryBackoff:     0 * time.Second,
	})
	suite.EqualValues(timeout, 0)

	timeout = policy.GetNextBackoffDuration(&resmgr.Task{
		PlacementAttemptCount:   1,
		PlacementTimeoutSeconds: 60,
	}, config)
	suite.EqualValues(timeout, 30)
	timeout = policy.GetNextBackoffDuration(&resmgr.Task{
		PlacementAttemptCount:   2,
		PlacementTimeoutSeconds: 60,
	}, config)
	suite.EqualValues(timeout, 60)
	timeout = policy.GetNextBackoffDuration(&resmgr.Task{
		PlacementAttemptCount:   3,
		PlacementTimeoutSeconds: 60,
	}, config)
	suite.EqualValues(timeout, 0)
}

func (suite *BackoffPolicyTestSuite) TestCycleCompleted() {
	InitPolicyFactory()

	config := &Config{
		LaunchingTimeout:          1 * time.Minute,
		PlacingTimeout:            1 * time.Minute,
		PolicyName:                ExponentialBackOffPolicy,
		PlacementAttemptsPerCycle: 3,
		PlacementRetryBackoff:     30 * time.Second,
	}
	policy, err := GetFactory().CreateBackOffPolicy(config)
	suite.NoError(err)
	suite.NotNil(policy)
	isCompleted := policy.IsCycleCompleted(&resmgr.Task{
		PlacementAttemptCount: 0,
	}, config)
	suite.EqualValues(isCompleted, false)
	isCompleted = policy.IsCycleCompleted(&resmgr.Task{
		PlacementAttemptCount: 1,
	}, config)
	suite.EqualValues(isCompleted, false)
	isCompleted = policy.IsCycleCompleted(&resmgr.Task{
		PlacementAttemptCount: 2,
	}, config)
	suite.EqualValues(isCompleted, false)
	isCompleted = policy.IsCycleCompleted(&resmgr.Task{
		PlacementAttemptCount: 3,
	}, config)
	suite.EqualValues(isCompleted, true)
}

func (suite *BackoffPolicyTestSuite) TestAllCycleCompleted() {
	InitPolicyFactory()

	config := &Config{
		LaunchingTimeout:          1 * time.Minute,
		PlacingTimeout:            1 * time.Minute,
		PolicyName:                ExponentialBackOffPolicy,
		PlacementRetryCycle:       3,
		PlacementAttemptsPerCycle: 3,
		PlacementRetryBackoff:     30 * time.Second,
	}
	policy, err := GetFactory().CreateBackOffPolicy(config)
	suite.NoError(err)
	suite.NotNil(policy)
	isCompleted := policy.allCyclesCompleted(&resmgr.Task{
		PlacementRetryCount:   0,
		PlacementAttemptCount: 2,
	}, config)
	suite.EqualValues(isCompleted, false)
	isCompleted = policy.allCyclesCompleted(&resmgr.Task{
		PlacementRetryCount:   1,
		PlacementAttemptCount: 3,
	}, config)
	suite.EqualValues(isCompleted, false)
	isCompleted = policy.allCyclesCompleted(&resmgr.Task{
		PlacementRetryCount:   2,
		PlacementAttemptCount: 3,
	}, config)
	suite.EqualValues(isCompleted, true)
}
