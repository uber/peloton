package task

import (
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"github.com/stretchr/testify/suite"
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
		LaunchingTimeout:      1 * time.Minute,
		PlacingTimeout:        1 * time.Minute,
		PolicyName:            ExponentialBackOffPolicy,
		PlacementRetryCycle:   3,
		PlacementRetryBackoff: 30 * time.Second,
	}
	policy, err := GetFactory().CreateBackOffPolicy(config)
	suite.NoError(err)
	suite.NotNil(policy)
	timeout := policy.GetNextBackoffDuration(nil, config)
	suite.EqualValues(timeout, 0)
	timeout = policy.GetNextBackoffDuration(&resmgr.Task{
		PlacementRetryCount:     1,
		PlacementTimeoutSeconds: 60,
	}, nil)
	suite.EqualValues(timeout, 0)

	timeout = policy.GetNextBackoffDuration(&resmgr.Task{
		PlacementRetryCount:     1,
		PlacementTimeoutSeconds: 60,
	}, &Config{
		LaunchingTimeout:      1 * time.Minute,
		PlacingTimeout:        1 * time.Minute,
		PolicyName:            ExponentialBackOffPolicy,
		PlacementRetryCycle:   0,
		PlacementRetryBackoff: 30 * time.Second,
	})
	suite.EqualValues(timeout, 0)

	timeout = policy.GetNextBackoffDuration(&resmgr.Task{
		PlacementRetryCount:     1,
		PlacementTimeoutSeconds: 60,
	}, &Config{
		LaunchingTimeout:      1 * time.Minute,
		PlacingTimeout:        1 * time.Minute,
		PolicyName:            ExponentialBackOffPolicy,
		PlacementRetryCycle:   3,
		PlacementRetryBackoff: 0 * time.Second,
	})
	suite.EqualValues(timeout, 0)

	timeout = policy.GetNextBackoffDuration(&resmgr.Task{
		PlacementRetryCount:     1,
		PlacementTimeoutSeconds: 60,
	}, config)
	suite.EqualValues(timeout, 30)
	timeout = policy.GetNextBackoffDuration(&resmgr.Task{
		PlacementRetryCount:     2,
		PlacementTimeoutSeconds: 60,
	}, config)
	suite.EqualValues(timeout, 60)
	timeout = policy.GetNextBackoffDuration(&resmgr.Task{
		PlacementRetryCount:     3,
		PlacementTimeoutSeconds: 60,
	}, config)
	suite.EqualValues(timeout, 0)
}

func (suite *BackoffPolicyTestSuite) TestCycleCompleted() {
	InitPolicyFactory()

	config := &Config{
		LaunchingTimeout:      1 * time.Minute,
		PlacingTimeout:        1 * time.Minute,
		PolicyName:            ExponentialBackOffPolicy,
		PlacementRetryCycle:   3,
		PlacementRetryBackoff: 30 * time.Second,
	}
	policy, err := GetFactory().CreateBackOffPolicy(config)
	suite.NoError(err)
	suite.NotNil(policy)
	isCompleted := policy.IsCycleCompleted(&resmgr.Task{
		PlacementRetryCount: 1,
	}, config)
	suite.EqualValues(isCompleted, false)
	isCompleted = policy.IsCycleCompleted(&resmgr.Task{
		PlacementRetryCount: 2,
	}, config)
	suite.EqualValues(isCompleted, false)
	isCompleted = policy.IsCycleCompleted(&resmgr.Task{
		PlacementRetryCount: 3,
	}, config)
	suite.EqualValues(isCompleted, true)
}
