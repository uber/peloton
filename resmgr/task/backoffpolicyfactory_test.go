package task

import (
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type BackoffPolicyFactoryTestSuite struct {
	suite.Suite
}

func TestBackoffPolicyFactory(t *testing.T) {
	suite.Run(t, new(BackoffPolicyFactoryTestSuite))
}

func (suite *BackoffPolicyFactoryTestSuite) TestCreatePolicy() {
	InitPolicyFactory()
	policy, err := GetFactory().CreateBackOffPolicy(&Config{
		LaunchingTimeout:      1 * time.Minute,
		PlacingTimeout:        1 * time.Minute,
		PolicyName:            ExponentialBackOffPolicy,
		PlacementRetryCycle:   3,
		PlacementRetryBackoff: 30 * time.Second,
	})
	suite.NoError(err)
	suite.NotNil(policy)
}

func (suite *BackoffPolicyFactoryTestSuite) TestCreatePolicyInvalidPlacingConfig() {
	InitPolicyFactory()
	policy, err := GetFactory().CreateBackOffPolicy(&Config{
		LaunchingTimeout:      1 * time.Minute,
		PlacingTimeout:        -1 * time.Minute,
		PolicyName:            ExponentialBackOffPolicy,
		PlacementRetryCycle:   3,
		PlacementRetryBackoff: 30 * time.Second,
	})
	suite.Error(err)
	suite.Nil(policy)
}

func (suite *BackoffPolicyFactoryTestSuite) TestCreatePolicyWithInvalidConf() {
	InitPolicyFactory()
	policy, err := GetFactory().CreateBackOffPolicy(nil)
	suite.Error(err)
	suite.Nil(policy)
	policy, err = GetFactory().CreateBackOffPolicy(&Config{
		LaunchingTimeout:      1 * time.Minute,
		PlacingTimeout:        1 * time.Minute,
		PolicyName:            "",
		PlacementRetryCycle:   3,
		PlacementRetryBackoff: 30 * time.Second,
	})
	suite.Error(err)
	suite.Nil(policy)
}

func (suite *BackoffPolicyFactoryTestSuite) TestCreateWrongPolicy() {
	InitPolicyFactory()
	policy, err := GetFactory().CreateBackOffPolicy(&Config{
		LaunchingTimeout:      1 * time.Minute,
		PlacingTimeout:        1 * time.Minute,
		PolicyName:            "exponential-policy-1",
		PlacementRetryCycle:   3,
		PlacementRetryBackoff: 30 * time.Second,
	})
	suite.Error(err)
	suite.Nil(policy)
}

func (suite *BackoffPolicyFactoryTestSuite) TestNoPolicy() {
	err := Factory.register(ExponentialBackOffPolicy, nil)
	suite.Error(err)
	err = InitPolicyFactory()
	suite.NoError(err)
	err = Factory.register(ExponentialBackOffPolicy, Factory.NewExponentialPolicy)
	suite.NoError(err)
}
