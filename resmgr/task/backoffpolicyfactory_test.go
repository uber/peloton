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
