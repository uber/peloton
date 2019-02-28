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

package offerpool

import (
	"math"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
)

type ConstraintTestSuite struct {
	suite.Suite
}

func (suite *ConstraintTestSuite) TestEffectiveHostLimit() {
	// this deprecated semantic is equivalent to a single constraints
	// with same limit.
	c := &hostsvc.HostFilter{
		Quantity: &hostsvc.QuantityControl{
			MaxHosts: 0,
		},
	}

	suite.Equal(uint32(math.MaxUint32), effectiveHostLimit(c))

	c.Quantity.MaxHosts = 1
	suite.Equal(uint32(1), effectiveHostLimit(c))

	c.Quantity.MaxHosts = 10
	suite.Equal(uint32(10), effectiveHostLimit(c))
}

func TestConstraintTestSuite(t *testing.T) {
	suite.Run(t, new(ConstraintTestSuite))
}
