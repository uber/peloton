package offer

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
)

type ConstraintTestSuite struct {
	suite.Suite
}

func (suite *ConstraintTestSuite) TestEffectiveHostLimit() {
	// this deprecated semantic is equivalent to a single constraints
	// with same limit.
	c := &hostsvc.Constraint{
		HostLimit: 0,
	}

	suite.Equal(uint32(1), effectiveHostLimit(c))

	c.HostLimit = 1
	suite.Equal(uint32(1), effectiveHostLimit(c))

	c.HostLimit = 10
	suite.Equal(uint32(10), effectiveHostLimit(c))
}

func TestConstraintTestSuite(t *testing.T) {
	suite.Run(t, new(ConstraintTestSuite))
}
