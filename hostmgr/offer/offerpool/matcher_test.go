package offerpool

import (
	"math"
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
