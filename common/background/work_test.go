package background

import (
	"testing"

	"time"

	"github.com/stretchr/testify/suite"
	"github.com/uber-go/atomic"
)

type WorkManagerTestSuite struct {
	suite.Suite
}

func TestWorkManagerTestSuite(t *testing.T) {
	suite.Run(t, new(WorkManagerTestSuite))
}

func (suite *WorkManagerTestSuite) TestMultipleWorksStartStop() {
	v1 := atomic.Int64{}
	v2 := atomic.Int64{}

	manager := NewManager()
	err := manager.RegisterWorks(
		Work{
			Name:   "update_v1",
			Period: time.Millisecond,
			Func: func(_ *atomic.Bool) {
				v1.Inc()
			},
		},
		Work{
			Name:   "update_v2",
			Period: time.Millisecond,
			Func: func(_ *atomic.Bool) {
				v2.Inc()
			},
			InitialDelay: time.Millisecond * 100,
		},
	)

	suite.NoError(err)
	time.Sleep(time.Millisecond * 30)
	suite.Zero(v1.Load())
	suite.Zero(v2.Load())

	manager.Start()
	time.Sleep(time.Millisecond * 30)
	suite.NotZero(v1.Load())
	suite.Zero(v2.Load())

	time.Sleep(time.Millisecond * 100)
	suite.NotZero(v1.Load())
	suite.NotZero(v2.Load())

	manager.Stop()
	time.Sleep(time.Millisecond * 30)
	stop1 := v1.Load()
	stop2 := v2.Load()
	time.Sleep(time.Millisecond * 30)
	suite.Equal(stop1, v1.Load())
	suite.Equal(stop2, v2.Load())
}
