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

// TestRegisterWorks_BadWork registers invalid work items
func (suite *WorkManagerTestSuite) TestRegisterWorks_BadWork() {
	manager := NewManager()

	// Empty name
	empty := Work{}
	suite.Error(manager.RegisterWorks(empty))

	// duplicates
	w := Work{Name: "w"}
	suite.NoError(manager.RegisterWorks(w))
	suite.Error(manager.RegisterWorks(w))
}

// TestStopBeforeInitialDelay stops the manager before initial delay expires
func (suite *WorkManagerTestSuite) TestStopBeforeInitialDelay() {
	v1 := atomic.Int64{}
	v2 := atomic.Int64{}

	manager := NewManager()
	err := manager.RegisterWorks(
		Work{
			Name:   "TestStopBeforeInitialDelay_1",
			Period: time.Millisecond,
			Func: func(_ *atomic.Bool) {
				v1.Inc()
			},
		},
		Work{
			Name:   "TestStopBeforeInitialDelay_2",
			Period: time.Millisecond,
			Func: func(_ *atomic.Bool) {
				v2.Inc()
			},
			InitialDelay: time.Millisecond * 100,
		},
	)
	suite.NoError(err)
	manager.Start()
	time.Sleep(time.Millisecond * 20)
	manager.Stop()
	suite.NotZero(v1.Load())
	suite.Zero(v2.Load())
}

// Test repeated start (stop) without a stop (start) on between
func (suite *WorkManagerTestSuite) TestRepeatedStartStop() {
	v1 := atomic.Int64{}
	testMgr := NewManager()
	err := testMgr.RegisterWorks(
		Work{
			Name:   "TestRepeatedStartStop",
			Period: time.Millisecond * 2,
			Func: func(_ *atomic.Bool) {
				v1.Inc()
			},
		},
	)
	suite.NoError(err)
	testMgr.Start()
	time.Sleep(time.Millisecond * 15)
	suite.NotZero(v1.Load())

	// second start
	testMgr.Start()
	time.Sleep(time.Millisecond * 15)
	suite.True(v1.Load() < 20)

	testMgr.Stop()
	runner := testMgr.(*manager).runners["TestRepeatedStartStop"]
	suite.False(runner.running.Load())
	// stop again
	testMgr.Stop()
	suite.False(runner.running.Load())
	suite.Zero(len(runner.stopChan))
}
