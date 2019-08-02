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

package lifecyclemgr

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type lockStateTestSuite struct {
	suite.Suite

	lockState *lockState
}

func (suite *lockStateTestSuite) SetupTest() {
	suite.lockState = &lockState{}
}

func TestLockStateTestSuite(t *testing.T) {
	suite.Run(t, new(lockStateTestSuite))
}

// TestReadLock tests lock on kill operations
func (suite *lockStateTestSuite) TestKillLock() {
	// no lock at beginning
	suite.False(suite.lockState.hasKillLock())
	suite.False(suite.lockState.hasLaunchLock())

	// kill lock
	suite.lockState.LockKill()
	suite.True(suite.lockState.hasKillLock())
	suite.False(suite.lockState.hasLaunchLock())

	// kill unlock
	suite.lockState.UnlockKill()
	suite.False(suite.lockState.hasKillLock())
	suite.False(suite.lockState.hasLaunchLock())
}

// TestLaunchLock tests lock on launch operations
func (suite *lockStateTestSuite) TestLaunchLock() {
	// no lock at beginning
	suite.False(suite.lockState.hasKillLock())
	suite.False(suite.lockState.hasLaunchLock())

	// launch lock
	suite.lockState.LockLaunch()
	suite.False(suite.lockState.hasKillLock())
	suite.True(suite.lockState.hasLaunchLock())

	// launch unlock
	suite.lockState.UnlockLaunch()
	suite.False(suite.lockState.hasKillLock())
	suite.False(suite.lockState.hasLaunchLock())
}

// TestKillLaunchLock tests mixed lock on Kill/Launch
func (suite *lockStateTestSuite) TestKillLaunchLock() {
	// no lock at beginning
	suite.False(suite.lockState.hasKillLock())
	suite.False(suite.lockState.hasLaunchLock())

	// launch lock
	suite.lockState.LockLaunch()
	suite.False(suite.lockState.hasKillLock())
	suite.True(suite.lockState.hasLaunchLock())

	// both launch and kill lock
	suite.lockState.LockKill()
	suite.True(suite.lockState.hasKillLock())
	suite.True(suite.lockState.hasLaunchLock())

	// kill unlock, launch still locked
	suite.lockState.UnlockKill()
	suite.False(suite.lockState.hasKillLock())
	suite.True(suite.lockState.hasLaunchLock())

	// launch unlock
	suite.lockState.UnlockLaunch()
	suite.False(suite.lockState.hasKillLock())
	suite.False(suite.lockState.hasLaunchLock())
}
