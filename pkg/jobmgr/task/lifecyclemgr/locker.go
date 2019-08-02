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

import "sync"

// Lockable interface defines the operations that can be locked
type Lockable interface {
	// LockKill locks kill operations including executor shutdown
	LockKill()
	// UnlockKill unlocks kill operations including executor shutdown
	UnlockKill()
	// LockLaunch locks launch operations
	LockLaunch()
	// UnlockLaunch unlocks launch operations
	UnlockLaunch()
}

// lockState represents if API has no lock/ read lock or write lock
type lockState struct {
	sync.RWMutex
	// if first bit is set, it means kill ops are locked.
	// if second bit is set, it means launch ops are locked.
	state int
}

// LockKill locks kill operations including executor shutdown
func (l *lockState) LockKill() {
	l.Lock()
	defer l.Unlock()

	l.state = l.state | 0x1
}

// LockLaunch locks launch operations
func (l *lockState) LockLaunch() {
	l.Lock()
	defer l.Unlock()

	l.state = l.state | (0x1 << 1)
}

// UnlockKill unlocks kill operations including executor shutdown
func (l *lockState) UnlockKill() {
	l.Lock()
	defer l.Unlock()

	l.state = l.state & (^0x1)
}

// UnlockLaunch unlocks launch operations
func (l *lockState) UnlockLaunch() {
	l.Lock()
	defer l.Unlock()

	l.state = l.state & (^(0x1 << 1))
}

func (l *lockState) hasKillLock() bool {
	l.RLock()
	defer l.RUnlock()

	return (l.state & 0x1) != 0
}

func (l *lockState) hasLaunchLock() bool {
	l.RLock()
	defer l.RUnlock()

	return (l.state & (0x1 << 1)) != 0
}
