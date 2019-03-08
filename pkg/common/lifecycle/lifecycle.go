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

package lifecycle

import (
	"sync"
)

// LifeCycle manages the lifecycle for the owner of the object
// example:
//	lifeCycle LifeCycle
//	lifeCycle.Start()
//	go func() {
//		select {
//		case <-lifeCycle.StopCh():
//			lifeCycle.StopComplete()
//			return
//		}
//	}()
//	lifeCycle.Stop() // block until the goroutine returns
type LifeCycle interface {
	// Start is idempotent
	// No action would be performed if Start is called twice without calling Stop
	// return false if already started
	Start() bool
	// Stop is idempotent
	// No action would be performed if Stop is called twice without calling Start
	// return false if already stopped
	Stop() bool
	// StopComplete should be called by user when the stop action terminates
	// (e.g. clean up is finished, all the goroutine has exited)
	// It would unblock Wait()
	StopComplete()
	// StopCh would broadcast the Stop message when Stop is called
	StopCh() <-chan struct{}
	// Wait() would block until StopComplete is called
	Wait()
}

type lifeCycle struct {
	sync.RWMutex
	// stopCh is not nil after Start is called,
	// StopCh is nil after Stop is called
	stopCh         chan struct{}
	stopCompleteCh chan struct{}
}

// NewLifeCycle creates a new LifeCycle instance
func NewLifeCycle() LifeCycle {
	return &lifeCycle{
		stopCompleteCh: make(chan struct{}, 1),
	}
}

func (l *lifeCycle) Start() bool {
	l.Lock()
	defer l.Unlock()

	if l.stopCh != nil {
		return false
	}

	l.stopCh = make(chan struct{})
	return true
}

func (l *lifeCycle) Stop() bool {
	l.Lock()
	defer l.Unlock()

	if l.stopCh == nil {
		return false
	}

	close(l.stopCh)
	l.stopCh = nil
	return true
}

func (l *lifeCycle) StopCh() <-chan struct{} {
	l.RLock()
	defer l.RUnlock()

	// stopCh can be nil for some edge cases, such as:
	//	l.Start()
	//	go func() {
	//	  <-l.StopCh()
	//	}()
	//	l.Stop() // called before StopCh is called
	if l.stopCh == nil {
		closedCh := make(chan struct{})
		close(closedCh)
		return closedCh
	}

	return l.stopCh
}

func (l *lifeCycle) StopComplete() {
	l.RLock()
	defer l.RUnlock()

	select {
	case l.stopCompleteCh <- struct{}{}:
		return
	default:
		// StopComplete is already called,
		// do nothing and return
		return
	}

}

func (l *lifeCycle) Wait() {
	<-l.stopCompleteCh
}
