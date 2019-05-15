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

package leader

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/leadership"
	libkvmock "github.com/docker/libkv/store/mock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/uber-go/tally"
)

type testComponent struct {
	host   string
	port   string
	events chan string
}

func (x *testComponent) GainedLeadershipCallback() error {
	log.Info("GainedLeadershipCallback called")
	x.events <- "leadership_gained"
	return nil
}
func (x *testComponent) LostLeadershipCallback() error {
	log.Info("LostLeadershipCallback called")
	x.events <- "leadership_lost"
	return nil
}
func (x *testComponent) ShutDownCallback() error {
	log.Info("ShutdownCallback called")
	x.events <- "shutdown"
	return nil
}
func (x *testComponent) HasGainedLeadership() bool {
	log.Info("HasGainedLeadership called")
	x.events <- "has_gained_leadership"
	return true
}
func (x *testComponent) GetID() string { return x.host + ":" + x.port }

type testLock struct {
	lock   sync.RWMutex
	lostCh chan struct{}
}

func (l *testLock) Lock(stopChan chan struct{}) (<-chan struct{}, error) {
	l.lock.Lock()
	l.lostCh = make(chan struct{})
	return l.lostCh, nil
}

func (l *testLock) Unlock() error {
	l.lock.Unlock()
	close(l.lostCh)
	return nil
}

func TestNewCandidate(t *testing.T) {
	config := ElectionConfig{
		ZKServers: []string{"1.1.1.1:2181"},
		Root:      "peloton",
	}

	nomination := &testComponent{
		host:   "testhost",
		port:   "666",
		events: make(chan string, 100),
	}

	el, err := NewCandidate(
		config,
		tally.NoopScope,
		"aurora",
		nomination,
	)
	assert.NoError(t, err)

	err = el.Start()
	assert.NoError(t, err)
}

func TestLeaderElection(t *testing.T) {
	// the zkservers will be replaced with the mock libkv client, dont worry :)
	role := "testrole"
	zkpath := "/peloton/fake"
	key := strings.TrimPrefix(zkpath, "/")

	kv, err := libkvmock.New([]string{}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	mockStore := kv.(*libkvmock.Mock)
	testLock := &testLock{}
	// mock store should return the same lock for the same key
	mockStore.On("NewLock", key, mock.Anything).Return(testLock, nil)

	nomination := &testComponent{
		host:   "testhost",
		port:   "666",
		events: make(chan string, 100),
	}

	el := election{
		role:       role,
		metrics:    newElectionMetrics(tally.NoopScope, "hostname"),
		candidate:  leadership.NewCandidate(mockStore, key, "testhost:666", ttl),
		nomination: nomination,
		stopChan:   make(chan struct{}, 1),
	}

	log.Info("About to start")
	err = el.Start()
	log.Info("started")
	assert.NoError(t, err)

	// Should issue a false upon start, no matter what.
	assert.Equal(t, "leadership_lost", <-nomination.events)

	// Since the lock always succeeds, we should get elected.
	assert.Equal(t, "leadership_gained", <-nomination.events)
	assert.Equal(t, el.IsLeader(), true)

	// When we resign, unlock will get called, we'll be notified of the
	// de-election and we'll try to get the lock again.
	go el.Resign()
	assert.Equal(t, "leadership_lost", <-nomination.events)
	assert.Equal(t, "leadership_gained", <-nomination.events)
	assert.Equal(t, true, el.IsLeader())

	log.Info("about to stop election")
	err = el.Stop()
	log.Info("stopped election")
	assert.NoError(t, err)
	// make sure abdicating triggers shutdown handler
	assert.Equal(t, "shutdown", <-nomination.events)
	// and then you are no longer leader
	assert.Equal(t, false, el.IsLeader())

}

// electionFailureTestComponent fails the initial call of
// GainedLeadershipCallback
type electionFailureTestComponent struct {
	sync.RWMutex
	firstCall bool
	*testComponent
}

func (x *electionFailureTestComponent) GainedLeadershipCallback() error {
	x.Lock()
	defer x.Unlock()
	x.testComponent.GainedLeadershipCallback()
	if x.firstCall {
		x.firstCall = false
		return fmt.Errorf("GainedLeadershipCallback test err")
	}
	return nil
}

// if GainedLeadershipCallback fails, a new leader should be elected
func TestLeaderElectionIfGainedLeadershipCallbackFails(t *testing.T) {
	role := "testrole"
	zkpath := "/peloton/fake"
	key := strings.TrimPrefix(zkpath, "/")

	kv, err := libkvmock.New([]string{}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	mockStore := kv.(*libkvmock.Mock)
	testLock := &testLock{}
	// mock store should return the same lock for the same key
	mockStore.On("NewLock", key, mock.Anything).Return(testLock, nil)

	nomination := &electionFailureTestComponent{
		firstCall: true,
		testComponent: &testComponent{
			host:   "testhost",
			port:   "666",
			events: make(chan string, 100),
		},
	}

	el := election{
		role:       role,
		metrics:    newElectionMetrics(tally.NoopScope, "hostname"),
		candidate:  leadership.NewCandidate(mockStore, key, "testhost:666", ttl),
		nomination: nomination,
		stopChan:   make(chan struct{}, 1),
	}

	log.Info("About to start")
	err = el.Start()
	log.Info("started")
	assert.NoError(t, err)

	// Should issue a false upon start, no matter what.
	assert.Equal(t, "leadership_lost", <-nomination.events)
	// Since the lock always succeeds, we should get elected.
	assert.Equal(t, "leadership_gained", <-nomination.events)
	// GainedLeadershipCallback fails, we should lose the leadership
	assert.Equal(t, "leadership_lost", <-nomination.events)
	// regain the leadership on the second try
	assert.Equal(t, "leadership_gained", <-nomination.events)
	assert.Equal(t, true, el.IsLeader())

	log.Info("about to stop election")
	err = el.Stop()
	log.Info("stopped election")
	assert.NoError(t, err)
	// make sure abdicating triggers shutdown handler
	assert.Equal(t, "shutdown", <-nomination.events)
	// and then you are no longer leader
	assert.Equal(t, false, el.IsLeader())
}

// when an election stops, all the background goroutine should exit
func TestElectionStop(t *testing.T) {
	role := "testrole"
	zkpath := "/peloton/fake"
	key := strings.TrimPrefix(zkpath, "/")
	nomination := &testComponent{
		host:   "testhost",
		port:   "666",
		events: make(chan string, 100),
	}
	kv, err := libkvmock.New([]string{}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	mockStore := kv.(*libkvmock.Mock)
	testLock := &testLock{}
	// mock store should return the same lock for the same key
	mockStore.On("NewLock", key, mock.Anything).Return(testLock, nil)

	el := election{
		role:       role,
		metrics:    newElectionMetrics(tally.NoopScope, "hostname"),
		candidate:  leadership.NewCandidate(mockStore, key, "testhost:666", ttl),
		nomination: nomination,
		stopChan:   make(chan struct{}, 1),
		running:    true,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		el.campaign()
		el.updateLeaderElectionMetrics(time.Second)
		wg.Done()
	}()
	// Wait for events from campaign to make sure el.Stop() is called after
	// leader election begins
	assert.Equal(t, "leadership_lost", <-nomination.events)
	assert.Equal(t, "leadership_gained", <-nomination.events)
	el.Stop()
	wg.Wait()
}
