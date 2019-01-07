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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/leadership"
	"github.com/docker/libkv/store"
	libkvmock "github.com/docker/libkv/store/mock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/uber-go/tally"
)

func TestObserver(t *testing.T) {
	// the zkservers will be replaced with the mock libkv client, dont worry :)
	zkpath := "/peloton/fake"
	key := strings.TrimPrefix(zkpath, "/")
	role := "testrole"
	events := make(chan string)

	kv, err := libkvmock.New([]string{}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	mockStore := kv.(*libkvmock.Mock)
	kvCh := make(chan *store.KVPair)
	var mockKVCh <-chan *store.KVPair = kvCh
	mockStore.On("Watch", key, mock.Anything).Return(mockKVCh, nil)

	o := observer{
		follower: leadership.NewFollower(mockStore, key),
		role:     role,
		metrics:  newObserverMetrics(tally.NoopScope, "testobserverrole"),
		stopChan: make(chan struct{}),
		callback: func(leader string) error {
			log.Infof("NewLeaderCallback called with %s", leader)
			events <- "new_leader:" + leader
			return nil
		},
	}

	// Simulate leader updates
	go func() {
		updates := []string{"leader1", "leader2", "leader2", "leader3", "leader3", "leader1", "leader2"}
		for _, u := range updates {
			kvCh <- &store.KVPair{Key: key, Value: []byte(u)}
			// add a bit of delay in leader change events, to sidestep when leadership changes
			// happen at the "same time", making event ordering nondeterministic
			time.Sleep(10 * time.Millisecond)
		}
	}()

	leader, err := o.CurrentLeader()
	assert.Error(t, err)

	log.Info("About to start")
	err = o.Start()
	log.Info("started")
	assert.NoError(t, err)

	// we expect to be notified of leadership changes in order, and without dupes
	expected := []string{"leader1", "leader2", "leader3", "leader1", "leader2"}
	for _, ex := range expected {
		assert.Equal(t, "new_leader:"+ex, <-events)
		leader, err = o.CurrentLeader()
		assert.NoError(t, err)
		assert.Equal(t, ex, leader)
	}

	log.Info("stopping election observer")
	o.Stop()
	log.Info("stopped election observer")

	// and then you are no longer able to observe leadership changes
	log.Info("checking leader a final time")
	leader, err = o.CurrentLeader()
	assert.Error(t, err)
	log.Info("observer testing complete!")
}

func TestObserverStop(t *testing.T) {
	role := "testrole"
	zkpath := "/peloton/fake"
	key := strings.TrimPrefix(zkpath, "/")

	kv, err := libkvmock.New([]string{}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	mockStore := kv.(*libkvmock.Mock)
	kvCh := make(chan *store.KVPair)
	var mockKVCh <-chan *store.KVPair = kvCh
	mockStore.On("Watch", key, mock.Anything).Return(mockKVCh, nil)

	o := observer{
		role:     role,
		follower: leadership.NewFollower(kv, key),
		metrics:  newObserverMetrics(tally.NoopScope, "testobserverrole"),
		stopChan: make(chan struct{}),
		running:  true,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		o.observe()
		wg.Done()
	}()
	// wait for 1 sec to make sure observer.observe calls observer.waitForEvent
	time.Sleep(1 * time.Second)
	o.Stop()
	close(kvCh)
	wg.Wait()
}
