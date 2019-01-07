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
	"errors"
	"sync"
	"time"

	"github.com/docker/leadership"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/zookeeper"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// Observer is an interface that describes something that can observe an election for a given role,
// and can Start() observing, query the CurrentLeader(), and Stop() observing.
type Observer interface {
	CurrentLeader() (string, error)
	Start() error
	Stop()
}

type observer struct {
	sync.Mutex
	metrics  observerMetrics
	follower *leadership.Follower
	role     string
	callback func(string) error
	leader   string
	running  bool
	stopChan chan struct{}
}

// NewObserver creates a new Observer that will watch and react to new leadership events for leaders in
// a given `role`, and will call newLeaderCallback whenever leadership changes
func NewObserver(cfg ElectionConfig, scope tally.Scope, role string, newLeaderCallback func(string) error) (Observer, error) {
	log.WithFields(log.Fields{"role": role}).Debug("Creating new observer of election")
	client, err := zookeeper.New(cfg.ZKServers, &store.Config{ConnectionTimeout: zkConnErrRetry})
	if err != nil {
		return nil, err
	}
	obs := observer{
		role:     role,
		metrics:  newObserverMetrics(scope, role),
		callback: newLeaderCallback,
		follower: leadership.NewFollower(client, leaderZkPath(cfg.Root, role)),
		stopChan: make(chan struct{}),
	}
	return &obs, nil
}

// Start begins observing the election results. When new leaders are detected, the callback will be invoked.
// watching the election happens in a background goroutine.
func (o *observer) Start() error {
	o.Lock()
	defer o.Unlock()
	if o.running {
		return errors.New("Already observing election, cannot Start again")
	}
	o.running = true
	o.metrics.Start.Inc(1)
	o.metrics.Running.Update(1)

	log.WithFields(log.Fields{"role": o.role}).Info("Watching for leadership changes")

	go o.observe()
	return nil
}

// Stop cancels the observation of an election. It will terminate the background goroutine that is observing.
func (o *observer) Stop() {
	o.Lock()
	defer o.Unlock()
	if o.running {
		o.running = false
		close(o.stopChan)
		o.follower.Stop()
		o.metrics.Stop.Inc(1)
		o.metrics.Running.Update(0)
	}
}

// CurrentLeader returns the currently observed leader, or an error if not running.
// NOTE: Calls to CurrentLeader() return an error if the Observer is not started
func (o *observer) CurrentLeader() (string, error) {
	o.Lock()
	defer o.Unlock()
	if o.running {
		return o.leader, nil
	}
	return "", errors.New("observer is not running")
}

// waitForEvent handles events like a new leader being elected, or an error occurring (i.e. a connectivity error).
// this function blocks until an event is handled from either the error channel or the leader channel. It
// should be called by a wrapper function that handles retries
func (o *observer) waitForEvent() error {
	leaderCh, errCh := o.follower.FollowElection()
	for {
		select {
		case leader, ok := <-leaderCh:
			if !ok {
				return nil
			}
			o.Lock() // make sure we lock around modifying the current leader, and invoking callback
			log.WithFields(log.Fields{"role": o.role, "leader": leader}).Info("New leader detected")
			o.metrics.LeaderChanged.Inc(1)
			o.leader = leader
			err := o.callback(leader)
			o.Unlock()
			if err != nil {
				log.WithFields(log.Fields{"role": o.role, "error": err}).Error("NewLeaderCallback failed")
			}
		case err := <-errCh:
			if err != nil {
				log.WithFields(log.Fields{"role": o.role, "error": err}).Error("Error following election")
				o.metrics.Error.Inc(1)
				return err
			}
			// just a shutdown signal from the docker/leadership lib,
			// we can propogate this and let the caller decide if we
			// should continue to run, or terminate
			return nil
		}
	}
}

// observe will repeatedly call waitForEvent(), and retry when errors are encountered
func (o *observer) observe() {
	for {
		select {
		case <-o.stopChan:
			return
		default:
			err := o.waitForEvent()
			if err != nil {
				log.WithFields(log.Fields{
					"role":  o.role,
					"error": err,
				}).Errorf("Failure observing election; retrying")
				// if we already stop the observer, return without sleep
				select {
				case <-o.stopChan:
					return
				default:
					time.Sleep(zkConnErrRetry)
				}
			}
		}
	}
}
