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
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/docker/leadership"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/zookeeper"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"github.com/uber/peloton/pkg/common"
)

const (
	// ttl is the election ttl for docker/leadership.
	// Caution: required but not used.
	ttl = 5 * time.Second

	// Znode Ephemeral Timeout: timeout after which a sequential ephemeral node
	// used for leader election would disappear if heartbeat failing due to
	// network loss between the host and ZK.
	znodeEphemeralTimeout = 5 * time.Second

	// zkConnErrRetry how long to wait before restarting campaigning for
	// leadership on connection error.
	zkConnErrRetry = 30 * time.Second

	// _metricsUpdateTick is the period between consecutive emissions of leader
	// election metrics.
	_metricsUpdateTick = 10 * time.Second
)

// ElectionConfig is config related to leader election of this service.
type ElectionConfig struct {
	// A comma separated list of ZK servers to use for leader election.
	ZKServers []string `yaml:"zk_servers"`

	// The root path in ZK to use for role leader election.
	// This will be something like /peloton/YOURCLUSTERHERE.
	Root string `yaml:"root"`
}

// election holds the state of the zkelection.
type election struct {
	sync.Mutex
	metrics    electionMetrics
	running    bool
	leader     string
	role       string
	candidate  *leadership.Candidate
	nomination Nomination
	stopChan   chan struct{}
}

// NewCandidate creates new election object to control participation in leader
// election.
func NewCandidate(
	cfg ElectionConfig,
	parent tally.Scope,
	role string,
	nomination Nomination) (Candidate, error) {
	var leaderPath string
	if role == "" {
		return nil, errors.New("You need to specify a role to campaign " +
			"for that isnt the empty string")
	}

	client, err := zookeeper.New(
		cfg.ZKServers,
		&store.Config{ConnectionTimeout: znodeEphemeralTimeout},
	)
	if err != nil {
		return nil, err
	}

	if role == common.PelotonAuroraBridgeRole {
		leaderPath = leaderBridgeZKPath(cfg.Root, role)
	} else {
		leaderPath = leaderZkPath(cfg.Root, role)
	}
	log.WithFields(log.Fields{
		"id":          nomination.GetID(),
		"role":        role,
		"leader_path": leaderPath,
	}).Debug("Creating new Candidate")

	candidate := leadership.NewCandidate(
		client,
		leaderPath,
		nomination.GetID(),
		ttl,
	)
	scope := parent.SubScope("election")
	hostname, err := os.Hostname()
	if err != nil {
		log.WithError(err).Fatal("failed to get hostname")
	}
	el := election{
		running:    false,
		metrics:    newElectionMetrics(scope, hostname),
		role:       role,
		nomination: nomination,
		candidate:  candidate,
		stopChan:   make(chan struct{}),
	}

	return &el, nil
}

// Start begins running election for leadership and calls callbacks when caller
// gain/lose leadership.
// NOTE: this handles connection errors and retries, and runs until you
// call Stop().
func (el *election) Start() error {
	el.Lock()
	defer el.Unlock()

	if el.running {
		return errors.New("Already running election")
	}
	el.running = true
	el.metrics.Start.Inc(1)
	el.metrics.Running.Update(1)

	log.WithFields(log.Fields{"role": el.role}).Info("Joining election")

	// start to campaign for leadership
	go el.campaign()
	// Update leader election metrics
	go el.updateLeaderElectionMetrics(_metricsUpdateTick)

	return nil
}

// updateLeaderElectionMetric emits leader election metrics at constant
// interval.
func (el *election) updateLeaderElectionMetrics(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-el.stopChan:
			log.Info("Stopped leader election metrics emission")
			return
		case <-ticker.C:
			if el.IsLeader() {
				el.metrics.IsLeader.Update(1)
			} else {
				el.metrics.IsLeader.Update(0)
			}
		}
	}
}

// campaign will repeatedly call waitForEvent(), and retry when errors are
// encountered.
func (el *election) campaign() {
	for {
		select {
		case <-el.stopChan:
			log.Info("Stopped running election")
			return
		default:
			err := el.waitForEvent()
			if err != nil {
				log.WithFields(log.Fields{"role": el.role}).
					Errorf("Failure running election; retrying: %v", err)
				time.Sleep(zkConnErrRetry)
			}
		}
	}
}

// declareLostLeadership declares lost leadership.
func (el *election) declareLostLeadership() error {
	log.WithFields(log.Fields{
		"id":   el.nomination.GetID(),
		"role": el.role,
	}).Info("Leadership lost")
	el.metrics.LostLeadership.Inc(1)
	el.metrics.IsLeader.Update(0)
	return el.nomination.LostLeadershipCallback()
}

// waitForEvent handles events like this host gaining or losing leadership.
// NOTE: this function blocks until an event is handled from either:
// the error channel, the event channel.
// It should be called by a wrapper function that handles retries.
func (el *election) waitForEvent() error {
	electionCh, errCh := el.candidate.RunForElection()

	for {
		select {
		case isElected, ok := <-electionCh:
			// Channel is closed, terminate the loop.
			if !ok {
				return nil
			}
			if isElected {
				log.WithFields(log.Fields{
					"id":   el.nomination.GetID(),
					"role": el.role,
				}).Info("Leadership gained")
				el.metrics.GainedLeadership.Inc(1)
				el.metrics.IsLeader.Update(1)
				err := el.nomination.GainedLeadershipCallback()
				if err != nil {
					log.WithError(err).WithFields(log.Fields{
						"id":   el.nomination.GetID(),
						"role": el.role,
					}).Error("GainedLeadershipCallback failed")
					el.candidate.Resign()
				}
			} else {
				err := el.declareLostLeadership()
				if err != nil {
					log.WithError(err).WithFields(log.Fields{
						"id":   el.nomination.GetID(),
						"role": el.role,
					}).Error("LostLeadershipCallback failed")
				}
			}
		case err := <-errCh:
			if err != nil {
				log.WithError(err).WithFields(log.Fields{
					"role": el.role,
				}).Error("Error participating in election")
				el.metrics.Error.Inc(1)
				return err
			}
			// Just a shutdown signal from the docker/leadership lib, we can
			// propogate this and let the caller decide if we should continue to
			// run, or terminate.
			return nil
		}
	}
}

// Stop stops campaigning for leadership, calls shutdown.
// NOTE: dont call this more than once, or you will panic trying to close a
// closed channel.
func (el *election) Stop() error {
	el.Lock()
	defer el.Unlock()
	if el.running {
		el.running = false
		close(el.stopChan)
		el.candidate.Stop()
		el.metrics.Stop.Inc(1)
		el.metrics.Running.Update(0)
		el.metrics.Resigned.Inc(1)
	}
	return el.nomination.ShutDownCallback()
}

// IsLeader returns whether this candidate is the current leader.
func (el *election) IsLeader() bool {
	el.Lock()
	defer el.Unlock()

	// Interestingly, the candidate reports leader even if we have resigned,
	// so gate delegating to isLeader on whether we are actively campaigning for
	// the leadership.
	return el.running && el.candidate.IsLeader()
}

// Resign gives up leadership.
func (el *election) Resign() {
	el.metrics.Resigned.Inc(1)
	el.candidate.Resign()
}

// leaderZkPath returns the full ZK path to the leader node given a
// election config (the path root) and a component.
func leaderZkPath(rootPath string, role string) string {
	// NOTE: remember, there cannot be a leading / for libkv.
	return strings.TrimPrefix(path.Join(rootPath, role, "leader"), "/")
}

// leaderBridgeZKPath returns the zk path for Peloton-Aurora Bridge.
func leaderBridgeZKPath(rootPath string, role string) string {
	return strings.TrimPrefix(path.Join(rootPath, role, "member_0000000001"), "/")
}
