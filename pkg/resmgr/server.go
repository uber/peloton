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

package resmgr

import (
	"sync"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/resmgr/task"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// ServerProcess is the interface for a process inside a server which starts and
// stops based on the leadership delegation of the server
type ServerProcess interface {
	Start() error
	Stop() error
}

// Server struct for handling the zk election
type Server struct {
	sync.Mutex

	ID   string // The peloton resource manager master address
	role string // The role of the server

	metrics *Metrics

	// the processes which need to start with the leader
	resTree               ServerProcess
	entitlementCalculator ServerProcess
	recoveryHandler       ServerProcess
	reconciler            ServerProcess
	drainer               ServerProcess
	preemptor             ServerProcess
	batchScorer           ServerProcess
	// TODO move these to use ServerProcess
	getTaskScheduler func() task.Scheduler

	// isLeader is set once leadership callback completes
	isLeader bool
}

// NewServer will create the elect handle object
func NewServer(
	parent tally.Scope,
	httpPort,
	grpcPort int,
	tree ServerProcess,
	recoveryHandler ServerProcess,
	entitlementCalculator ServerProcess,
	reconciler ServerProcess,
	preemptor ServerProcess,
	drainer ServerProcess,
	batchScorer ServerProcess) *Server {
	return &Server{
		ID:                    leader.NewID(httpPort, grpcPort),
		role:                  common.ResourceManagerRole,
		resTree:               tree,
		getTaskScheduler:      task.GetScheduler,
		entitlementCalculator: entitlementCalculator,
		recoveryHandler:       recoveryHandler,
		reconciler:            reconciler,
		preemptor:             preemptor,
		drainer:               drainer,
		batchScorer:           batchScorer,
		metrics:               NewMetrics(parent),
	}
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() (err error) {
	s.Lock()
	defer s.Unlock()

	defer func() {
		if err == nil {
			s.isLeader = true
		}
	}()

	log.WithField("role", s.role).
		Info("Gained leadership")
	s.metrics.Elected.Update(1.0)

	// Initialize the in-memory resource tree
	if err = s.resTree.Start(); err != nil {
		log.
			WithError(err).
			Error("Failed to initialize the resource pool tree")
		return err
	}

	// Recover tasks before accepting any API requests
	if err = s.recoveryHandler.Start(); err != nil {
		// If we can not recover then we need to do suicide
		log.
			WithError(err).
			Error("Failed to start recovery handler")
		return err
	}

	// Start entitlement calculation
	if err = s.entitlementCalculator.Start(); err != nil {
		log.WithError(err).
			Error("Failed to start entitlement calculator")
		return err
	}

	// Start scheduling
	if err = s.getTaskScheduler().Start(); err != nil {
		log.
			WithError(err).
			Error("Failed to start task scheduler")
		return err
	}

	// Start reconciliation in the background
	if err = s.reconciler.Start(); err != nil {
		log.WithError(err).
			Error("Failed to start task reconciler")
		return err
	}

	// Start the preemptor
	if err = s.preemptor.Start(); err != nil {
		log.WithError(err).
			Error("Failed to start task preemptor")
		return err
	}

	// Start the drainer
	if err = s.drainer.Start(); err != nil {
		log.WithError(err).
			Error("Failed to start host drainer")
		return err
	}

	// Start the batch scorer
	if err = s.batchScorer.Start(); err != nil {
		log.WithError(err).
			Error("Failed to start batch scorer")
		return err
	}
	return nil
}

// LostLeadershipCallback is the callback when the current node lost leadership
func (s *Server) LostLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithField("role", s.role).Info("Lost leadership")
	s.metrics.Elected.Update(0.0)

	// we set the node as anon-leader before we stop the services
	s.isLeader = false

	if err := s.drainer.Stop(); err != nil {
		log.Errorf("Failed to stop host drainer")
		return err
	}

	if err := s.preemptor.Stop(); err != nil {
		log.Errorf("Failed to stop task preemptor")
		return err
	}

	if err := s.reconciler.Stop(); err != nil {
		log.Errorf("Failed to stop task reconciler")
		return err
	}

	if err := s.getTaskScheduler().Stop(); err != nil {
		log.Errorf("Failed to stop task scheduler")
		return err
	}

	if err := s.entitlementCalculator.Stop(); err != nil {
		log.Errorf("Failed to stop entitlement calculator")
		return err
	}

	if err := s.recoveryHandler.Stop(); err != nil {
		log.Errorf("Failed to stop recovery handler")
		return err
	}

	if err := s.resTree.Stop(); err != nil {
		log.Errorf("Failed to stop resource pool tree")
		return err
	}

	if err := s.batchScorer.Stop(); err != nil {
		log.Errorf("Failed to stop batch scorer")
		return err
	}

	return nil
}

// HasGainedLeadership returns true iff once GainedLeadershipCallback
// completes
func (s *Server) HasGainedLeadership() bool {
	s.Lock()
	defer s.Unlock()

	return s.isLeader
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (s *Server) ShutDownCallback() error {
	s.Lock()
	defer s.Unlock()

	log.Infof("Quiting the election")
	s.isLeader = false

	return nil
}

// GetID function returns the peloton resource manager master address
// required to implement leader.Nomination
func (s *Server) GetID() string {
	return s.ID
}
