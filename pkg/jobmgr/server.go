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

package jobmgr

import (
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/background"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	"github.com/uber/peloton/pkg/jobmgr/goalstate"
	"github.com/uber/peloton/pkg/jobmgr/task/deadline"
	"github.com/uber/peloton/pkg/jobmgr/task/event"
	"github.com/uber/peloton/pkg/jobmgr/task/evictor"
	"github.com/uber/peloton/pkg/jobmgr/task/placement"
	"github.com/uber/peloton/pkg/jobmgr/watchsvc"
)

// Server contains all structs necessary to run a jobmgr server.
// This struct also implements leader.Node interface so that it can
// perform leader election among multiple job manager server
// instances.
type Server struct {
	sync.Mutex

	ID   string
	role string

	jobFactory         cached.JobFactory
	taskEvictor        evictor.Evictor
	goalstateDriver    goalstate.Driver
	deadlineTracker    deadline.Tracker
	placementProcessor placement.Processor
	statusUpdate       event.StatusUpdate
	backgroundManager  background.Manager
	watchProcessor     watchsvc.WatchProcessor

	// isLeader is set once leadership callback completes
	isLeader bool
}

// NewServer creates a job manager Server instance.
func NewServer(
	httpPort, grpcPort int,
	jobFactory cached.JobFactory,
	goalstateDriver goalstate.Driver,
	taskPreemptor evictor.Evictor,
	deadlineTracker deadline.Tracker,
	placementProcessor placement.Processor,
	statusUpdate event.StatusUpdate,
	backgroundManager background.Manager,
	watchProcessor watchsvc.WatchProcessor,
) *Server {
	return &Server{
		ID:                 leader.NewID(httpPort, grpcPort),
		role:               common.JobManagerRole,
		jobFactory:         jobFactory,
		taskEvictor:        taskPreemptor,
		goalstateDriver:    goalstateDriver,
		deadlineTracker:    deadlineTracker,
		placementProcessor: placementProcessor,
		statusUpdate:       statusUpdate,
		backgroundManager:  backgroundManager,
		watchProcessor:     watchProcessor,
	}
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	defer func() {
		s.isLeader = true
	}()

	log.WithFields(log.Fields{"role": s.role}).Info("Gained leadership")
	s.jobFactory.Start()

	// goalstateDriver will perform recovery of jobs from DB as
	// part of startup. Other than cache initialization and start
	// of API handlers, recovery is the first thing which should
	// be completed before any other routines start. This ensures
	// job manager cache has the baseline state of all jobs recovered
	// from DB before handling any events which can modify this state.
	s.goalstateDriver.Start()
	s.taskEvictor.Start()
	s.placementProcessor.Start()
	s.deadlineTracker.Start()
	s.statusUpdate.Start()
	s.backgroundManager.Start()

	return nil
}

// LostLeadershipCallback is the callback when the current node lost
// leadership
func (s *Server) LostLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithField("role", s.role).Info("Lost leadership")
	s.isLeader = false

	s.statusUpdate.Stop()
	s.placementProcessor.Stop()
	s.taskEvictor.Stop()
	s.deadlineTracker.Stop()
	s.backgroundManager.Stop()
	s.goalstateDriver.Stop(true)
	s.jobFactory.Stop()
	s.watchProcessor.StopTaskClients()

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

	log.WithFields(log.Fields{"role": s.role}).Info("Quitting election")
	s.isLeader = false

	s.statusUpdate.Stop()
	s.placementProcessor.Stop()
	s.taskEvictor.Stop()
	s.deadlineTracker.Stop()
	s.backgroundManager.Stop()
	s.goalstateDriver.Stop(true)
	s.jobFactory.Stop()
	s.watchProcessor.StopTaskClients()

	return nil
}

// GetID function returns jobmgr app address.
// This implements leader.Nomination.
func (s *Server) GetID() string {
	return s.ID
}
