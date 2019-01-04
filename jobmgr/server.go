package jobmgr

import (
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/uber/peloton/common"
	"github.com/uber/peloton/common/background"
	"github.com/uber/peloton/jobmgr/cached"
	"github.com/uber/peloton/jobmgr/goalstate"
	"github.com/uber/peloton/jobmgr/task/deadline"
	"github.com/uber/peloton/jobmgr/task/event"
	"github.com/uber/peloton/jobmgr/task/placement"
	"github.com/uber/peloton/jobmgr/task/preemptor"
	"github.com/uber/peloton/leader"
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
	taskPreemptor      preemptor.Preemptor
	goalstateDriver    goalstate.Driver
	deadlineTracker    deadline.Tracker
	placementProcessor placement.Processor
	statusUpdate       event.StatusUpdate
	backgroundManager  background.Manager
}

// NewServer creates a job manager Server instance.
func NewServer(
	httpPort, grpcPort int,
	jobFactory cached.JobFactory,
	goalstateDriver goalstate.Driver,
	taskPreemptor preemptor.Preemptor,
	deadlineTracker deadline.Tracker,
	placementProcessor placement.Processor,
	statusUpdate event.StatusUpdate,
	backgroundManager background.Manager,
) *Server {
	return &Server{
		ID:                 leader.NewID(httpPort, grpcPort),
		role:               common.JobManagerRole,
		jobFactory:         jobFactory,
		taskPreemptor:      taskPreemptor,
		goalstateDriver:    goalstateDriver,
		deadlineTracker:    deadlineTracker,
		placementProcessor: placementProcessor,
		statusUpdate:       statusUpdate,
		backgroundManager:  backgroundManager,
	}
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() error {

	log.WithFields(log.Fields{"role": s.role}).Info("Gained leadership")

	s.jobFactory.Start()

	// goalstateDriver will perform recovery of jobs from DB as
	// part of startup. Other than cache initialization and start
	// of API handlers, recovery is the first thing which should
	// be completed before any other routines start. This ensures
	// job manager cache has the baseline state of all jobs recovered
	// from DB before handling any events which can modify this state.
	s.goalstateDriver.Start()
	s.taskPreemptor.Start()
	s.placementProcessor.Start()
	s.deadlineTracker.Start()
	s.statusUpdate.Start()
	s.backgroundManager.Start()

	return nil
}

// LostLeadershipCallback is the callback when the current node lost
// leadership
func (s *Server) LostLeadershipCallback() error {

	log.WithField("role", s.role).Info("Lost leadership")

	s.statusUpdate.Stop()
	s.placementProcessor.Stop()
	s.taskPreemptor.Stop()
	s.deadlineTracker.Stop()
	s.backgroundManager.Stop()
	s.goalstateDriver.Stop()
	s.jobFactory.Stop()

	return nil
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (s *Server) ShutDownCallback() error {

	log.WithFields(log.Fields{"role": s.role}).Info("Quitting election")

	s.statusUpdate.Stop()
	s.placementProcessor.Stop()
	s.taskPreemptor.Stop()
	s.deadlineTracker.Stop()
	s.backgroundManager.Stop()
	s.goalstateDriver.Stop()
	s.jobFactory.Stop()

	return nil
}

// GetID function returns jobmgr app address.
// This implements leader.Nomination.
func (s *Server) GetID() string {
	return s.ID
}
