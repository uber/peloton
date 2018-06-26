package jobmgr

import (
	"sync"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/task/deadline"
	"code.uber.internal/infra/peloton/jobmgr/task/event"
	"code.uber.internal/infra/peloton/jobmgr/task/placement"
	"code.uber.internal/infra/peloton/jobmgr/task/preemptor"
	"code.uber.internal/infra/peloton/leader"
	log "github.com/sirupsen/logrus"
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
	}
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() error {

	log.WithFields(log.Fields{"role": s.role}).Info("Gained leadership")

	s.jobFactory.Start()
	s.taskPreemptor.Start()
	s.goalstateDriver.Start()
	s.placementProcessor.Start()
	s.deadlineTracker.Start()
	s.statusUpdate.Start()

	return nil
}

// LostLeadershipCallback is the callback when the current node lost
// leadership
func (s *Server) LostLeadershipCallback() error {

	log.WithField("role", s.role).Info("Lost leadership")

	s.jobFactory.Stop()
	s.statusUpdate.Stop()
	s.placementProcessor.Stop()
	s.taskPreemptor.Stop()
	s.goalstateDriver.Stop()
	s.deadlineTracker.Stop()

	return nil
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (s *Server) ShutDownCallback() error {

	log.WithFields(log.Fields{"role": s.role}).Info("Quitting election")

	s.jobFactory.Stop()
	s.statusUpdate.Stop()
	s.placementProcessor.Stop()
	s.taskPreemptor.Stop()
	s.goalstateDriver.Stop()
	s.deadlineTracker.Stop()

	return nil
}

// GetID function returns jobmgr app address.
// This implements leader.Nomination.
func (s *Server) GetID() string {
	return s.ID
}
