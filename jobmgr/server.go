package jobmgr

import (
	"sync"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	"code.uber.internal/infra/peloton/jobmgr/task/deadline"
	"code.uber.internal/infra/peloton/jobmgr/task/event"
	"code.uber.internal/infra/peloton/jobmgr/task/placement"
	"code.uber.internal/infra/peloton/jobmgr/task/preemptor"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/jobmgr/updatesvc"
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

	getStatusUpdate   func() event.StatusUpdate
	getStatusUpdateRM func() event.StatusUpdateRM

	taskPreemptor      preemptor.Preemptor
	goalstateEngine    goalstate.Engine
	trackedManager     tracked.Manager
	deadlineTracker    deadline.Tracker
	updateManager      updatesvc.Manager
	placementProcessor placement.Processor
}

// NewServer creates a job manager Server instance.
func NewServer(
	httpPort, grpcPort int,
	goalstateEngine goalstate.Engine,
	trackedManager tracked.Manager,
	taskPreemptor preemptor.Preemptor,
	deadlineTracker deadline.Tracker,
	updateManager updatesvc.Manager,
	placementProcessor placement.Processor,
) *Server {

	return &Server{
		ID:                 leader.NewID(httpPort, grpcPort),
		role:               common.JobManagerRole,
		getStatusUpdate:    event.GetStatusUpdater,
		getStatusUpdateRM:  event.GetStatusUpdaterRM,
		taskPreemptor:      taskPreemptor,
		goalstateEngine:    goalstateEngine,
		trackedManager:     trackedManager,
		deadlineTracker:    deadlineTracker,
		updateManager:      updateManager,
		placementProcessor: placementProcessor,
	}
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() error {

	log.WithFields(log.Fields{"role": s.role}).Info("Gained leadership")

	s.getStatusUpdateRM().Start()
	s.taskPreemptor.Start()
	s.goalstateEngine.Start()
	s.trackedManager.Start()
	s.placementProcessor.Start()
	s.deadlineTracker.Start()
	s.updateManager.Start()
	s.getStatusUpdate().Start()

	return nil
}

// LostLeadershipCallback is the callback when the current node lost
// leadership
func (s *Server) LostLeadershipCallback() error {

	log.WithField("role", s.role).Info("Lost leadership")

	s.getStatusUpdate().Stop()
	s.getStatusUpdateRM().Stop()
	s.placementProcessor.Stop()
	s.taskPreemptor.Stop()
	s.goalstateEngine.Stop()
	s.trackedManager.Stop()
	s.deadlineTracker.Stop()
	s.updateManager.Stop()

	return nil
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (s *Server) ShutDownCallback() error {

	log.WithFields(log.Fields{"role": s.role}).Info("Quitting election")

	s.getStatusUpdate().Stop()
	s.getStatusUpdateRM().Stop()
	s.placementProcessor.Stop()
	s.taskPreemptor.Stop()
	s.goalstateEngine.Stop()
	s.trackedManager.Stop()
	s.deadlineTracker.Stop()

	return nil
}

// GetID function returns jobmgr app address.
// This implements leader.Nomination.
func (s *Server) GetID() string {
	return s.ID
}
