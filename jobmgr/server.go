package jobmgr

import (
	"sync"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/leader"
	log "github.com/Sirupsen/logrus"
)

// Server contains all structs necessary to run a jobmgr server.
// This struct also implements leader.Node interface so that it can
// perform leader election among multiple job manager server
// instances.
type Server struct {
	sync.Mutex

	ID              string
	role            string
	getStatusUpdate func() task.StatusUpdate
}

// NewServer creates a job manager Server instance.
func NewServer(port int) *Server {

	return &Server{
		ID:              leader.NewID(port),
		role:            common.JobManagerRole,
		getStatusUpdate: task.GetStatusUpdater,
	}
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() error {

	log.WithFields(log.Fields{"role": s.role}).Info("Gained leadership")

	s.getStatusUpdate().Start()

	return nil
}

// LostLeadershipCallback is the callback when the current node lost
// leadership
func (s *Server) LostLeadershipCallback() error {

	log.WithField("role", s.role).Info("Lost leadership")

	s.getStatusUpdate().Stop()

	return nil
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (s *Server) ShutDownCallback() error {

	log.WithFields(log.Fields{"role": s.role}).Info("Quitting election")

	s.getStatusUpdate().Stop()

	return nil
}

// GetID function returns jobmgr app address.
// This implements leader.Nomination.
func (s *Server) GetID() string {
	return s.ID
}
