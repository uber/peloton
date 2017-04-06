package master

import (
	"sync"

	"code.uber.internal/infra/peloton/hostmgr"
	"code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/jobmgr"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/resmgr"
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc/transport"
)

// Server contains all structs necessary to run a master server.
// This struct also implements leader.Node interface so that it can
// perform leader election among multiple master server
// instances.
type Server struct {
	sync.Mutex
	ID   string
	apps []leader.Nomination
}

// NewServer creates a master Server instance.
func NewServer(
	port int,
	mesosDetector mesos.MasterDetector,
	mesosInbound mhttp.Inbound,
	mesosOutbound transport.Outbounds) *Server {

	rm := resmgr.NewServer(port)
	hm := hostmgr.NewServer(
		port,
		mesosDetector,
		mesosInbound,
		mesosOutbound,
	)
	jm := jobmgr.NewServer(port)
	return &Server{
		ID:   leader.NewID(port),
		apps: []leader.Nomination{rm, hm, jm},
	}
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	var lastErr error
	for _, app := range s.apps {
		err := app.GainedLeadershipCallback()
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// LostLeadershipCallback is the callback when the current node lost
// leadership
func (s *Server) LostLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	var lastErr error
	for _, app := range s.apps {
		err := app.LostLeadershipCallback()
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (s *Server) ShutDownCallback() error {
	s.Lock()
	defer s.Unlock()

	log.Infof("Quiting the election")
	return nil
}

// GetID function returns the peloton master address
// required to implement leader.Nomination
func (s *Server) GetID() string {
	return s.ID
}
