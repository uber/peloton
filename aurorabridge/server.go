package aurorabridge

import (
	"sync"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/leader"
	log "github.com/sirupsen/logrus"
)

// Server contains all structs necessary to run a aurorabrdige server.
// This struct also implements leader.Node interface so that it can
// perform leader election among multiple aurorabridge server
// instances.
type Server struct {
	sync.Mutex

	ID   string
	role string
}

// NewServer creates a aurorabridge Server instance.
func NewServer(
	httpPort int,

) *Server {
	endpoint := leader.NewEndpoint(httpPort)
	additionalEndpoints := make(map[string]leader.Endpoint)
	additionalEndpoints["http"] = endpoint

	return &Server{
		ID:   leader.NewServiceInstance(endpoint, additionalEndpoints),
		role: common.PelotonAuroraBridgeRole,
	}
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() error {
	log.WithFields(log.Fields{"role": s.role}).Info("Gained leadership")

	return nil
}

// LostLeadershipCallback is the callback when the current node lost
// leadership
func (s *Server) LostLeadershipCallback() error {
	log.WithField("role", s.role).Info("Lost leadership")

	return nil
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (s *Server) ShutDownCallback() error {
	log.WithFields(log.Fields{"role": s.role}).Info("Quitting election")

	return nil
}

// GetID function returns jobmgr app address.
// This implements leader.Nomination.
func (s *Server) GetID() string {
	return s.ID
}
