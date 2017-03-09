package hostmgr

import (
	"sync"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/hostmgr/offer"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc/transport"
)

// Server contains all structs necessary to run a hostmgr server.
// This struct also implements leader.Node interface so that it can
// perform leader election among multiple host manager server
// instances.
type Server struct {
	sync.Mutex
	ID                   string
	role                 string
	getOfferEventHandler func() offer.EventHandler

	// TODO: move mesos related fields into hostmgr.ServiceHandler
	mesosDetector mesos.MasterDetector
	mesosInbound  mhttp.Inbound
	mesosOutbound transport.Outbounds
}

// NewServer creates a host manager Server instance.
func NewServer(
	port int,
	mesosDetector mesos.MasterDetector,
	mesosInbound mhttp.Inbound,
	mesosOutbound transport.Outbounds) *Server {

	return &Server{
		ID:                   leader.NewID(port),
		role:                 common.HostManagerRole,
		getOfferEventHandler: offer.GetEventHandler,
		mesosDetector:        mesosDetector,
		mesosInbound:         mesosInbound,
		mesosOutbound:        mesosOutbound,
	}
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithFields(log.Fields{"role": s.role}).Info("Gained leadership")

	mesosMasterAddr, err := s.mesosDetector.GetMasterLocation()
	if err != nil {
		log.Errorf("Failed to get mesosMasterAddr, err = %v", err)
		return err
	}

	err = s.mesosInbound.StartMesosLoop(mesosMasterAddr)
	if err != nil {
		log.Errorf("Failed to StartMesosLoop, err = %v", err)
		return err
	}

	s.getOfferEventHandler().Start()

	return nil
}

// LostLeadershipCallback is the callback when the current node lost
// leadership
func (s *Server) LostLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithFields(log.Fields{"role": s.role}).Info("Lost leadership")

	err := s.mesosInbound.Stop()
	if err != nil {
		log.Errorf("Failed to stop mesos inbound, err = %v", err)
	}

	s.getOfferEventHandler().Stop()

	return err
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (s *Server) ShutDownCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithFields(log.Fields{"role": s.role}).Info("Quitting election")
	return nil
}

// GetID function returns the peloton master address
// required to implement leader.Nomination
func (s *Server) GetID() string {
	return s.ID
}
