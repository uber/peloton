package hostmgr

import (
	"sync"

	"code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/hostmgr/offer"
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc/transport"
)

// Server contains all structs necessary to run a hostmgr server.
// This struct also implements leader.Node interface so that it can perform leader election
// among multiple host manager server instances.
type Server struct {
	mutex         *sync.Mutex
	cfg           *Config
	mesosDetector mesos.MasterDetector
	mesosInbound  mhttp.Inbound
	mesosOutbound transport.Outbounds
	offerManager  *offer.Manager
	localAddr     string
}

// NewServer creates a host manager Server instance.
func NewServer(
	cfg *Config,
	mesosDetector mesos.MasterDetector,
	mesosInbound mhttp.Inbound,
	mesosOutbound transport.Outbounds,
	offerManager *offer.Manager,
	localAddr string) *Server {
	return &Server{
		mutex:         &sync.Mutex{},
		cfg:           cfg,
		mesosDetector: mesosDetector,
		mesosInbound:  mesosInbound,
		mesosOutbound: mesosOutbound,
		offerManager:  offerManager,
		localAddr:     localAddr,
	}
}

// GainedLeadershipCallback is the callback when the current node becomes the leader
func (s *Server) GainedLeadershipCallback() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

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

	s.offerManager.Start()

	return nil
}

// LostLeadershipCallback is the callback when the current node lost leadership
func (s *Server) LostLeadershipCallback() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	log.Infof("Lost leadership")
	err := s.mesosInbound.Stop()
	if err != nil {
		log.Errorf("Failed to stop mesos inbound, err = %v", err)
	}

	s.offerManager.Stop()

	return err
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (s *Server) ShutDownCallback() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	log.Infof("Quiting the election")
	return nil
}

// GetID function returns the peloton master address
// required to implement leader.Nomination
func (s *Server) GetID() string {
	return s.localAddr
}
