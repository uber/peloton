package hostmgr

import (
	"sync"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/hostmgr/offer"
	"code.uber.internal/infra/peloton/hostmgr/reconcile"
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
	getTaskReconciler    func() reconcile.TaskReconciler

	// TODO: move Mesos related fields into hostmgr.ServiceHandler
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
		getTaskReconciler:    reconcile.GetTaskReconciler,
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

	// TODO(zhitao): Right now, this errors out after 5 min
	// (getMesosMasterHostPortTimeout) when a leader cannot be found.
	// This is risky for production use case as we want hostmgr to block
	// and retry infinitely as long as no ZK configuration change.

	mesosMasterAddr, err := s.mesosDetector.GetMasterLocation()
	if err != nil {
		log.Errorf("Failed to get mesosMasterAddr, err = %v", err)
		return err
	}

	err = s.mesosInbound.StartMesosLoop(mesosMasterAddr)
	if err != nil {
		log.WithError(err).Error("Failed to StartMesosLoop")
		return err
	}

	s.getOfferEventHandler().Start()

	// TODO(mu): trigger task reconciliation if Mesos master failover.
	s.getTaskReconciler().Start()

	return nil
}

// LostLeadershipCallback is the callback when the current node lost
// leadership
func (s *Server) LostLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithField("role", s.role).Info("Lost leadership")

	s.getTaskReconciler().Stop()

	err := s.mesosInbound.Stop()
	if err != nil {
		log.WithError(err).Error("Failed to stop mesos inbound")
	}

	s.getOfferEventHandler().Stop()

	return nil
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (s *Server) ShutDownCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithFields(log.Fields{"role": s.role}).Info("Quitting election")
	return nil
}

// GetID function returns the peloton master address.
// This implements leader.Nomination.
func (s *Server) GetID() string {
	return s.ID
}
