package resmgr

import (
	"sync"

	log "github.com/Sirupsen/logrus"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/resmgr/entitlement"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/task"
)

// Server struct for handling the zk election
type Server struct {
	sync.Mutex
	ID                       string
	role                     string
	getResPoolHandler        func() respool.ServiceHandler
	getTaskScheduler         func() task.Scheduler
	getEntitlementCalculator func() entitlement.Calculator
}

// NewServer will create the elect handle object
func NewServer(port int) *Server {
	server := Server{
		ID:                       leader.NewID(port),
		role:                     common.ResourceManagerRole,
		getResPoolHandler:        respool.GetServiceHandler,
		getTaskScheduler:         task.GetScheduler,
		getEntitlementCalculator: entitlement.GetCalculator,
	}
	return &server
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithFields(log.Fields{"role": s.role}).Info("Gained leadership")

	err := s.getResPoolHandler().Start()
	if err != nil {
		log.Errorf("Failed to start respool service handler")
		return err
	}
	err = s.getTaskScheduler().Start()
	if err != nil {
		log.Errorf("Failed to start task scheduler")
		return err
	}

	err = s.getEntitlementCalculator().Start()
	if err != nil {
		log.Errorf("Failed to start entitlement Calculator")
		return err
	}
	return nil
}

// LostLeadershipCallback is the callback when the current node lost leadership
func (s *Server) LostLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithFields(log.Fields{"role": s.role}).Info("Lost leadership")

	err := s.getResPoolHandler().Stop()
	if err != nil {
		log.Errorf("Failed to stop respool service handler")
		return err
	}

	err = s.getTaskScheduler().Stop()
	if err != nil {
		log.Errorf("Failed to stop task scheduler")
		return err
	}

	err = s.getEntitlementCalculator().Stop()
	if err != nil {
		log.Errorf("Failed to stop entitlement Calculator")
		return err
	}

	return nil
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (s *Server) ShutDownCallback() error {
	s.Lock()
	defer s.Unlock()

	log.Infof("Quiting the election")
	return nil
}

// GetID function returns the peloton resource manager master address
// required to implement leader.Nomination
func (s *Server) GetID() string {
	return s.ID
}
