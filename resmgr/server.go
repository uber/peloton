package resmgr

import (
	"sync"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/resmgr/entitlement"
	"code.uber.internal/infra/peloton/resmgr/preemption"
	"code.uber.internal/infra/peloton/resmgr/respool/respoolsvc"
	"code.uber.internal/infra/peloton/resmgr/task"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// ServerProcess is the interface for a process inside a server which starts and
// stops based on the leadership delegation of the server
// TODO move all processes to use this interface
type ServerProcess interface {
	Start() error
	Stop() error
}

// Server struct for handling the zk election
type Server struct {
	sync.Mutex
	ID                       string
	role                     string
	metrics                  *Metrics
	getResPoolHandler        func() respoolsvc.ServiceHandler
	getTaskScheduler         func() task.Scheduler
	getEntitlementCalculator func() entitlement.Calculator
	getRecoveryHandler       func() RecoveryHandler
	reconciler               ServerProcess
	getPreemptor             func() preemption.Preemptor
	drainer                  ServerProcess
}

// NewServer will create the elect handle object
func NewServer(
	parent tally.Scope,
	httpPort,
	grpcPort int,
	drainer ServerProcess,
	reconciler ServerProcess) *Server {
	server := Server{
		ID:                       leader.NewID(httpPort, grpcPort),
		role:                     common.ResourceManagerRole,
		getResPoolHandler:        respoolsvc.GetServiceHandler,
		getTaskScheduler:         task.GetScheduler,
		getEntitlementCalculator: entitlement.GetCalculator,
		getRecoveryHandler:       GetRecoveryHandler,
		reconciler:               reconciler,
		getPreemptor:             preemption.GetPreemptor,
		drainer:                  drainer,
		metrics:                  NewMetrics(parent),
	}
	return &server
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithField("role", s.role).
		Info("Gained leadership")
	s.metrics.Elected.Update(1.0)

	if err := s.getResPoolHandler().Start(); err != nil {
		log.WithError(err).Error("Failed to start respool service handler")
		if err != respoolsvc.ErrServiceHandlerAlreadyStarted {
			return err
		}
	}

	if err := s.getRecoveryHandler().Start(); err != nil {
		// If we can not recover then we need to do suicide
		log.WithError(err).Fatal("Failed to start recovery handler")
		return err
	}

	if err := s.getTaskScheduler().Start(); err != nil {
		log.Errorf("Failed to start task scheduler")
		return err
	}

	if err := s.getEntitlementCalculator().Start(); err != nil {
		log.Errorf("Failed to start entitlement calculator")
		return err
	}

	if err := s.reconciler.Start(); err != nil {
		log.Errorf("Failed to start task reconciler")
		return err
	}

	if err := s.getPreemptor().Start(); err != nil {
		log.Errorf("Failed to start task preemptor")
		return err
	}

	if err := s.drainer.Start(); err != nil {
		log.Errorf("Failed to start host drainer")
		return err
	}

	return nil
}

// LostLeadershipCallback is the callback when the current node lost leadership
func (s *Server) LostLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithFields(log.Fields{"role": s.role}).Info("Lost leadership")
	s.metrics.Elected.Update(0.0)

	if err := s.getResPoolHandler().Stop(); err != nil {
		log.WithError(err).Error("Failed to stop respool service handler")
		if err != respoolsvc.ErrServiceHandlerAlreadyStopped {
			return err
		}
	}

	if err := s.getRecoveryHandler().Stop(); err != nil {
		log.Errorf("Failed to stop recovery handler")
		return err
	}

	if err := s.getTaskScheduler().Stop(); err != nil {
		log.Errorf("Failed to stop task scheduler")
		return err
	}

	if err := s.getEntitlementCalculator().Stop(); err != nil {
		log.Errorf("Failed to stop entitlement calculator")
		return err
	}

	if err := s.reconciler.Stop(); err != nil {
		log.Errorf("Failed to stop task reconciler")
		return err
	}

	if err := s.getPreemptor().Stop(); err != nil {
		log.Errorf("Failed to stop task preemptor")
		return err
	}

	if err := s.drainer.Stop(); err != nil {
		log.Errorf("Failed to stop host drainer")
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
