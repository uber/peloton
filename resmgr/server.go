package resmgr

import (
	"sync"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/resmgr/entitlement"
	"code.uber.internal/infra/peloton/resmgr/preemption"
	respoolsvc "code.uber.internal/infra/peloton/resmgr/respool/respoolsvc"
	"code.uber.internal/infra/peloton/resmgr/task"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

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
	getReconciler            func() task.Reconciler
	getPreemptor             func() preemption.Preemptor
}

// NewServer will create the elect handle object
func NewServer(parent tally.Scope, httpPort, grpcPort int) *Server {
	server := Server{
		ID:                       leader.NewID(httpPort, grpcPort),
		role:                     common.ResourceManagerRole,
		getResPoolHandler:        respoolsvc.GetServiceHandler,
		getTaskScheduler:         task.GetScheduler,
		getEntitlementCalculator: entitlement.GetCalculator,
		getRecoveryHandler:       GetRecoveryHandler,
		getReconciler:            task.GetReconciler,
		getPreemptor:             preemption.GetPreemptor,
		metrics:                  NewMetrics(parent),
	}
	return &server
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithFields(log.Fields{"role": s.role}).Info("Gained leadership")
	s.metrics.Elected.Update(1.0)

	err := s.getResPoolHandler().Start()
	if err != nil {
		log.WithError(err).Error("Failed to start respool service handler")
		if err != respoolsvc.ErrServiceHandlerAlreadyStarted {
			return err
		}
	}
	err = s.getRecoveryHandler().Start()
	if err != nil {
		// If we can not recover then we need to do suicide
		log.WithError(err).Fatal("Failed to start recovery handler")
		return err
	}

	err = s.getTaskScheduler().Start()
	if err != nil {
		log.Errorf("Failed to start task scheduler")
		return err
	}

	err = s.getEntitlementCalculator().Start()
	if err != nil {
		log.Errorf("Failed to start entitlement calculator")
		return err
	}

	err = s.getReconciler().Start()
	if err != nil {
		log.Errorf("Failed to start task reconciler")
		return err
	}

	err = s.getPreemptor().Start()
	if err != nil {
		log.Errorf("Failed to start task preemptor")
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

	err := s.getResPoolHandler().Stop()
	if err != nil {
		log.WithError(err).Error("Failed to stop respool service handler")
		if err != respoolsvc.ErrServiceHandlerAlreadyStopped {
			return err
		}
	}

	err = s.getRecoveryHandler().Stop()
	if err != nil {
		log.Errorf("Failed to stop recovery handler")
		return err
	}

	err = s.getTaskScheduler().Stop()
	if err != nil {
		log.Errorf("Failed to stop task scheduler")
		return err
	}

	err = s.getEntitlementCalculator().Stop()
	if err != nil {
		log.Errorf("Failed to stop entitlement calculator")
		return err
	}

	err = s.getReconciler().Stop()
	if err != nil {
		log.Errorf("Failed to stop task reconciler")
		return err
	}

	err = s.getPreemptor().Stop()
	if err != nil {
		log.Errorf("Failed to stop task preemptor")
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
