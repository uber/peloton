package resmgr

import (
	"sync"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/resmgr/preemption"
	"code.uber.internal/infra/peloton/resmgr/task"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// ServerProcess is the interface for a process inside a server which starts and
// stops based on the leadership delegation of the server
type ServerProcess interface {
	Start() error
	Stop() error
}

// Server struct for handling the zk election
type Server struct {
	sync.Mutex

	ID   string // The peloton resource manager master address
	role string // The role of the server

	metrics *Metrics

	// the processes which need to start with the leader
	resPoolHandler        ServerProcess
	entitlementCalculator ServerProcess
	recoveryHandler       ServerProcess
	reconciler            ServerProcess
	drainer               ServerProcess

	// TODO move these to use ServerProcess
	getPreemptor     func() preemption.Preemptor
	getTaskScheduler func() task.Scheduler
}

// NewServer will create the elect handle object
func NewServer(
	parent tally.Scope,
	httpPort,
	grpcPort int,
	resPoolHandler ServerProcess,
	entitlementCalculator ServerProcess,
	recoveryHandler ServerProcess,
	drainer ServerProcess,
	reconciler ServerProcess) *Server {
	return &Server{
		ID:                    leader.NewID(httpPort, grpcPort),
		role:                  common.ResourceManagerRole,
		resPoolHandler:        resPoolHandler,
		getTaskScheduler:      task.GetScheduler,
		entitlementCalculator: entitlementCalculator,
		recoveryHandler:       recoveryHandler,
		reconciler:            reconciler,
		getPreemptor:          preemption.GetPreemptor,
		drainer:               drainer,
		metrics:               NewMetrics(parent),
	}
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithField("role", s.role).
		Info("Gained leadership")
	s.metrics.Elected.Update(1.0)

	if err := s.resPoolHandler.Start(); err != nil {
		log.
			WithError(err).
			Error("Failed to start respool service handler")
		return err
	}

	if err := s.recoveryHandler.Start(); err != nil {
		// If we can not recover then we need to do suicide
		log.
			WithError(err).
			Error("Failed to start recovery handler")
		return err
	}

	if err := s.getTaskScheduler().Start(); err != nil {
		log.
			WithError(err).
			Error("Failed to start task scheduler")
		return err
	}

	if err := s.entitlementCalculator.Start(); err != nil {
		log.WithError(err).
			Error("Failed to start entitlement calculator")
		return err
	}

	if err := s.reconciler.Start(); err != nil {
		log.WithError(err).
			Error("Failed to start task reconciler")
		return err
	}

	if err := s.getPreemptor().Start(); err != nil {
		log.WithError(err).
			Error("Failed to start task preemptor")
		return err
	}

	if err := s.drainer.Start(); err != nil {
		log.WithError(err).
			Error("Failed to start host drainer")
		return err
	}

	return nil
}

// LostLeadershipCallback is the callback when the current node lost leadership
func (s *Server) LostLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithField("role", s.role).Info("Lost leadership")
	s.metrics.Elected.Update(0.0)

	if err := s.resPoolHandler.Stop(); err != nil {
		log.WithError(err).Error("Failed to stop respool service handler")
		return err
	}

	if err := s.recoveryHandler.Stop(); err != nil {
		log.Errorf("Failed to stop recovery handler")
		return err
	}

	if err := s.getTaskScheduler().Stop(); err != nil {
		log.Errorf("Failed to stop task scheduler")
		return err
	}

	if err := s.entitlementCalculator.Stop(); err != nil {
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
