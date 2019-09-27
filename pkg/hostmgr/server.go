// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hostmgr

import (
	"context"
	"sync"
	"time"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/background"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/hostmgr/host/drainer"
	hpm "github.com/uber/peloton/pkg/hostmgr/hostpool/manager"
	"github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/transport/mhttp"
	"github.com/uber/peloton/pkg/hostmgr/metrics"
	"github.com/uber/peloton/pkg/hostmgr/offer"
	"github.com/uber/peloton/pkg/hostmgr/p2k/hostcache"
	"github.com/uber/peloton/pkg/hostmgr/p2k/plugins"
	"github.com/uber/peloton/pkg/hostmgr/reconcile"
	"github.com/uber/peloton/pkg/hostmgr/reserver"
	"github.com/uber/peloton/pkg/hostmgr/watchevent"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"
	"go.uber.org/yarpc/api/transport"
)

const (
	// TODO: Make these backoff configurations.
	_minBackoff = 100 * time.Millisecond
	_maxBackoff = 5 * time.Minute
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
	backgroundManager    background.Manager

	// TODO: move Mesos related fields into hostmgr.ServiceHandler
	mesosDetector mesos.MasterDetector
	mesosInbound  mhttp.Inbound
	mesosOutbound transport.Outbounds

	reconciler reconcile.TaskReconciler

	minBackoff time.Duration
	maxBackoff time.Duration

	currentBackoffNano atomic.Int64
	backoffUntilNano   atomic.Int64

	elected         atomic.Bool
	handlersRunning atomic.Bool

	// isLeader is set once leadership callback completes
	isLeader bool

	recoveryHandler RecoveryHandler

	drainer drainer.Drainer

	reserver reserver.Reserver

	metrics *metrics.Metrics

	// ticker controls connection state check loop
	ticker *time.Ticker

	// watch processor control all the client connection listening for the events
	watchProcessor watchevent.WatchProcessor

	plugin plugins.Plugin

	hostCache hostcache.HostCache

	// temporary workaround to start mesos manager on server start
	mesosManager plugins.Plugin

	hostPoolManager hpm.HostPoolManager
}

// NewServer creates a host manager Server instance.
func NewServer(
	parent tally.Scope,
	backgroundManager background.Manager,
	httpPort, grpcPort int,
	mesosDetector mesos.MasterDetector,
	mesosInbound mhttp.Inbound,
	mesosOutbound transport.Outbounds,
	reconciler reconcile.TaskReconciler,
	recoveryHandler RecoveryHandler,
	drainer drainer.Drainer,
	reserver reserver.Reserver,
	watchProcessor watchevent.WatchProcessor,
	plugin plugins.Plugin,
	hostCache hostcache.HostCache,
	mesosManager plugins.Plugin,
	hostPoolManager hpm.HostPoolManager,
) *Server {

	s := &Server{
		ID:                   leader.NewID(httpPort, grpcPort),
		role:                 common.HostManagerRole,
		getOfferEventHandler: offer.GetEventHandler,
		backgroundManager:    backgroundManager,
		mesosDetector:        mesosDetector,
		mesosInbound:         mesosInbound,
		mesosOutbound:        mesosOutbound,
		reconciler:           reconciler,
		minBackoff:           _minBackoff,
		maxBackoff:           _maxBackoff,
		recoveryHandler:      recoveryHandler,
		drainer:              drainer,
		reserver:             reserver,
		metrics:              metrics.NewMetrics(parent),
		watchProcessor:       watchProcessor,
		plugin:               plugin,
		hostCache:            hostCache,
		mesosManager:         mesosManager,
		hostPoolManager:      hostPoolManager,
	}
	log.Info("Hostmgr server started.")
	return s
}

// Start starts the ticker
func (s *Server) Start() {
	s.ticker = time.NewTicker(s.minBackoff)
	go s.ensureStateLoop(s.ticker.C)
}

// Stop stops the ticker
func (s *Server) Stop() {
	s.ticker.Stop()
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithFields(log.Fields{"role": s.role}).Info("Gained leadership")
	s.elected.Store(true)
	s.isLeader = true

	return nil
}

// LostLeadershipCallback is the callback when the current node lost
// leadership.
func (s *Server) LostLeadershipCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithField("role", s.role).Info("Lost leadership")
	s.elected.Store(false)
	s.isLeader = false

	s.watchProcessor.StopEventClients()

	return nil
}

// HasGainedLeadership returns true iff once GainedLeadershipCallback
// completes.
func (s *Server) HasGainedLeadership() bool {
	s.Lock()
	defer s.Unlock()

	return s.isLeader
}

// ShutDownCallback is the callback to shut down gracefully if possible.
func (s *Server) ShutDownCallback() error {
	s.Lock()
	defer s.Unlock()

	log.WithFields(log.Fields{"role": s.role}).Info("Quitting election")

	s.elected.Store(false)
	s.isLeader = false
	s.watchProcessor.StopEventClients()

	return nil
}

// GetID function returns the peloton master address.
// This implements leader.Nomination.
func (s *Server) GetID() string {
	return s.ID
}

// Helper function for converting boolean to float64.
func btof(v bool) float64 {
	if v {
		return 1.0
	}
	return 0.0
}

// ensureStateLoop is a function to run in a separate go-routine to ensure
// this instance respect connection state based on both leader election and
// Mesos connection.
func (s *Server) ensureStateLoop(c <-chan time.Time) {
	for range c {
		log.WithFields(log.Fields{
			"elected":         s.elected.Load(),
			"mesos_connected": s.mesosInbound.IsRunning(),
			"running":         s.handlersRunning.Load(),
		}).Debug("Maintaining Mesos connection state")
		s.ensureStateRound()
	}
}

func (s *Server) resetBackoff() {
	s.currentBackoffNano.Store(0)
	s.backoffUntilNano.Store(0)
}

// Ensure that Mesos connection and handlers are running upon
// elected.
func (s *Server) ensureRunning() {
	// Make sure Mesos connection running.
	if !s.mesosInbound.IsRunning() {
		// Ensure handlers are stopped at least once, because
		// offer handler requires that to clear previously
		// cached offers.
		// TODO: Consider start offer handler with event stream
		// then clear up there.
		if s.handlersRunning.Load() {
			s.stopHandlers()
		}

		// Retry connection with possible backoff.
		// If backoffUntilNano is zero, this will compares Now with
		// epoch zero (00:00:00 UTC Thursday 1, January 1970), which
		// should be true.
		backoffUntil := time.Unix(0, s.backoffUntilNano.Load())
		if time.Now().After(backoffUntil) {
			if shouldBackoff := s.reconnect(context.Background()); shouldBackoff {
				d := s.currentBackoffNano.Load() * 2
				if d < s.minBackoff.Nanoseconds() {
					d = s.minBackoff.Nanoseconds()
				} else if d > s.maxBackoff.Nanoseconds() {
					d = s.maxBackoff.Nanoseconds()
				}
				s.currentBackoffNano.Store(d)
				next := time.Now().Add(
					time.Nanosecond *
						time.Duration(d))
				s.backoffUntilNano.Store(
					next.UnixNano())
			} else {
				s.resetBackoff()
			}
		} else {
			log.WithField("until", backoffUntil).
				Info("Backoff Mesos connection")
		}
	} else {
		s.resetBackoff()
	}

	if !s.mesosInbound.IsRunning() {
		return
	}

	s.hostCache.Start()
	if err := s.plugin.Start(); err != nil {
		log.Errorf("Failed to start plugin: %s", err)
		return
	}
	if err := s.mesosManager.Start(); err != nil {
		log.Errorf("Failed to start mesosManager: %s", err)
		return
	}
	if s.hostPoolManager != nil {
		if err := s.hostPoolManager.Start(); err != nil {
			log.WithError(err).Error("Failed to start hostpool manager")
			return
		}
	}

	if !s.handlersRunning.Load() {
		s.startHandlers()
	}
}

// Ensure that Mesos connection and handlers are stopped, usually
// upon lost leadership.
func (s *Server) ensureStopped() {
	// Upon unelected, stop running connection and handlers.
	s.resetBackoff()

	if s.mesosInbound.IsRunning() {
		s.disconnect()
	}

	if s.handlersRunning.Load() {
		s.stopHandlers()
	}

	if s.hostPoolManager != nil {
		s.hostPoolManager.Stop()
	}
	s.plugin.Stop()
	s.mesosManager.Stop()
	s.hostCache.Stop()
}

// This function ensures desired states based on whether current
// server is elected, and whether actively connected to Mesos.
func (s *Server) ensureStateRound() {
	// Update metrics.
	s.metrics.Elected.Update(btof(s.elected.Load()))
	s.metrics.MesosConnected.Update(btof(s.mesosInbound.IsRunning()))
	s.metrics.HandlersRunning.Update(btof(s.handlersRunning.Load()))

	if !s.elected.Load() {
		s.ensureStopped()
	} else {
		s.ensureRunning()
	}
}

func (s *Server) stopHandlers() {
	log.Info("Stopping HostMgr handlers")

	s.Lock()
	defer s.Unlock()

	if s.handlersRunning.Swap(false) {
		s.recoveryHandler.Stop()
		s.backgroundManager.Stop()
		s.getOfferEventHandler().Stop()
		s.drainer.Stop()
		s.reserver.Stop()
	}
}

func (s *Server) startHandlers() {
	log.Info("Starting HostMgr handlers")

	s.Lock()
	defer s.Unlock()

	if !s.handlersRunning.Swap(true) {
		s.recoveryHandler.Start()
		// Set Explicit Reconcile Turn to true, to make sure Explicit Reconciliation runs
		// on Host Manager or Mesos Master re-election.
		s.reconciler.SetExplicitReconcileTurn(true)
		s.backgroundManager.Start()
		s.getOfferEventHandler().Start()
		s.drainer.Start()
		s.reserver.Start()
	}
}

func (s *Server) disconnect() {
	log.WithField("role", s.role).Info("Disconnecting from Mesos")

	err := s.mesosInbound.Stop()
	if err != nil {
		log.WithError(err).Error("Failed to stop mesos inbound")
	}
}

// Try to reconnect to Mesos leader if one is detected.
// If we have a leader but cannot connect to it, exponentially back off so that
// we do not overload the leader.
// Returns whether we should back off after current connection.
func (s *Server) reconnect(ctx context.Context) bool {
	log.WithField("role", s.role).Info("Connecting to Mesos")

	s.Lock()
	defer s.Unlock()

	hostPort := s.mesosDetector.HostPort()
	if len(hostPort) == 0 {
		log.Error("Failed to get leader address")
		return false
	}

	if _, err := s.mesosInbound.StartMesosLoop(ctx, hostPort); err != nil {
		log.WithError(err).Error("Failed to StartMesosLoop")
		return true
	}

	return false
}
