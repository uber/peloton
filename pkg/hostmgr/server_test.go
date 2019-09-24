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
	"errors"
	"testing"
	"time"

	backgound_mocks "github.com/uber/peloton/pkg/common/background/mocks"
	drainer_mocks "github.com/uber/peloton/pkg/hostmgr/host/drainer/mocks"
	hpmmocks "github.com/uber/peloton/pkg/hostmgr/hostpool/manager/mocks"
	hm_mocks "github.com/uber/peloton/pkg/hostmgr/mesos/mocks"
	mhttp_mocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/transport/mhttp/mocks"
	"github.com/uber/peloton/pkg/hostmgr/metrics"
	recovery_mocks "github.com/uber/peloton/pkg/hostmgr/mocks"
	"github.com/uber/peloton/pkg/hostmgr/offer"
	offer_mocks "github.com/uber/peloton/pkg/hostmgr/offer/mocks"
	hostcache_mocks "github.com/uber/peloton/pkg/hostmgr/p2k/hostcache/mocks"
	plugins_mocks "github.com/uber/peloton/pkg/hostmgr/p2k/plugins/mocks"
	reconciler_mocks "github.com/uber/peloton/pkg/hostmgr/reconcile/mocks"
	reserver_mocks "github.com/uber/peloton/pkg/hostmgr/reserver/mocks"
	watchmocks "github.com/uber/peloton/pkg/hostmgr/watchevent/mocks"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/api/transport"
)

const (
	_ID   = "test-id"
	_role = "test-role"

	_hostPort = "1.2.3.4:5"
)

var (
	errFoo = errors.New("test")
)

type ServerTestSuite struct {
	suite.Suite

	ctrl *gomock.Controller

	testScope tally.TestScope

	eventHandler      *offer_mocks.MockEventHandler
	backgroundManager *backgound_mocks.MockManager
	detector          *hm_mocks.MockMasterDetector
	mInbound          *mhttp_mocks.MockInbound
	recoveryHandler   *recovery_mocks.MockRecoveryHandler

	reconciler     *reconciler_mocks.MockTaskReconciler
	drainer        *drainer_mocks.MockDrainer
	reserver       *reserver_mocks.MockReserver
	watchProcessor *watchmocks.MockWatchProcessor

	plugin      *plugins_mocks.MockPlugin
	hostCache   *hostcache_mocks.MockHostCache
	mesosPlugin *plugins_mocks.MockPlugin

	hostPoolManager *hpmmocks.MockHostPoolManager

	server *Server
}

func (suite *ServerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.eventHandler = offer_mocks.NewMockEventHandler(suite.ctrl)
	suite.backgroundManager = backgound_mocks.NewMockManager(suite.ctrl)
	suite.detector = hm_mocks.NewMockMasterDetector(suite.ctrl)
	suite.mInbound = mhttp_mocks.NewMockInbound(suite.ctrl)
	suite.reconciler = reconciler_mocks.NewMockTaskReconciler(suite.ctrl)
	suite.recoveryHandler = recovery_mocks.NewMockRecoveryHandler(suite.ctrl)
	suite.drainer = drainer_mocks.NewMockDrainer(suite.ctrl)
	suite.reserver = reserver_mocks.NewMockReserver(suite.ctrl)
	suite.watchProcessor = watchmocks.NewMockWatchProcessor(suite.ctrl)
	suite.plugin = plugins_mocks.NewMockPlugin(suite.ctrl)
	suite.hostCache = hostcache_mocks.NewMockHostCache(suite.ctrl)
	suite.mesosPlugin = plugins_mocks.NewMockPlugin(suite.ctrl)
	suite.hostPoolManager = hpmmocks.NewMockHostPoolManager(suite.ctrl)

	suite.server = &Server{
		ID:   _ID,
		role: _role,

		getOfferEventHandler: func() offer.EventHandler {
			return suite.eventHandler
		},

		backgroundManager: suite.backgroundManager,

		mesosDetector:   suite.detector,
		mesosInbound:    suite.mInbound,
		recoveryHandler: suite.recoveryHandler,
		drainer:         suite.drainer,
		reserver:        suite.reserver,
		// Add outbound when we need it.

		reconciler: suite.reconciler,

		minBackoff: _minBackoff,
		maxBackoff: _maxBackoff,

		metrics:        metrics.NewMetrics(suite.testScope),
		watchProcessor: suite.watchProcessor,

		plugin:          suite.plugin,
		mesosManager:    suite.mesosPlugin,
		hostCache:       suite.hostCache,
		hostPoolManager: suite.hostPoolManager,
	}
	suite.server.Start()
}

func (suite *ServerTestSuite) TearDownTest() {
	log.Debug("tearing down")
	suite.server.Stop()
}

// Test new server creation
func (suite *ServerTestSuite) TestNewServer() {
	s := NewServer(
		suite.testScope,
		suite.backgroundManager,
		0,
		0,
		suite.detector,
		suite.mInbound,
		transport.Outbounds{},
		suite.reconciler,
		suite.recoveryHandler,
		suite.drainer,
		suite.reserver,
		suite.watchProcessor,
		suite.plugin,
		suite.hostCache,
		suite.mesosPlugin,
		suite.hostPoolManager,
	)
	suite.ctrl.Finish()
	suite.NotNil(s)
}

// Test gained leadership callback
func (suite *ServerTestSuite) TestGainedLeadershipCallback() {
	suite.mInbound.EXPECT().IsRunning().Return(true).AnyTimes()
	suite.server.GainedLeadershipCallback()
	suite.ctrl.Finish()
	suite.True(suite.server.elected.Load())
}

// Test gained leadership callback
func (suite *ServerTestSuite) TestLostLeadershipCallback() {
	suite.mInbound.EXPECT().IsRunning().Return(false).AnyTimes()
	suite.watchProcessor.EXPECT().StopEventClients()
	suite.server.handlersRunning.Store(false)
	suite.server.LostLeadershipCallback()
	suite.ctrl.Finish()
	suite.False(suite.server.elected.Load())
}

// Tests that if unelected and things are stopped, doing nothing.
func (suite *ServerTestSuite) TestUnelectedNoOp() {
	suite.server.elected.Store(false)
	suite.server.handlersRunning.Store(false)
	gomock.InOrder(
		suite.mInbound.EXPECT().IsRunning().Return(false).Times(2),
		suite.hostPoolManager.EXPECT().Stop(),
		suite.plugin.EXPECT().Stop(),
		suite.mesosPlugin.EXPECT().Stop(),
		suite.hostCache.EXPECT().Stop(),
	)
	suite.server.ensureStateRound()
	suite.ctrl.Finish()
	suite.Zero(suite.server.currentBackoffNano.Load())
	suite.False(suite.server.elected.Load())
	suite.False(suite.server.handlersRunning.Load())
}

// Tests that if unelected but seeing connection, calling stop on them.
func (suite *ServerTestSuite) TestUnelectedStopConnection() {
	suite.server.elected.Store(false)
	suite.server.handlersRunning.Store(false)

	gomock.InOrder(
		suite.mInbound.EXPECT().IsRunning().Return(true).AnyTimes(),
		suite.mInbound.EXPECT().Stop(),
		suite.mInbound.EXPECT().IsRunning().Return(false).AnyTimes(),
		suite.hostPoolManager.EXPECT().Stop(),
		suite.plugin.EXPECT().Stop(),
		suite.mesosPlugin.EXPECT().Stop(),
		suite.hostCache.EXPECT().Stop(),
	)

	suite.server.ensureStateRound()
	suite.ctrl.Finish()
	suite.Zero(suite.server.currentBackoffNano.Load())
	suite.False(suite.server.elected.Load())
	suite.False(suite.server.handlersRunning.Load())
}

// Test mesos detector host port error
func (suite *ServerTestSuite) TestMesosDetectorHostPortError() {
	suite.detector.EXPECT().HostPort().Return("")
	backoff := suite.server.reconnect(context.Background())
	suite.ctrl.Finish()
	suite.False(backoff)
}

// Tests that if unelected but seeing handlers running, calling stop on them.
func (suite *ServerTestSuite) TestUnelectedStopHandler() {
	suite.server.elected.Store(false)
	suite.server.handlersRunning.Store(true)

	gomock.InOrder(
		suite.mInbound.EXPECT().IsRunning().Return(false).AnyTimes(),
		suite.recoveryHandler.EXPECT().Stop(),
		suite.backgroundManager.EXPECT().Stop(),
		suite.eventHandler.EXPECT().Stop(),
		suite.drainer.EXPECT().Stop(),
		suite.reserver.EXPECT().Stop(),
		suite.hostPoolManager.EXPECT().Stop(),
		suite.plugin.EXPECT().Stop(),
		suite.mesosPlugin.EXPECT().Stop(),
		suite.hostCache.EXPECT().Stop(),
	)

	suite.server.ensureStateRound()
	suite.ctrl.Finish()
	suite.Zero(suite.server.currentBackoffNano.Load())
	suite.False(suite.server.elected.Load())
	suite.False(suite.server.handlersRunning.Load())
}

// Tests that if unelected but seeing handlers and connection, stop both.
func (suite *ServerTestSuite) TestUnelectedStopConnectionAndHandler() {
	suite.server.elected.Store(false)
	suite.server.handlersRunning.Store(true)

	gomock.InOrder(
		suite.mInbound.EXPECT().IsRunning().Return(true).AnyTimes(),
		suite.mInbound.EXPECT().Stop(),
		suite.recoveryHandler.EXPECT().Stop(),
		suite.backgroundManager.EXPECT().Stop(),
		suite.eventHandler.EXPECT().Stop(),
		suite.drainer.EXPECT().Stop(),
		suite.reserver.EXPECT().Stop(),
		suite.mInbound.EXPECT().IsRunning().Return(false).AnyTimes(),
		suite.hostPoolManager.EXPECT().Stop(),
		suite.plugin.EXPECT().Stop(),
		suite.mesosPlugin.EXPECT().Stop(),
		suite.hostCache.EXPECT().Stop(),
	)

	suite.server.ensureStateRound()
	suite.ctrl.Finish()
	suite.Zero(suite.server.currentBackoffNano.Load())
	suite.False(suite.server.elected.Load())
	suite.False(suite.server.handlersRunning.Load())
}

// Tests that if election and things are running, doing nothing.
func (suite *ServerTestSuite) TestElectedNoOp() {
	suite.server.elected.Store(true)
	suite.server.handlersRunning.Store(true)
	gomock.InOrder(
		suite.mInbound.EXPECT().IsRunning().Return(true).AnyTimes(),
		suite.hostCache.EXPECT().Start(),
		suite.plugin.EXPECT().Start(),
		suite.mesosPlugin.EXPECT().Start(),
		suite.hostPoolManager.EXPECT().Start(),
	)
	suite.server.ensureStateRound()
	suite.ctrl.Finish()
	suite.Zero(suite.server.currentBackoffNano.Load())
	suite.True(suite.server.elected.Load())
	suite.True(suite.server.handlersRunning.Load())
}

// Tests that if elected but seeing stopped connection, restart it.
func (suite *ServerTestSuite) TestElectedRestartConnection() {
	suite.server.elected.Store(true)
	suite.server.handlersRunning.Store(true)
	gomock.InOrder(
		// Initial check for Mesos connection.
		suite.mInbound.EXPECT().IsRunning().Return(false).AnyTimes(),

		// Stop handlers.
		suite.recoveryHandler.EXPECT().Stop(),
		suite.backgroundManager.EXPECT().Stop(),
		suite.eventHandler.EXPECT().Stop(),
		suite.drainer.EXPECT().Stop(),
		suite.reserver.EXPECT().Stop(),

		// Detect leader and start loop successfully.
		suite.detector.EXPECT().HostPort().Return(_hostPort),
		suite.mInbound.
			EXPECT().
			StartMesosLoop(context.Background(), gomock.Eq(_hostPort)).
			Return(nil, nil),

		// Connected, now start handlers.
		suite.mInbound.EXPECT().IsRunning().Return(true),
		suite.hostCache.EXPECT().Start(),
		suite.plugin.EXPECT().Start(),
		suite.mesosPlugin.EXPECT().Start(),
		suite.hostPoolManager.EXPECT().Start(),

		// Triggers Explicit Reconciliation on Mesos Master re-election
		suite.recoveryHandler.EXPECT().Start(),
		suite.reconciler.EXPECT().SetExplicitReconcileTurn(true).Times(1),
		suite.backgroundManager.EXPECT().Start(),
		suite.eventHandler.EXPECT().Start(),
		suite.drainer.EXPECT().Start(),
		suite.reserver.EXPECT().Start(),
	)
	suite.server.ensureStateRound()
	suite.ctrl.Finish()
	suite.Zero(suite.server.currentBackoffNano.Load())
	suite.True(suite.server.elected.Load())
	suite.True(suite.server.handlersRunning.Load())
}

// Tests that if elected but seeing stopped handlers, restart.
func (suite *ServerTestSuite) TestElectedRestartHandlers() {
	suite.server.elected.Store(true)
	suite.server.handlersRunning.Store(false)

	gomock.InOrder(
		suite.mInbound.EXPECT().IsRunning().Return(true).Times(3),
		suite.hostCache.EXPECT().Start(),
		suite.plugin.EXPECT().Start(),
		suite.mesosPlugin.EXPECT().Start(),
		suite.hostPoolManager.EXPECT().Start(),
		suite.recoveryHandler.EXPECT().Start(),
		suite.reconciler.EXPECT().SetExplicitReconcileTurn(true).Times(1),
		suite.backgroundManager.EXPECT().Start(),
		suite.eventHandler.EXPECT().Start(),
		suite.drainer.EXPECT().Start(),
		suite.reserver.EXPECT().Start(),
	)

	suite.server.ensureStateRound()
	suite.ctrl.Finish()
	suite.Zero(suite.server.currentBackoffNano.Load())
	suite.True(suite.server.elected.Load())
	suite.True(suite.server.handlersRunning.Load())
}

// Tests that if elected but seeing stopped handlers and connection,
// restart both.
func (suite *ServerTestSuite) TestElectedRestartConnectionAndHandler() {
	suite.server.elected.Store(true)
	suite.server.handlersRunning.Store(false)

	gomock.InOrder(
		// Initial check for Mesos connection.
		suite.mInbound.EXPECT().IsRunning().Return(false).AnyTimes(),

		// Detect leader and start loop successfully.
		suite.detector.EXPECT().HostPort().Return(_hostPort),
		suite.mInbound.
			EXPECT().
			StartMesosLoop(context.Background(), gomock.Eq(_hostPort)).
			Return(nil, nil),

		// Connected, now start handlers.
		suite.mInbound.EXPECT().IsRunning().Return(true),
		suite.hostCache.EXPECT().Start(),
		suite.plugin.EXPECT().Start(),
		suite.mesosPlugin.EXPECT().Start(),
		suite.hostPoolManager.EXPECT().Start(),

		// Triggers Explicit Reconciliation on re-election of host manager.
		suite.recoveryHandler.EXPECT().Start(),
		suite.reconciler.EXPECT().SetExplicitReconcileTurn(true).Times(1),
		suite.backgroundManager.EXPECT().Start(),
		suite.eventHandler.EXPECT().Start(),
		suite.drainer.EXPECT().Start(),
		suite.reserver.EXPECT().Start(),
	)

	suite.server.ensureStateRound()
	suite.ctrl.Finish()
	suite.Zero(suite.server.currentBackoffNano.Load())
	suite.True(suite.server.elected.Load())
	suite.True(suite.server.handlersRunning.Load())
}

// Tests that Mesos connection failure triggers a backoff.
func (suite *ServerTestSuite) TestBackoffOnMesosConnectFailure() {
	suite.server.elected.Store(true)
	suite.server.handlersRunning.Store(false)

	lower := time.Now()
	upper := lower.Add(suite.server.minBackoff * 2)

	// For stats gathering.
	suite.mInbound.
		EXPECT().
		IsRunning().
		Return(false).
		Times(2)

	// Initial check for Mesos connection.
	suite.mInbound.
		EXPECT().
		IsRunning().
		Return(false)

	// Detector returns a real host.
	suite.detector.
		EXPECT().
		HostPort().
		Return(_hostPort)

	// StartMesosLoop returns an error.
	suite.mInbound.
		EXPECT().
		StartMesosLoop(context.Background(), gomock.Eq(_hostPort)).
		Return(nil, errFoo)

	suite.server.ensureStateRound()
	suite.ctrl.Finish()
	suite.Equal(
		suite.server.minBackoff.Nanoseconds(),
		suite.server.currentBackoffNano.Load())
	suite.True(lower.UnixNano() < suite.server.backoffUntilNano.Load())
	suite.True(upper.UnixNano() > suite.server.backoffUntilNano.Load())
}

// Tests that backoff doubles on another failure.
func (suite *ServerTestSuite) TestDoubleBackoff() {
	now := time.Now()
	lower := now.Add(suite.server.minBackoff * 2)
	upper := lower.Add(suite.server.minBackoff * 2)

	suite.server.elected.Store(true)
	suite.server.handlersRunning.Store(false)
	suite.server.currentBackoffNano.Store(
		suite.server.minBackoff.Nanoseconds())
	suite.server.backoffUntilNano.Store(now.UnixNano())

	// Initial check for Mesos connection.
	suite.mInbound.
		EXPECT().
		IsRunning().
		Return(false)

	// Detector returns a real host.
	suite.detector.
		EXPECT().
		HostPort().
		Return(_hostPort)

	// StartMesosLoop returns an error.
	suite.mInbound.
		EXPECT().
		StartMesosLoop(context.Background(), gomock.Eq(_hostPort)).
		Return(nil, errFoo)

	// For stats gathering.
	suite.mInbound.
		EXPECT().
		IsRunning().
		Return(false).
		Times(2)
	suite.server.ensureStateRound()
	suite.ctrl.Finish()
	suite.Equal(
		suite.server.minBackoff.Nanoseconds()*2,
		suite.server.currentBackoffNano.Load())
	suite.True(lower.UnixNano() < suite.server.backoffUntilNano.Load())
	suite.True(upper.UnixNano() > suite.server.backoffUntilNano.Load())
}

// Tests that backoff caps at maximum.
func (suite *ServerTestSuite) TestMaxBackoff() {
	now := time.Now()
	lower := now.Add(suite.server.maxBackoff)
	upper := lower.Add(suite.server.minBackoff)

	suite.server.elected.Store(true)
	suite.server.handlersRunning.Store(false)
	suite.server.currentBackoffNano.Store(
		suite.server.maxBackoff.Nanoseconds() - 1)
	suite.server.backoffUntilNano.Store(now.UnixNano())

	// Initial check for Mesos connection.
	suite.mInbound.
		EXPECT().
		IsRunning().
		Return(false)

	// Detector returns a real host.
	suite.detector.
		EXPECT().
		HostPort().
		Return(_hostPort)

	// StartMesosLoop returns an error.
	suite.mInbound.
		EXPECT().
		StartMesosLoop(context.Background(), gomock.Eq(_hostPort)).
		Return(nil, errFoo)

	// For stats gathering.
	suite.mInbound.
		EXPECT().
		IsRunning().
		Return(false).
		Times(2)

	suite.server.ensureStateRound()
	suite.ctrl.Finish()
	suite.Equal(
		suite.server.maxBackoff.Nanoseconds(),
		suite.server.currentBackoffNano.Load())
	suite.True(lower.UnixNano() < suite.server.backoffUntilNano.Load())
	suite.True(upper.UnixNano() > suite.server.backoffUntilNano.Load())
}

// Tests that we do not perform connection withinn backoff window.
func (suite *ServerTestSuite) TestEffectiveBackoff() {
	now := time.Now()
	future := now.Add(suite.server.minBackoff)

	suite.server.elected.Store(true)
	suite.server.handlersRunning.Store(false)
	suite.server.currentBackoffNano.Store(
		suite.server.minBackoff.Nanoseconds())
	suite.server.backoffUntilNano.Store(future.UnixNano())

	gomock.InOrder(
		// Initial check for Mesos connection.
		suite.mInbound.
			EXPECT().
			IsRunning().
			Return(false),

		// For stats gathering.
		suite.mInbound.
			EXPECT().
			IsRunning().
			Return(false).
			Times(2),
	)
	suite.server.ensureStateRound()
	suite.ctrl.Finish()
	suite.Equal(
		suite.server.minBackoff.Nanoseconds(),
		suite.server.currentBackoffNano.Load())
	suite.Equal(future.UnixNano(), suite.server.backoffUntilNano.Load())
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}
