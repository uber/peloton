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

package hostmover

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	host "github.com/uber/peloton/.gen/peloton/api/v0/host"
	hpb "github.com/uber/peloton/.gen/peloton/api/v0/host"
	hostsvc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"
	hostmocks "github.com/uber/peloton/.gen/peloton/api/v0/host/svc/mocks"

	"github.com/uber/peloton/pkg/common/async"
	"github.com/uber/peloton/pkg/common/backoff"
	"github.com/uber/peloton/pkg/common/queue"
	"github.com/uber/peloton/pkg/common/queue/mocks"
	"github.com/uber/peloton/pkg/hostmgr/hostpool"
	poolmocks "github.com/uber/peloton/pkg/hostmgr/hostpool/manager/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

const (
	SourcePool      = "srcPool"
	DestPool        = "destPool"
	_hostIdTemplate = "hostName%d"
)

// HostPoolTestSuite is test suite for host pool.
type hostMoverTestSuite struct {
	suite.Suite
	mover               *HostMover
	mockCtrl            *gomock.Controller
	mockHostMgr         *hostmocks.MockHostServiceYARPCClient
	mockHostPoolManager *poolmocks.MockHostPoolManager
	mockQueue           *mocks.MockQueue
}

func (suite *hostMoverTestSuite) SetupSuite() {
}

func (suite *hostMoverTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockHostMgr = hostmocks.NewMockHostServiceYARPCClient(suite.mockCtrl)
	suite.mockHostPoolManager = poolmocks.NewMockHostPoolManager(suite.mockCtrl)
	suite.mockQueue = mocks.NewMockQueue(suite.mockCtrl)
}

// TestHostMoverTestSuite runs HostMoverTestSuite.
func TestHostMoverTestSuite(t *testing.T) {
	suite.Run(t, new(hostMoverTestSuite))
}

// TestMoveHostsAcrossPoolsSuccessPath tests host mover in success path
func (suite *hostMoverTestSuite) TestMoveHostsAcrossPoolsSuccessPath() {
	// Create custom host-mover object
	moverObject := NewHostMover(suite.mockHostMgr,
		suite.mockHostPoolManager,
		tally.NewTestScope("", map[string]string{}))

	suite.mover = &moverObject
	moverObject.Start()
	resp := &hostsvc.StartMaintenanceResponse{}

	suite.mockHostMgr.EXPECT().
		StartMaintenance(gomock.Any(), gomock.Any()).
		Return(resp, nil).
		AnyTimes()

	hostInfoList := make([]*host.HostInfo, 2)

	for i := 0; i < 2; i++ {
		hostInfo := host.HostInfo{Hostname: fmt.Sprintf(_hostIdTemplate, i),
			State: hpb.HostState_HOST_STATE_DOWN}
		hostInfoList[i] = &hostInfo
	}

	tt := struct {
		resp *hostsvc.QueryHostsResponse
		err  error
	}{
		&hostsvc.QueryHostsResponse{
			HostInfos: hostInfoList,
		},
		nil,
	}

	suite.mockHostMgr.EXPECT().
		QueryHosts(gomock.Any(), gomock.Any()).
		Return(tt.resp, tt.err).
		AnyTimes()

	suite.mockHostMgr.EXPECT().
		CompleteMaintenance(gomock.Any(), gomock.Any()).
		Return(nil, nil).
		AnyTimes()

	suite.mockHostPoolManager.EXPECT().
		GetPool(SourcePool).
		Return(nil, nil).AnyTimes()

	suite.mockHostPoolManager.EXPECT().
		GetPool(DestPool).
		Return(nil, nil).AnyTimes()

	pool := hostpool.New(SourcePool, tally.NewTestScope("",
		map[string]string{}))

	for i := 0; i < 2; i++ {
		suite.mockHostPoolManager.EXPECT().
			GetPoolByHostname(fmt.Sprintf(_hostIdTemplate, i)).
			Return(pool, nil)
	}

	id, _ := moverObject.MoveHostsAcrossPools(context.Background(),
		2,
		SourcePool,
		DestPool,
	)

	time.Sleep(30 * time.Second)

	// request doesn't exist
	blob := moverObject.QueryRequestStatus(context.Background(), id+"_new")
	// Both the hosts are in DOWN state. Request should succeed
	suite.Equal(blob.requestStatus, requestNotFound)

	// request exist
	blob = moverObject.QueryRequestStatus(context.Background(), id)
	// Both the hosts are in DOWN state. Request should succeed
	suite.EqualValues(blob.numHostsMoved, 2)
	suite.Equal(blob.requestStatus, requestSucceeded)
	moverObject.Stop()
}

// TestMoveHostsAcrossPoolsSuccessPath tests host mover in the failure path
func (suite *hostMoverTestSuite) TestMoveHostsAcrossPoolsFailurePath() {
	// Create custom host-mover object
	moverObject := &hostMover{
		requestsIdToStatusMapping: make(map[RequestId]*MoveStatus),
		queuedRequests: queue.NewQueue("move-request-queue",
			reflect.TypeOf(InFlightRequest{}), 10),
		reconcileQueue: queue.NewQueue("reconcile-request-queue",
			reflect.TypeOf(""), 0),
		hostPoolManager: suite.mockHostPoolManager,
		hostClient:      suite.mockHostMgr,
		numWorkers:      1,
		retryPolicy:     backoff.NewRetryPolicy(MaxRetryAttempts, 1),
		metrics:         NewMetrics(tally.NewTestScope("", map[string]string{})),
		shutDown:        make(chan struct{}),
	}

	moverObject.daemon = async.NewDaemon("HostMover", moverObject)
	moverObject.Start()

	resp := &hostsvc.StartMaintenanceResponse{}

	suite.mockHostPoolManager.EXPECT().
		GetPool(SourcePool).
		Return(nil, nil).
		AnyTimes()

	suite.mockHostPoolManager.EXPECT().
		GetPool(DestPool).
		Return(nil, nil).
		AnyTimes()

	pool := hostpool.New(SourcePool, tally.NewTestScope("",
		map[string]string{}))

	for j := 0; j < 3; j++ {
		for i := 0; i < 2; i++ {
			suite.mockHostPoolManager.EXPECT().
				GetPoolByHostname(fmt.Sprintf(_hostIdTemplate, i)).
				Return(pool, nil)
		}
	}

	// Start maintenance error
	suite.mockHostMgr.EXPECT().
		StartMaintenance(gomock.Any(), gomock.Any()).
		Return(resp, errors.New("maintenance error")).
		Times(6)

	id, _ := moverObject.MoveHostsAcrossPools(context.Background(),
		2,
		SourcePool,
		DestPool,
	)

	time.Sleep(30 * time.Second)

	blob := moverObject.QueryRequestStatus(context.Background(), id)
	suite.EqualValues(blob.numHostsMoved, 0)

	// Complete maintenance error
	hostInfoList := make([]*host.HostInfo, 2)
	for i := 0; i < 2; i++ {
		hostInfo := host.HostInfo{Hostname: fmt.Sprintf(_hostIdTemplate, i),
			State: hpb.HostState_HOST_STATE_DOWN}
		hostInfoList[i] = &hostInfo
	}

	tt := struct {
		resp *hostsvc.QueryHostsResponse
		err  error
	}{
		&hostsvc.QueryHostsResponse{
			HostInfos: hostInfoList,
		},
		nil,
	}

	suite.mockHostMgr.EXPECT().
		StartMaintenance(gomock.Any(), gomock.Any()).
		Return(resp, nil).
		Times(2)

	suite.mockHostMgr.EXPECT().
		QueryHosts(gomock.Any(), gomock.Any()).
		Return(tt.resp, tt.err).
		Times(2)

	// Complete maintenance is error
	suite.mockHostMgr.EXPECT().
		CompleteMaintenance(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("maintenance error")).
		Times(6)

	id, _ = moverObject.MoveHostsAcrossPools(context.Background(),
		2,
		SourcePool,
		DestPool,
	)

	time.Sleep(30 * time.Second)

	blob = moverObject.QueryRequestStatus(context.Background(), id)
	suite.EqualValues(blob.numHostsMoved, 0)

	// Only one of the host is down
	hostInfoList = make([]*host.HostInfo, 1)
	for i := 0; i < 1; i++ {
		hostInfo := host.HostInfo{Hostname: fmt.Sprintf(_hostIdTemplate, i),
			State: hpb.HostState_HOST_STATE_DOWN}
		hostInfoList[i] = &hostInfo
	}

	tt = struct {
		resp *hostsvc.QueryHostsResponse
		err  error
	}{
		&hostsvc.QueryHostsResponse{
			HostInfos: hostInfoList,
		},
		nil,
	}

	resp = &hostsvc.StartMaintenanceResponse{}

	suite.mockHostMgr.EXPECT().
		StartMaintenance(gomock.Any(), gomock.Any()).
		Return(resp, nil).
		Times(6)

	suite.mockHostMgr.EXPECT().
		QueryHosts(gomock.Any(), gomock.Any()).
		Return(tt.resp, tt.err).
		AnyTimes()

	suite.mockHostMgr.EXPECT().
		CompleteMaintenance(gomock.Any(), gomock.Any()).
		Return(nil, nil).
		Times(6)

	id, _ = moverObject.MoveHostsAcrossPools(context.Background(),
		2,
		SourcePool,
		DestPool,
	)

	time.Sleep(30 * time.Second)

	blob = moverObject.QueryRequestStatus(context.Background(), id)

	// Host1 is still in draining mode.
	suite.EqualValues(blob.numHostsMoved, 1)

	moverObject.Stop()
}

// TestReconcile tests reconciliation in the complete maintenance path.
func (suite *hostMoverTestSuite) TestReconcileCompleteMaintenance() {
	// Create custom host-mover object
	moverObject := &hostMover{
		requestsIdToStatusMapping: make(map[RequestId]*MoveStatus),
		queuedRequests: queue.NewQueue("move-request-queue",
			reflect.TypeOf(InFlightRequest{}), 10),
		reconcileQueue: queue.NewQueue("reconcile-request-queue",
			reflect.TypeOf(""), 1),
		hostPoolManager:             suite.mockHostPoolManager,
		hostClient:                  suite.mockHostMgr,
		numWorkers:                  1,
		totalWaitForSingleHostDrain: 1 * time.Second,
		retryPolicy: backoff.NewRetryPolicy(MaxRetryAttempts,
			1*time.Second),
		metrics:  NewMetrics(tally.NewTestScope("", map[string]string{})),
		shutDown: make(chan struct{}),
	}

	moverObject.daemon = async.NewDaemon("HostMover", moverObject)
	hostInfoList := make([]*host.HostInfo, 1)
	resp := &hostsvc.StartMaintenanceResponse{}

	suite.mockHostMgr.EXPECT().
		StartMaintenance(gomock.Any(), gomock.Any()).
		Return(resp, nil).
		AnyTimes()

	for i := 0; i < 1; i++ {
		hostInfo := host.HostInfo{Hostname: fmt.Sprintf(_hostIdTemplate, i),
			State: hpb.HostState_HOST_STATE_DOWN}
		hostInfoList[i] = &hostInfo
	}

	tt := struct {
		resp *hostsvc.QueryHostsResponse
		err  error
	}{
		&hostsvc.QueryHostsResponse{
			HostInfos: hostInfoList,
		},
		nil,
	}

	suite.mockHostPoolManager.EXPECT().
		GetPool(SourcePool).
		Return(nil, nil).
		AnyTimes()

	suite.mockHostPoolManager.EXPECT().
		GetPool(DestPool).
		Return(nil, nil).
		AnyTimes()

	pool := hostpool.New(SourcePool, tally.NewTestScope("",
		map[string]string{}))
	for j := 0; j < 2; j++ {
		for i := 0; i < 1; i++ {
			suite.mockHostPoolManager.EXPECT().
				GetPoolByHostname(fmt.Sprintf(_hostIdTemplate, i)).
				Return(pool, nil)
		}
	}

	suite.mockHostMgr.EXPECT().
		QueryHosts(gomock.Any(), gomock.Any()).
		Return(tt.resp, tt.err).
		AnyTimes()

	// Trigger moving hosts to the reconcile queue
	suite.mockHostMgr.EXPECT().
		CompleteMaintenance(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("error in maintenance")).
		Times(3)

	// Success in reconcile code path
	suite.mockHostMgr.EXPECT().
		CompleteMaintenance(gomock.Any(), gomock.Any()).
		Return(nil, nil).
		Times(1)

	// Start the daemon
	moverObject.Start()

	// Host move request
	_, _ = moverObject.MoveHostsAcrossPools(context.Background(),
		1,
		SourcePool,
		DestPool,
	)

	time.Sleep(30 * time.Second)

	// No pending host in the reconcile queue by now
	suite.EqualValues(moverObject.reconcileQueue.Length(), 0)

	// Trigger moving hosts to the reconcile queue
	suite.mockHostMgr.EXPECT().
		CompleteMaintenance(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("maintenance error")).
		AnyTimes()

	// Host move request
	_, _ = moverObject.MoveHostsAcrossPools(context.Background(),
		1,
		SourcePool,
		DestPool,
	)

	time.Sleep(30 * time.Second)

	// Pending host in the reconcile queue due to error in complete maintenance
	suite.EqualValues(moverObject.reconcileQueue.Length(), 1)

	moverObject.Stop()
}

// TestReconcile tests reconciliation in the enqueue/dequeue path.
func (suite *hostMoverTestSuite) TestReconcileQueueTest() {
	// Create custom host-mover object
	moverObject := &hostMover{
		requestsIdToStatusMapping: make(map[RequestId]*MoveStatus),
		queuedRequests: queue.NewQueue("move-request-queue",
			reflect.TypeOf(InFlightRequest{}), 10),
		reconcileQueue:              suite.mockQueue,
		hostPoolManager:             suite.mockHostPoolManager,
		hostClient:                  suite.mockHostMgr,
		numWorkers:                  1,
		totalWaitForSingleHostDrain: 1 * time.Second,
		retryPolicy:                 backoff.NewRetryPolicy(MaxRetryAttempts, 1),
		metrics:                     NewMetrics(tally.NewTestScope("", map[string]string{})),
		shutDown:                    make(chan struct{}),
	}

	resp := &hostsvc.StartMaintenanceResponse{}

	suite.mockHostMgr.EXPECT().
		StartMaintenance(gomock.Any(), gomock.Any()).
		Return(resp, nil).
		AnyTimes()

	hostInfoList := make([]*host.HostInfo, 1)
	for i := 0; i < 1; i++ {
		hostInfo := host.HostInfo{Hostname: fmt.Sprintf(_hostIdTemplate, i),
			State: hpb.HostState_HOST_STATE_DOWN}
		hostInfoList[i] = &hostInfo
	}

	tt := struct {
		resp *hostsvc.QueryHostsResponse
		err  error
	}{
		&hostsvc.QueryHostsResponse{
			HostInfos: hostInfoList,
		},
		nil,
	}

	suite.mockHostPoolManager.EXPECT().
		GetPool(SourcePool).
		Return(nil, nil).
		AnyTimes()

	suite.mockHostPoolManager.EXPECT().
		GetPool(DestPool).
		Return(nil, nil).
		AnyTimes()

	pool := hostpool.New(SourcePool, tally.NewTestScope("",
		map[string]string{}))
	suite.mockHostPoolManager.EXPECT().
		GetPoolByHostname(fmt.Sprintf(_hostIdTemplate, 0)).
		Return(pool, nil)

	suite.mockHostMgr.EXPECT().
		QueryHosts(gomock.Any(), gomock.Any()).
		Return(tt.resp, tt.err).
		AnyTimes()

	// Trigger moving hosts to the reconcile queue
	suite.mockHostMgr.EXPECT().
		CompleteMaintenance(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("error in maintenance")).
		Times(3)

	// Success in reconcile path
	suite.mockHostMgr.EXPECT().
		CompleteMaintenance(gomock.Any(), gomock.Any()).
		Return(nil, nil)

	suite.mockQueue.EXPECT().
		Enqueue(gomock.Any()).
		Return(nil).
		AnyTimes()

	// Trigger error in dequeue
	suite.mockQueue.EXPECT().
		Dequeue(MaxRequestDequeueWaitTime).
		Return(nil, errors.New("dequeue error")).
		AnyTimes()

	suite.mockQueue.EXPECT().
		Length().
		Return(1).
		AnyTimes()

	// New daemon and start
	moverObject.daemon = async.NewDaemon("HostMover", moverObject)
	moverObject.Start()

	// Host move request
	id, _ := moverObject.MoveHostsAcrossPools(context.Background(),
		1,
		SourcePool,
		DestPool,
	)

	time.Sleep(30 * time.Second)

	// Pending host in the reconcile queue due to error in dequeue path
	blob := moverObject.QueryRequestStatus(context.Background(), id)

	// Host1 is still in draining mode. Request should be in progress
	suite.EqualValues(blob.numHostsMoved, 0)
	moverObject.Stop()
}

// TestValidations tests different validation paths.
func (suite *hostMoverTestSuite) TestValidations() {
	// Create custom host-mover object
	moverObject := &hostMover{
		requestsIdToStatusMapping: make(map[RequestId]*MoveStatus),
		hostPoolManager:           suite.mockHostPoolManager,
		queuedRequests: queue.NewQueue("move-request-queue",
			reflect.TypeOf(InFlightRequest{}), MaxRequestQueueSize),
		metrics: NewMetrics(tally.NewTestScope("",
			map[string]string{})),
	}

	suite.mockHostPoolManager.EXPECT().
		GetPool("NoPool").
		Return(nil, errors.New("pool not exists")).
		AnyTimes()

	suite.mockHostPoolManager.EXPECT().
		GetPool(SourcePool).
		Return(nil, nil).
		AnyTimes()

	suite.mockHostPoolManager.EXPECT().
		GetPool(DestPool).
		Return(nil, nil).
		AnyTimes()

	_, err := moverObject.MoveHostsAcrossPools(context.Background(),
		2,
		"NoPool",
		DestPool,
	)

	// "No source pool exists"
	suite.EqualValues(err, errSourcePoolNotExists)

	_, err = moverObject.MoveHostsAcrossPools(context.Background(),
		2, SourcePool,
		"NoPool",
	)

	// "No dest pool exists"
	suite.EqualValues(err, errDestPoolNotExists)

	pool := hostpool.New("DiffPool", tally.NewTestScope("",
		map[string]string{}))
	for i := 0; i < 2; i++ {
		suite.mockHostPoolManager.EXPECT().
			GetPoolByHostname(fmt.Sprintf(_hostIdTemplate, i)).
			Return(pool, nil)
	}

	id, _ := moverObject.MoveHostsAcrossPools(context.Background(),
		2,
		SourcePool,
		DestPool,
	)

	time.Sleep(30 * time.Second)

	blob := moverObject.QueryRequestStatus(context.Background(), id)
	// No host will be moved as the actualPool of the host has been changed
	suite.EqualValues(blob.numHostsMoved, 0)

	pool = hostpool.New(SourcePool, tally.NewTestScope("",
		map[string]string{}))
	for i := 0; i < 2; i++ {
		suite.mockHostPoolManager.EXPECT().
			GetPoolByHostname(fmt.Sprintf(_hostIdTemplate, i)).
			Return(nil, errors.New("random error"))
	}

	id, _ = moverObject.MoveHostsAcrossPools(context.Background(), 2,
		SourcePool,
		DestPool,
	)

	time.Sleep(30 * time.Second)
	blob = moverObject.QueryRequestStatus(context.Background(), id)
	// No host will be moved as the GetPoolByHostname returned error
	suite.EqualValues(blob.numHostsMoved, 0)
}

// TestQueueErrors tests enqueue errors
func (suite *hostMoverTestSuite) TestQueueErrors() {

	// Create custom host-mover object
	moverObject := &hostMover{
		requestsIdToStatusMapping: make(map[RequestId]*MoveStatus),
		hostPoolManager:           suite.mockHostPoolManager,
		queuedRequests:            suite.mockQueue,
		metrics: NewMetrics(tally.NewTestScope("",
			map[string]string{})),
		retryPolicy: backoff.NewRetryPolicy(MaxRetryAttempts, 1),
		numWorkers:  1,
	}

	suite.mockHostPoolManager.EXPECT().
		GetPool(SourcePool).
		Return(nil, nil).
		AnyTimes()

	suite.mockHostPoolManager.EXPECT().
		GetPool(DestPool).
		Return(nil, nil).
		AnyTimes()

	suite.mockQueue.EXPECT().
		Enqueue(gomock.Any()).
		Return(errors.New("mQueue error")).
		AnyTimes()

	_, err := moverObject.MoveHostsAcrossPools(context.Background(),
		2,
		SourcePool,
		DestPool,
	)

	suite.EqualValues(err, errEnqueuing)
}
