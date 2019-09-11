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
	"net/http"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	pb_eventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	"github.com/uber/peloton/.gen/peloton/private/models"
	res_mocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"
	hostcache_mocks "github.com/uber/peloton/pkg/hostmgr/p2k/hostcache/mocks"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/background"
	"github.com/uber/peloton/pkg/common/eventstream"
	"github.com/uber/peloton/pkg/common/rpc"
	"github.com/uber/peloton/pkg/hostmgr/binpacking"
	"github.com/uber/peloton/pkg/hostmgr/config"
	"github.com/uber/peloton/pkg/hostmgr/hostpool/manager"
	hostmgr_mesos "github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	mpb_mocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	"github.com/uber/peloton/pkg/hostmgr/metrics"
	"github.com/uber/peloton/pkg/hostmgr/offer"
	watchmocks "github.com/uber/peloton/pkg/hostmgr/watchevent/mocks"
	storage_mocks "github.com/uber/peloton/pkg/storage/mocks"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
	ormStore "github.com/uber/peloton/pkg/storage/objects"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
)

const (
	_encoding      = mpb.ContentTypeJSON
	_zkPath        = "zkpath"
	_frameworkName = "framework-name"
)

type RecoveryTestSuite struct {
	suite.Suite
	mockCtrl        *gomock.Controller
	recoveryHandler RecoveryHandler
	mockTaskStore   *store_mocks.MockTaskStore
	activeJobsOps   *objectmocks.MockActiveJobsOps
	jobConfigOps    *objectmocks.MockJobConfigOps
	jobRuntimeOps   *objectmocks.MockJobRuntimeOps
	hostInfoOps     *objectmocks.MockHostInfoOps

	resMgrClient *res_mocks.MockResourceManagerServiceYARPCClient

	dispatcher *yarpc.Dispatcher
	testScope  tally.TestScope

	taskStatusUpdate *sched.Event
	event            *pb_eventstream.Event

	store              *storage_mocks.MockFrameworkInfoStore
	driver             hostmgr_mesos.SchedulerDriver
	defragRanker       binpacking.Ranker
	schedulerClient    *mpb_mocks.MockSchedulerClient
	watchProcessor     *watchmocks.MockWatchProcessor
	manager            manager.HostPoolManager
	eventStreamHandler *eventstream.Handler
	hostcache          *hostcache_mocks.MockHostCache
}

func (suite *RecoveryTestSuite) SetupTest() {
	log.Info("setting up test")
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockTaskStore = store_mocks.NewMockTaskStore(suite.mockCtrl)
	suite.activeJobsOps = objectmocks.NewMockActiveJobsOps(suite.mockCtrl)
	suite.jobConfigOps = objectmocks.NewMockJobConfigOps(suite.mockCtrl)
	suite.jobRuntimeOps = objectmocks.NewMockJobRuntimeOps(suite.mockCtrl)
	suite.hostInfoOps = objectmocks.NewMockHostInfoOps(suite.mockCtrl)

	suite.hostcache = hostcache_mocks.NewMockHostCache(suite.mockCtrl)
	suite.recoveryHandler = NewRecoveryHandler(
		tally.NoopScope,
		suite.mockTaskStore,
		&ormStore.Store{},
		suite.hostcache,
	)

	t := rpc.NewTransport()
	outbound := t.NewOutbound(nil)
	outbounds := yarpc.Outbounds{
		common.PelotonResourceManager: transport.Outbounds{
			Unary: outbound,
		},
	}
	suite.dispatcher = yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonHostManager,
		Inbounds:  nil,
		Outbounds: outbounds,
		Metrics: yarpc.MetricsConfig{
			Tally: tally.NoopScope,
		},
	})
	suite.schedulerClient = mpb_mocks.NewMockSchedulerClient(suite.mockCtrl)
	suite.resMgrClient = res_mocks.NewMockResourceManagerServiceYARPCClient(suite.mockCtrl)
	suite.watchProcessor = watchmocks.NewMockWatchProcessor(suite.mockCtrl)
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.store = storage_mocks.NewMockFrameworkInfoStore(suite.mockCtrl)
	suite.defragRanker = binpacking.GetRankerByName(binpacking.FirstFit)
	suite.eventStreamHandler = eventstream.NewEventStreamHandler(
		10,
		[]string{"client1"},
		nil,
		suite.testScope)
	suite.manager = manager.New(
		0, suite.eventStreamHandler, suite.hostInfoOps, tally.NoopScope)
	suite.driver = hostmgr_mesos.InitSchedulerDriver(
		&hostmgr_mesos.Config{
			Framework: &hostmgr_mesos.FrameworkConfig{
				Name:                        _frameworkName,
				GPUSupported:                true,
				TaskKillingStateSupported:   false,
				PartitionAwareSupported:     false,
				RevocableResourcesSupported: false,
			},
			ZkPath:   _zkPath,
			Encoding: _encoding,
		},
		suite.store,
		http.Header{},
	).(hostmgr_mesos.SchedulerDriver)

	hmConfig := config.Config{
		OfferHoldTimeSec:              60,
		OfferPruningPeriodSec:         60,
		HostPlacingOfferStatusTimeout: 1 * time.Minute,
		HostPruningPeriodSec:          1 * time.Minute,
		HeldHostPruningPeriodSec:      1 * time.Minute,
		BinPackingRefreshIntervalSec:  1 * time.Minute,
		TaskUpdateBufferSize:          10,
		TaskUpdateAckConcurrency:      0,
	}
	offer.InitEventHandler(
		suite.dispatcher,
		suite.testScope,
		suite.schedulerClient,
		suite.resMgrClient,
		background.NewManager(),
		suite.defragRanker,
		hmConfig,
		suite.watchProcessor,
		suite.manager,
		nil,
	)

	suite.recoveryHandler = &recoveryHandler{
		metrics:       metrics.NewMetrics(tally.NoopScope),
		recoveryScope: tally.NoopScope,

		taskStore:     suite.mockTaskStore,
		activeJobsOps: suite.activeJobsOps,
		jobConfigOps:  suite.jobConfigOps,
		jobRuntimeOps: suite.jobRuntimeOps,
		hostCache:     suite.hostcache,
	}
}

func (suite *RecoveryTestSuite) TearDownTest() {
	log.Info("tearing down test")
	suite.mockCtrl.Finish()
}

func TestHostmgrRecovery(t *testing.T) {
	suite.Run(t, new(RecoveryTestSuite))
}

func (suite *RecoveryTestSuite) TestStart() {
	hostname := "dummy_host"
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	taskID := "3851f4c0-a333-4f17-9438-d2f43fe9449e-1-1"
	mesosTaskID := mesos.TaskID{Value: &taskID}
	jobConfig := &job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 1,
	}

	// Do Recovery for Active Jobs
	suite.activeJobsOps.EXPECT().
		GetAll(gomock.Any()).
		Return([]*peloton.JobID{jobID}, nil)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), jobID).
		Return(&job.RuntimeInfo{
			State:     job.JobState_RUNNING,
			GoalState: job.JobState_SUCCEEDED,
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), jobID, gomock.Any()).
		Return(jobConfig, &models.ConfigAddOn{}, nil)

	suite.mockTaskStore.EXPECT().
		GetTasksForJobByRange(gomock.Any(), jobID, gomock.Any()).
		Return(map[uint32]*task.TaskInfo{
			uint32(0): {
				Runtime: &task.RuntimeInfo{
					Host:                 hostname,
					MesosTaskId:          &mesosTaskID,
					GoalState:            task.TaskState_RUNNING,
					DesiredConfigVersion: 42,
					ConfigVersion:        42,
				},
			},
		}, nil)

	suite.hostcache.EXPECT().RecoverPodInfoOnHost(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	)

	err := suite.recoveryHandler.Start()
	pool := offer.GetEventHandler().GetOfferPool()
	summary, _ := pool.GetHostSummary(hostname)
	suite.Equal(1, len(summary.GetTasks()))
	suite.NoError(err)
}

func (suite *RecoveryTestSuite) TestStartDBRecoveryFailure() {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	jobConfig := &job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 1,
	}

	// Do Recovery for Active Jobs
	suite.activeJobsOps.EXPECT().
		GetAll(gomock.Any()).
		Return([]*peloton.JobID{jobID}, nil)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), jobID).
		Return(&job.RuntimeInfo{
			State:     job.JobState_RUNNING,
			GoalState: job.JobState_SUCCEEDED,
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), jobID, gomock.Any()).
		Return(jobConfig, &models.ConfigAddOn{}, nil)

	suite.mockTaskStore.EXPECT().
		GetTasksForJobByRange(gomock.Any(), jobID, gomock.Any()).
		Return(nil, errors.New("db error"))

	err := suite.recoveryHandler.Start()
	suite.Error(err)
}

func (suite *RecoveryTestSuite) TestStop() {
	err := suite.recoveryHandler.Stop()
	suite.NoError(err)
}
