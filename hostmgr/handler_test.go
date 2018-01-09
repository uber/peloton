package hostmgr

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	mesos_master "code.uber.internal/infra/peloton/.gen/mesos/v1/master"
	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common/reservation"
	"code.uber.internal/infra/peloton/hostmgr/hostmap"
	hostmgr_mesos_mocks "code.uber.internal/infra/peloton/hostmgr/mesos/mocks"
	"code.uber.internal/infra/peloton/hostmgr/offer/offerpool"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"
	mpb_mocks "code.uber.internal/infra/peloton/yarpc/encoding/mpb/mocks"
)

const (
	_offerHoldTime = time.Minute * 5
	_streamID      = "streamID"
	_frameworkID   = "frameworkID"

	_perHostCPU  = 10.0
	_perHostMem  = 20.0
	_perHostDisk = 30.0

	_epsilon = 0.00001

	_taskIDFmt  = "testjob-%d-abcdef12-abcd-1234-5678-1234567890ab"
	_defaultCmd = "/bin/sh"

	_cpuName  = "cpus"
	_memName  = "mem"
	_diskName = "disk"
	_gpuName  = "gpus"

	_defaultResourceValue = 1
)

var (
	rootCtx      = context.Background()
	_testKey     = "testKey"
	_pelotonRole = "peloton"
)

func generateOffers(numOffers int) []*mesos.Offer {
	var offers []*mesos.Offer
	for i := 0; i < numOffers; i++ {
		oid := fmt.Sprintf("offer-%d", i)
		aid := fmt.Sprintf("agent-%d", i)
		hostname := fmt.Sprintf("hostname-%d", i)
		offers = append(offers, &mesos.Offer{
			Id:       &mesos.OfferID{Value: &oid},
			AgentId:  &mesos.AgentID{Value: &aid},
			Hostname: &hostname,
			Resources: []*mesos.Resource{
				util.NewMesosResourceBuilder().
					WithName("cpus").
					WithValue(_perHostCPU).
					Build(),
				util.NewMesosResourceBuilder().
					WithName("mem").
					WithValue(_perHostMem).
					Build(),
				util.NewMesosResourceBuilder().
					WithName("disk").
					WithValue(_perHostDisk).
					Build(),
			},
		})
	}
	return offers
}

func generateLaunchableTasks(numTasks int) []*hostsvc.LaunchableTask {
	var tasks []*hostsvc.LaunchableTask
	for i := 0; i < numTasks; i++ {
		tid := fmt.Sprintf(_taskIDFmt, i)
		tmpCmd := _defaultCmd
		tasks = append(tasks, &hostsvc.LaunchableTask{
			TaskId: &mesos.TaskID{Value: &tid},
			Config: &task.TaskConfig{
				Name: fmt.Sprintf("name-%d", i),
				Resource: &task.ResourceConfig{
					CpuLimit:    _perHostCPU,
					MemLimitMb:  _perHostMem,
					DiskLimitMb: _perHostDisk,
				},
				Command: &mesos.CommandInfo{
					Value: &tmpCmd,
				},
			},
			Volume: &hostsvc.Volume{
				Resource: util.NewMesosResourceBuilder().
					WithName("disk").
					WithValue(1.0).
					Build(),
				ContainerPath: "test",
				Id: &peloton.VolumeID{
					Value: "volumeid",
				},
			},
		})
	}
	return tasks
}

type HostMgrHandlerTestSuite struct {
	suite.Suite

	ctrl                 *gomock.Controller
	testScope            tally.TestScope
	schedulerClient      *mpb_mocks.MockSchedulerClient
	masterOperatorClient *mpb_mocks.MockMasterOperatorClient
	provider             *hostmgr_mesos_mocks.MockFrameworkInfoProvider
	volumeStore          *storage_mocks.MockPersistentVolumeStore
	pool                 offerpool.Pool
	handler              *serviceHandler
	frameworkID          *mesos.FrameworkID
}

func (suite *HostMgrHandlerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.schedulerClient = mpb_mocks.NewMockSchedulerClient(suite.ctrl)
	suite.masterOperatorClient = mpb_mocks.NewMockMasterOperatorClient(suite.ctrl)
	suite.provider = hostmgr_mesos_mocks.NewMockFrameworkInfoProvider(suite.ctrl)
	suite.volumeStore = storage_mocks.NewMockPersistentVolumeStore(suite.ctrl)

	mockValidValue := new(string)
	*mockValidValue = _frameworkID
	mockValidFrameWorkID := &mesos.FrameworkID{
		Value: mockValidValue,
	}

	suite.frameworkID = mockValidFrameWorkID

	suite.pool = offerpool.NewOfferPool(
		_offerHoldTime,
		suite.schedulerClient,
		offerpool.NewMetrics(suite.testScope.SubScope("offer")),
		nil,               /* frameworkInfoProvider */
		suite.volumeStore, /* volumeStore */
	)

	suite.handler = &serviceHandler{
		schedulerClient:       suite.schedulerClient,
		operatorMasterClient:  suite.masterOperatorClient,
		metrics:               NewMetrics(suite.testScope),
		offerPool:             suite.pool,
		frameworkInfoProvider: suite.provider,
		volumeStore:           suite.volumeStore,
	}
}

func (suite *HostMgrHandlerTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func (suite *HostMgrHandlerTestSuite) assertGaugeValues(
	values map[string]float64) {

	suite.pool.RefreshGaugeMaps()
	gauges := suite.testScope.Snapshot().Gauges()

	for key, value := range values {
		g, ok := gauges[key]
		suite.True(ok, "Snapshot %v does not have key %s", gauges, key)
		suite.InEpsilon(
			value,
			g.Value(),
			_epsilon,
			"Expected value %f does not match on key %s, full snapshot: %v",
			value,
			key,
			gauges,
		)
	}
}

// helper function to check particular set of resource quantity gauges
// matches number of hosts with default amount of resources.
func (suite *HostMgrHandlerTestSuite) checkResourcesGauges(
	numHosts int,
	status string,
) {
	values := map[string]float64{
		fmt.Sprintf("offer.pool.%s.cpu+", status):  float64(numHosts * _perHostCPU),
		fmt.Sprintf("offer.pool.%s.mem+", status):  float64(numHosts * _perHostMem),
		fmt.Sprintf("offer.pool.%s.disk+", status): float64(numHosts * _perHostDisk),
	}
	suite.assertGaugeValues(values)
}

// This checks the happy case of acquire -> release -> acquire
// sequence and verifies that released resources can be used
// again by next acquire call.
func (suite *HostMgrHandlerTestSuite) TestAcquireReleaseHostOffers() {
	defer suite.ctrl.Finish()

	numHosts := 5
	suite.pool.AddOffers(context.Background(), generateOffers(numHosts))

	suite.checkResourcesGauges(numHosts, "ready")
	suite.checkResourcesGauges(0, "placing")

	// Empty constraint.
	acquiredResp, err := suite.handler.AcquireHostOffers(
		rootCtx,
		&hostsvc.AcquireHostOffersRequest{},
	)

	suite.NoError(err)
	suite.NotNil(acquiredResp.GetError().GetInvalidHostFilter())

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["acquire_host_offers_invalid+"].Value())

	// Matching constraint.
	acquireReq := &hostsvc.AcquireHostOffersRequest{
		Filter: &hostsvc.HostFilter{
			Quantity: &hostsvc.QuantityControl{
				MaxHosts: uint32(numHosts * 2),
			},
			ResourceConstraint: &hostsvc.ResourceConstraint{
				Minimum: &task.ResourceConfig{
					CpuLimit:    _perHostCPU,
					MemLimitMb:  _perHostMem,
					DiskLimitMb: _perHostDisk,
				},
			},
		},
	}
	acquiredResp, err = suite.handler.AcquireHostOffers(
		rootCtx,
		acquireReq,
	)

	suite.NoError(err)
	suite.Nil(acquiredResp.GetError())
	acquiredHostOffers := acquiredResp.GetHostOffers()
	suite.Equal(numHosts, len(acquiredHostOffers))

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["acquire_host_offers+"].Value())

	suite.Equal(
		int64(numHosts),
		suite.testScope.Snapshot().Counters()["acquire_hosts_count+"].Value())

	// TODO: Add check for number of HostOffers in placing state.
	suite.checkResourcesGauges(0, "ready")
	suite.checkResourcesGauges(numHosts, "placing")

	// Call AcquireHostOffers again should not give us anything.
	acquiredResp, err = suite.handler.AcquireHostOffers(
		rootCtx,
		acquireReq,
	)

	suite.NoError(err)
	suite.Nil(acquiredResp.GetError())
	suite.Equal(0, len(acquiredResp.GetHostOffers()))

	suite.Equal(
		int64(2),
		suite.testScope.Snapshot().Counters()["acquire_host_offers+"].Value())

	// Release previously acquired host offers.

	releaseReq := &hostsvc.ReleaseHostOffersRequest{
		HostOffers: acquiredHostOffers,
	}

	releaseResp, err := suite.handler.ReleaseHostOffers(
		rootCtx,
		releaseReq,
	)

	suite.NoError(err)
	suite.Nil(releaseResp.GetError())

	// TODO: Add check for number of HostOffers in placing state.
	suite.checkResourcesGauges(numHosts, "ready")
	suite.checkResourcesGauges(0, "placing")

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["release_host_offers+"].Value())

	suite.Equal(
		int64(numHosts),
		suite.testScope.Snapshot().Counters()["release_hosts_count+"].Value())

	// Acquire again should return non empty result.
	acquiredResp, err = suite.handler.AcquireHostOffers(
		rootCtx,
		acquireReq,
	)

	suite.NoError(err)
	suite.Nil(acquiredResp.GetError())
	suite.Equal(numHosts, len(acquiredResp.GetHostOffers()))
}

// This checks the happy case of acquire -> launch
// sequence.
func (suite *HostMgrHandlerTestSuite) TestAcquireAndLaunch() {
	defer suite.ctrl.Finish()

	// only create one host offer in this test.
	numHosts := 1
	suite.pool.AddOffers(context.Background(), generateOffers(numHosts))

	// TODO: Add check for number of HostOffers in placing state.
	suite.checkResourcesGauges(numHosts, "ready")
	suite.checkResourcesGauges(0, "placing")

	// Matching constraint.
	acquireReq := &hostsvc.AcquireHostOffersRequest{
		Filter: &hostsvc.HostFilter{
			Quantity: &hostsvc.QuantityControl{
				MaxHosts: uint32(1),
			},
			ResourceConstraint: &hostsvc.ResourceConstraint{
				Minimum: &task.ResourceConfig{
					CpuLimit:    _perHostCPU,
					MemLimitMb:  _perHostMem,
					DiskLimitMb: _perHostDisk,
				},
			},
		},
	}
	acquiredResp, err := suite.handler.AcquireHostOffers(
		rootCtx,
		acquireReq,
	)

	suite.NoError(err)
	suite.Nil(acquiredResp.GetError())
	acquiredHostOffers := acquiredResp.GetHostOffers()
	suite.Equal(1, len(acquiredHostOffers))

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["acquire_host_offers+"].Value())

	// TODO: Add check for number of HostOffers in placing state.
	suite.checkResourcesGauges(0, "ready")
	suite.checkResourcesGauges(numHosts, "placing")

	// An empty launch request will trigger an error.
	launchReq := &hostsvc.LaunchTasksRequest{
		Hostname: acquiredHostOffers[0].GetHostname(),
		AgentId:  acquiredHostOffers[0].GetAgentId(),
		Tasks:    []*hostsvc.LaunchableTask{},
	}

	launchResp, err := suite.handler.LaunchTasks(
		rootCtx,
		launchReq,
	)

	suite.NoError(err)
	suite.NotNil(launchResp.GetError().GetInvalidArgument())

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["launch_tasks_invalid+"].Value())

	// Generate some launchable tasks.
	launchReq.Tasks = generateLaunchableTasks(1)

	gomock.InOrder(
		// Set expectations on provider
		suite.provider.EXPECT().GetFrameworkID(context.Background()).Return(
			suite.frameworkID),
		// Set expectations on provider
		suite.provider.EXPECT().GetMesosStreamID(context.Background()).Return(_streamID),
		// Set expectations on scheduler schedulerClient
		suite.schedulerClient.EXPECT().
			Call(
				gomock.Eq(_streamID),
				gomock.Any(),
			).
			Do(func(_ string, msg proto.Message) {
				// Verify clientCall message.
				call := msg.(*sched.Call)
				suite.Equal(sched.Call_ACCEPT, call.GetType())
				suite.Equal(_frameworkID, call.GetFrameworkId().GetValue())

				accept := call.GetAccept()
				suite.NotNil(accept)
				suite.Equal(1, len(accept.GetOfferIds()))
				suite.Equal("offer-0", accept.GetOfferIds()[0].GetValue())
				suite.Equal(1, len(accept.GetOperations()))
				operation := accept.GetOperations()[0]
				suite.Equal(
					mesos.Offer_Operation_LAUNCH,
					operation.GetType())
				launch := operation.GetLaunch()
				suite.NotNil(launch)
				suite.Equal(1, len(launch.GetTaskInfos()))
				suite.Equal(
					fmt.Sprintf(_taskIDFmt, 0),
					launch.GetTaskInfos()[0].GetTaskId().GetValue())
			}).
			Return(nil),
	)

	launchResp, err = suite.handler.LaunchTasks(
		rootCtx,
		launchReq,
	)

	suite.NoError(err)
	suite.Nil(launchResp.GetError())
	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["launch_tasks+"].Value())

	// TODO: Add check for number of HostOffers in placing state.
	suite.checkResourcesGauges(0, "ready")
	suite.checkResourcesGauges(0, "placing")
}

// This checks the happy case of acquire -> launch sequence using offer operations.
func (suite *HostMgrHandlerTestSuite) TestAcquireAndLaunchOperation() {
	defer suite.ctrl.Finish()

	// only create one host offer in this test.
	numHosts := 1
	suite.pool.AddOffers(context.Background(), generateOffers(numHosts))

	// TODO: Add check for number of HostOffers in placing state.
	suite.checkResourcesGauges(numHosts, "ready")
	suite.checkResourcesGauges(0, "placing")

	// Matching constraint.
	acquireReq := &hostsvc.AcquireHostOffersRequest{
		Filter: &hostsvc.HostFilter{
			Quantity: &hostsvc.QuantityControl{
				MaxHosts: uint32(1),
			},
			ResourceConstraint: &hostsvc.ResourceConstraint{
				Minimum: &task.ResourceConfig{
					CpuLimit:    _perHostCPU,
					MemLimitMb:  _perHostMem,
					DiskLimitMb: _perHostDisk,
				},
			},
		},
	}
	acquiredResp, err := suite.handler.AcquireHostOffers(
		rootCtx,
		acquireReq,
	)

	suite.NoError(err)
	suite.Nil(acquiredResp.GetError())
	acquiredHostOffers := acquiredResp.GetHostOffers()
	suite.Equal(1, len(acquiredHostOffers))

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["acquire_host_offers+"].Value())

	// TODO: Add check for number of HostOffers in placing state.
	suite.checkResourcesGauges(0, "ready")
	suite.checkResourcesGauges(numHosts, "placing")

	launchOperation := &hostsvc.OfferOperation{
		Type: hostsvc.OfferOperation_LAUNCH,
		Launch: &hostsvc.OfferOperation_Launch{
			Tasks: []*hostsvc.LaunchableTask{},
		},
	}
	// An empty launch request will trigger an error.
	operationReq := &hostsvc.OfferOperationsRequest{
		Hostname: acquiredHostOffers[0].GetHostname(),
		Operations: []*hostsvc.OfferOperation{
			launchOperation,
		},
	}

	operationResp, err := suite.handler.OfferOperations(
		rootCtx,
		operationReq,
	)

	suite.NoError(err)
	suite.NotNil(operationResp.GetError().GetInvalidArgument())

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["offer_operations_invalid+"].Value())

	// Generate some launchable tasks.
	operationReq.Operations[0].Launch.Tasks = generateLaunchableTasks(1)

	gomock.InOrder(
		// Set expectations on provider
		suite.provider.EXPECT().GetFrameworkID(context.Background()).Return(
			suite.frameworkID),
		// Set expectations on provider
		suite.provider.EXPECT().GetMesosStreamID(context.Background()).Return(_streamID),
		// Set expectations on scheduler schedulerClient
		suite.schedulerClient.EXPECT().
			Call(
				gomock.Eq(_streamID),
				gomock.Any(),
			).
			Do(func(_ string, msg proto.Message) {
				// Verify clientCall message.
				call := msg.(*sched.Call)
				suite.Equal(sched.Call_ACCEPT, call.GetType())
				suite.Equal(_frameworkID, call.GetFrameworkId().GetValue())

				accept := call.GetAccept()
				suite.NotNil(accept)
				suite.Equal(1, len(accept.GetOfferIds()))
				suite.Equal("offer-0", accept.GetOfferIds()[0].GetValue())
				suite.Equal(1, len(accept.GetOperations()))
				operation := accept.GetOperations()[0]
				suite.Equal(
					mesos.Offer_Operation_LAUNCH,
					operation.GetType())
				launch := operation.GetLaunch()
				suite.NotNil(launch)
				suite.Equal(1, len(launch.GetTaskInfos()))
				suite.Equal(
					fmt.Sprintf(_taskIDFmt, 0),
					launch.GetTaskInfos()[0].GetTaskId().GetValue())
			}).
			Return(nil),
	)

	operationResp, err = suite.handler.OfferOperations(
		rootCtx,
		operationReq,
	)

	suite.NoError(err)
	suite.Nil(operationResp.GetError())
	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["offer_operations+"].Value())

	// TODO: Add check for number of HostOffers in placing state.
	suite.checkResourcesGauges(0, "ready")
	suite.checkResourcesGauges(0, "placing")
}

// Test happy case of shutdown executor
func (suite *HostMgrHandlerTestSuite) TestShutdownExecutors() {
	defer suite.ctrl.Finish()

	e1 := "e1"
	a1 := "a1"
	e2 := "e2"
	a2 := "a2"

	shutdownExecutors := []*hostsvc.ExecutorOnAgent{
		{
			ExecutorId: &mesos.ExecutorID{Value: &e1},
			AgentId:    &mesos.AgentID{Value: &a1},
		},
		{
			ExecutorId: &mesos.ExecutorID{Value: &e2},
			AgentId:    &mesos.AgentID{Value: &a2},
		},
	}
	shutdownReq := &hostsvc.ShutdownExecutorsRequest{
		Executors: shutdownExecutors,
	}

	shutdownedExecutors := make(map[string]string)
	mockMutex := &sync.Mutex{}

	// Set expectations on provider
	suite.provider.EXPECT().GetFrameworkID(context.Background()).Return(
		suite.frameworkID,
	).Times(2)
	suite.provider.EXPECT().GetMesosStreamID(context.Background()).Return(
		_streamID,
	).Times(2)

	suite.schedulerClient.EXPECT().
		Call(
			gomock.Eq(_streamID),
			gomock.Any(),
		).
		Do(func(_ string, msg proto.Message) {
			// Verify clientCall message.
			call := msg.(*sched.Call)
			suite.Equal(sched.Call_SHUTDOWN, call.GetType())
			suite.Equal(_frameworkID, call.GetFrameworkId().GetValue())

			aid := call.GetShutdown().GetAgentId()
			eid := call.GetShutdown().GetExecutorId()

			suite.NotNil(aid)
			suite.NotNil(eid)

			mockMutex.Lock()
			defer mockMutex.Unlock()
			shutdownedExecutors[eid.GetValue()] = aid.GetValue()
		}).
		Return(nil).
		Times(2)

	resp, err := suite.handler.ShutdownExecutors(rootCtx, shutdownReq)
	suite.NoError(err)
	suite.Nil(resp.GetError())
	suite.Equal(
		map[string]string{"e1": "a1", "e2": "a2"},
		shutdownedExecutors)

	suite.Equal(
		int64(2),
		suite.testScope.Snapshot().Counters()["shutdown_executors+"].Value())
}

// Test some failure cases of shutdown executor
func (suite *HostMgrHandlerTestSuite) TestShutdownExecutorsFailure() {
	defer suite.ctrl.Finish()

	e1 := "e1"
	a1 := "a1"
	e2 := "e2"
	a2 := "a2"

	shutdownExecutors := []*hostsvc.ExecutorOnAgent{
		{
			ExecutorId: &mesos.ExecutorID{Value: &e1},
			AgentId:    &mesos.AgentID{Value: &a1},
		},
		{
			ExecutorId: &mesos.ExecutorID{Value: &e2},
			AgentId:    &mesos.AgentID{Value: &a2},
		},
	}
	shutdownReq := &hostsvc.ShutdownExecutorsRequest{
		Executors: shutdownExecutors,
	}

	failedExecutors := make(map[string]string)
	shutdownedExecutors := make(map[string]string)
	mockMutex := &sync.Mutex{}

	// Set expectations on provider
	suite.provider.EXPECT().GetFrameworkID(context.Background()).Return(
		suite.frameworkID,
	).Times(2)
	suite.provider.EXPECT().GetMesosStreamID(context.Background()).Return(
		_streamID,
	).Times(2)

	// A failed call.
	suite.schedulerClient.EXPECT().
		Call(
			gomock.Eq(_streamID),
			gomock.Any(),
		).
		Do(func(_ string, msg proto.Message) {
			// Verify call message and process the executor into failedShutdownedExecutors
			call := msg.(*sched.Call)
			suite.Equal(sched.Call_SHUTDOWN, call.GetType())
			suite.Equal(_frameworkID, call.GetFrameworkId().GetValue())

			aid := call.GetShutdown().GetAgentId()
			eid := call.GetShutdown().GetExecutorId()

			suite.NotNil(aid)
			suite.NotNil(eid)
			mockMutex.Lock()
			defer mockMutex.Unlock()
			failedExecutors[eid.GetValue()] = aid.GetValue()
		}).
		Return(errors.New("Some error"))

	// A successful call.
	suite.schedulerClient.EXPECT().
		Call(
			gomock.Eq(_streamID),
			gomock.Any(),
		).
		Do(func(_ string, msg proto.Message) {
			// Verify call message while process the kill task id into `killedTaskIds`
			call := msg.(*sched.Call)
			suite.Equal(sched.Call_SHUTDOWN, call.GetType())
			suite.Equal(_frameworkID, call.GetFrameworkId().GetValue())

			aid := call.GetShutdown().GetAgentId()
			eid := call.GetShutdown().GetExecutorId()

			suite.NotNil(aid)
			suite.NotNil(eid)
			mockMutex.Lock()
			defer mockMutex.Unlock()

			shutdownedExecutors[eid.GetValue()] = aid.GetValue()
		}).
		Return(nil)

	resp, err := suite.handler.ShutdownExecutors(rootCtx, shutdownReq)
	suite.NoError(err)
	suite.NotNil(resp.GetError().GetShutdownFailure())

	suite.Equal(1, len(failedExecutors))
	suite.Equal(1, len(shutdownedExecutors))

	suite.NotEqual(shutdownedExecutors, failedExecutors)

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["shutdown_executors+"].Value())

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["shutdown_executors_fail+"].Value())

}

// Test happy case of killing task
func (suite *HostMgrHandlerTestSuite) TestKillTask() {
	defer suite.ctrl.Finish()

	t1 := "t1"
	t2 := "t2"
	taskIDs := []*mesos.TaskID{
		{Value: &t1},
		{Value: &t2},
	}
	killReq := &hostsvc.KillTasksRequest{
		TaskIds: taskIDs,
	}
	killedTaskIds := make(map[string]bool)
	mockMutex := &sync.Mutex{}

	// Set expectations on provider
	suite.provider.EXPECT().GetFrameworkID(context.Background()).Return(
		suite.frameworkID,
	).Times(2)
	suite.provider.EXPECT().GetMesosStreamID(context.Background()).Return(
		_streamID,
	).Times(2)

	// Set expectations on scheduler schedulerClient
	suite.schedulerClient.EXPECT().
		Call(
			gomock.Eq(_streamID),
			gomock.Any(),
		).
		Do(func(_ string, msg proto.Message) {
			// Verify clientCall message.
			call := msg.(*sched.Call)
			suite.Equal(sched.Call_KILL, call.GetType())
			suite.Equal(_frameworkID, call.GetFrameworkId().GetValue())

			tid := call.GetKill().GetTaskId()
			suite.NotNil(tid)
			mockMutex.Lock()
			defer mockMutex.Unlock()
			killedTaskIds[tid.GetValue()] = true
		}).
		Return(nil).
		Times(2)

	resp, err := suite.handler.KillTasks(rootCtx, killReq)
	suite.NoError(err)
	suite.Nil(resp.GetError())
	suite.Equal(
		map[string]bool{"t1": true, "t2": true},
		killedTaskIds)

	suite.Equal(
		int64(2),
		suite.testScope.Snapshot().Counters()["kill_tasks+"].Value())
}

// Test some failure cases of killing task
func (suite *HostMgrHandlerTestSuite) TestKillTaskFailure() {
	defer suite.ctrl.Finish()

	t1 := "t1"
	t2 := "t2"
	taskIDs := []*mesos.TaskID{
		{Value: &t1},
		{Value: &t2},
	}
	killReq := &hostsvc.KillTasksRequest{
		TaskIds: taskIDs,
	}

	killedTaskIds := make(map[string]bool)
	failedTaskIds := make(map[string]bool)
	mockMutex := &sync.Mutex{}

	// Set expectations on provider
	suite.provider.EXPECT().GetFrameworkID(context.Background()).Return(
		suite.frameworkID,
	).Times(2)
	suite.provider.EXPECT().GetMesosStreamID(context.Background()).Return(
		_streamID,
	).Times(2)

	// A failed call.
	suite.schedulerClient.EXPECT().
		Call(
			gomock.Eq(_streamID),
			gomock.Any(),
		).
		Do(func(_ string, msg proto.Message) {
			// Verify call message and process task id into `failedTaskIds`
			call := msg.(*sched.Call)
			suite.Equal(sched.Call_KILL, call.GetType())
			suite.Equal(_frameworkID, call.GetFrameworkId().GetValue())

			tid := call.GetKill().GetTaskId()
			suite.NotNil(tid)
			mockMutex.Lock()
			defer mockMutex.Unlock()
			failedTaskIds[tid.GetValue()] = true
		}).
		Return(errors.New("Some error"))

	// A successful call.
	suite.schedulerClient.EXPECT().
		Call(
			gomock.Eq(_streamID),
			gomock.Any(),
		).
		Do(func(_ string, msg proto.Message) {
			// Verify call message while process the kill task id into `killedTaskIds`
			call := msg.(*sched.Call)
			suite.Equal(sched.Call_KILL, call.GetType())
			suite.Equal(_frameworkID, call.GetFrameworkId().GetValue())

			tid := call.GetKill().GetTaskId()
			suite.NotNil(tid)
			mockMutex.Lock()
			defer mockMutex.Unlock()

			killedTaskIds[tid.GetValue()] = true
		}).
		Return(nil)

	resp, err := suite.handler.KillTasks(rootCtx, killReq)
	suite.NoError(err)
	suite.NotNil(resp.GetError().GetKillFailure())

	suite.Equal(1, len(killedTaskIds))
	suite.Equal(1, len(failedTaskIds))

	suite.NotEqual(killedTaskIds, failedTaskIds)

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["kill_tasks+"].Value())

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["kill_tasks_fail+"].Value())
}

func (suite *HostMgrHandlerTestSuite) TestServiceHandlerClusterCapacity() {
	scalerType := mesos.Value_SCALAR
	scalerVal := 200.0
	name := "cpus"

	loader := &hostmap.Loader{
		OperatorClient: suite.masterOperatorClient,
		Scope:          suite.testScope,
	}
	numAgents := 2
	response := makeAgentsResponse(numAgents)
	suite.masterOperatorClient.EXPECT().Agents().Return(response, nil)

	loader.Load(nil)

	tests := []struct {
		err         error
		response    []*mesos.Resource
		clientCall  bool
		frameworkID *mesos.FrameworkID
	}{
		{
			err:         errors.New("no resources configured"),
			response:    nil,
			clientCall:  true,
			frameworkID: suite.frameworkID,
		},
		{
			err: nil,
			response: []*mesos.Resource{
				{
					Name: &name,
					Scalar: &mesos.Value_Scalar{
						Value: &scalerVal,
					},
					Type: &scalerType,
				},
			},
			clientCall:  true,
			frameworkID: suite.frameworkID,
		},
		{
			err:         errors.New("unable to fetch framework ID"),
			clientCall:  true,
			frameworkID: nil,
		},
	}

	clusterCapacityReq := &hostsvc.ClusterCapacityRequest{}
	for _, tt := range tests {
		// Set expectations on provider interface
		suite.provider.EXPECT().GetFrameworkID(context.Background()).Return(tt.frameworkID)

		if tt.clientCall {
			// Set expectations on the mesos operator client
			suite.masterOperatorClient.EXPECT().AllocatedResources(
				gomock.Any(),
			).Return(tt.response, tt.err)
		}

		// Make the cluster capacity API request
		resp, _ := suite.handler.ClusterCapacity(
			rootCtx,
			clusterCapacityReq,
		)

		if tt.err != nil {
			suite.NotNil(resp.Error)
			suite.Equal(
				tt.err.Error(),
				resp.Error.ClusterUnavailable.Message,
			)
			suite.Nil(resp.Resources)
			suite.Nil(resp.PhysicalResources)
		} else {
			suite.Nil(resp.Error)
			suite.NotNil(resp.Resources)
			suite.Equal(4, len(resp.PhysicalResources))
			for _, v := range resp.PhysicalResources {
				suite.Equal(v.Capacity, float64(numAgents))
			}
		}
	}
}

func (suite *HostMgrHandlerTestSuite) TestLaunchOperationWithReservedOffers() {
	defer suite.ctrl.Finish()

	volumeInfo := &volume.PersistentVolumeInfo{}

	suite.volumeStore.EXPECT().
		GetPersistentVolume(context.Background(), gomock.Any()).
		AnyTimes().
		Return(volumeInfo, nil)
	suite.volumeStore.EXPECT().
		UpdatePersistentVolume(context.Background(), volumeInfo).
		AnyTimes().
		Return(nil)

	gomock.InOrder(
		// Set expectations on provider
		suite.provider.EXPECT().GetFrameworkID(context.Background()).Return(
			suite.frameworkID),
		// Set expectations on provider
		suite.provider.EXPECT().GetMesosStreamID(context.Background()).Return(_streamID),
		// Set expectations on scheduler schedulerClient
		suite.schedulerClient.EXPECT().
			Call(
				gomock.Eq(_streamID),
				gomock.Any(),
			).
			Do(func(_ string, msg proto.Message) {
				// Verify clientCall message.
				call := msg.(*sched.Call)
				suite.Equal(sched.Call_ACCEPT, call.GetType())
				suite.Equal(_frameworkID, call.GetFrameworkId().GetValue())

				accept := call.GetAccept()
				suite.NotNil(accept)
				suite.Equal(1, len(accept.GetOfferIds()))
				suite.Equal("offer-0", accept.GetOfferIds()[0].GetValue())
				suite.Equal(1, len(accept.GetOperations()))
				launchOp := accept.GetOperations()[0]
				suite.Equal(
					mesos.Offer_Operation_LAUNCH,
					launchOp.GetType())
				launch := launchOp.GetLaunch()
				suite.NotNil(launch)
				suite.Equal(1, len(launch.GetTaskInfos()))
				suite.Equal(
					fmt.Sprintf(_taskIDFmt, 0),
					launch.GetTaskInfos()[0].GetTaskId().GetValue())
			}).
			Return(nil),
	)

	launchOperation := createHostLaunchOperation()

	operationReq := &hostsvc.OfferOperationsRequest{
		Hostname: "hostname-0",
		Operations: []*hostsvc.OfferOperation{
			launchOperation,
		},
	}

	reservedOffers := generateOffers(1)
	reservedOffer := reservedOffers[0]
	reservation := &mesos.Resource_ReservationInfo{
		Labels: createReservationLabels(),
	}
	diskInfo := &mesos.Resource_DiskInfo{
		Persistence: &mesos.Resource_DiskInfo_Persistence{
			Id: &_testKey,
		},
	}
	reservedResources := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName("cpus").
			WithValue(_perHostCPU).
			WithRole(_pelotonRole).
			WithReservation(reservation).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("mem").
			WithValue(_perHostMem).
			WithReservation(reservation).
			WithRole(_pelotonRole).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("disk").
			WithValue(_perHostDisk).
			WithRole(_pelotonRole).
			WithReservation(reservation).
			WithDisk(diskInfo).
			Build(),
	}
	reservedOffer.Resources = append(reservedOffer.Resources, reservedResources...)
	suite.pool.AddOffers(context.Background(), reservedOffers)

	operationResp, err := suite.handler.OfferOperations(
		rootCtx,
		operationReq,
	)

	suite.NoError(err)
	suite.Nil(operationResp.GetError())
	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["offer_operations+"].Value())
}

func (suite *HostMgrHandlerTestSuite) TestReserveCreateLaunchOperation() {
	defer suite.ctrl.Finish()

	// only create one host offer in this test.
	numHosts := 1
	suite.pool.AddOffers(context.Background(), generateOffers(numHosts))

	// TODO: Add check for number of HostOffers in placing state.
	suite.checkResourcesGauges(numHosts, "ready")
	suite.checkResourcesGauges(0, "placing")

	// Matching constraint.
	acquireReq := getAcquireHostOffersRequest()
	acquiredResp, err := suite.handler.AcquireHostOffers(
		rootCtx,
		acquireReq,
	)

	suite.NoError(err)
	suite.Nil(acquiredResp.GetError())
	acquiredHostOffers := acquiredResp.GetHostOffers()
	suite.Equal(1, len(acquiredHostOffers))

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["acquire_host_offers+"].Value())

	// TODO: Add check for number of HostOffers in placing state.
	suite.checkResourcesGauges(0, "ready")
	suite.checkResourcesGauges(numHosts, "placing")

	reserveOperation := createHostReserveOperation()
	createOperation := createHostCreateOperation()
	launchOperation := createHostLaunchOperation()

	// launch operation before reserve/create will trigger an error.
	operationReq := &hostsvc.OfferOperationsRequest{
		Hostname: acquiredHostOffers[0].GetHostname(),
		Operations: []*hostsvc.OfferOperation{
			reserveOperation,
			launchOperation,
			createOperation,
		},
	}

	operationResp, err := suite.handler.OfferOperations(
		rootCtx,
		operationReq,
	)

	suite.NoError(err)
	suite.NotNil(operationResp.GetError().GetInvalidArgument())

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["offer_operations_invalid+"].Value())

	gomock.InOrder(
		suite.volumeStore.EXPECT().
			GetPersistentVolume(gomock.Any(), gomock.Any()).
			Return(nil, nil),

		suite.volumeStore.EXPECT().
			CreatePersistentVolume(gomock.Any(), gomock.Any()).
			Return(nil),

		// Set expectations on provider
		suite.provider.EXPECT().GetFrameworkID(context.Background()).Return(
			suite.frameworkID),
		// Set expectations on provider
		suite.provider.EXPECT().GetMesosStreamID(context.Background()).Return(_streamID),
		// Set expectations on scheduler schedulerClient
		suite.schedulerClient.EXPECT().
			Call(
				gomock.Eq(_streamID),
				gomock.Any(),
			).
			Do(func(_ string, msg proto.Message) {
				// Verify clientCall message.
				call := msg.(*sched.Call)
				suite.Equal(sched.Call_ACCEPT, call.GetType())
				suite.Equal(_frameworkID, call.GetFrameworkId().GetValue())

				accept := call.GetAccept()
				suite.NotNil(accept)
				suite.Equal(1, len(accept.GetOfferIds()))
				suite.Equal("offer-0", accept.GetOfferIds()[0].GetValue())
				suite.Equal(3, len(accept.GetOperations()))
				reserveOp := accept.GetOperations()[0]
				createOp := accept.GetOperations()[1]
				launchOp := accept.GetOperations()[2]
				suite.Equal(
					mesos.Offer_Operation_RESERVE,
					reserveOp.GetType())
				suite.Equal(
					mesos.Offer_Operation_CREATE,
					createOp.GetType())
				suite.Equal(
					mesos.Offer_Operation_LAUNCH,
					launchOp.GetType())
				launch := launchOp.GetLaunch()
				suite.NotNil(launch)
				suite.Equal(1, len(launch.GetTaskInfos()))
				suite.Equal(
					fmt.Sprintf(_taskIDFmt, 0),
					launch.GetTaskInfos()[0].GetTaskId().GetValue())
			}).
			Return(nil),
	)

	operationReq = &hostsvc.OfferOperationsRequest{
		Hostname: acquiredHostOffers[0].GetHostname(),
		Operations: []*hostsvc.OfferOperation{
			reserveOperation,
			createOperation,
			launchOperation,
		},
	}

	operationResp, err = suite.handler.OfferOperations(
		rootCtx,
		operationReq,
	)

	suite.NoError(err)
	suite.Nil(operationResp.GetError())
	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["offer_operations+"].Value())

	// TODO: Add check for number of HostOffers in placing state.
	suite.checkResourcesGauges(0, "ready")
	suite.checkResourcesGauges(0, "placing")
}

func (suite *HostMgrHandlerTestSuite) TestReserveCreateLaunchOperationWithCreatedVolume() {
	defer suite.ctrl.Finish()

	// only create one host offer in this test.
	numHosts := 1
	suite.pool.AddOffers(context.Background(), generateOffers(numHosts))

	// Matching constraint.
	acquireReq := getAcquireHostOffersRequest()
	_, err := suite.handler.AcquireHostOffers(
		rootCtx,
		acquireReq,
	)

	reserveOperation := createHostReserveOperation()
	createOperation := createHostCreateOperation()
	launchOperation := createHostLaunchOperation()

	volumeInfo := &volume.PersistentVolumeInfo{}

	gomock.InOrder(
		suite.volumeStore.EXPECT().
			GetPersistentVolume(gomock.Any(), gomock.Any()).
			Return(volumeInfo, nil),

		// Set expectations on provider
		suite.provider.EXPECT().GetFrameworkID(context.Background()).Return(
			suite.frameworkID),
		// Set expectations on provider
		suite.provider.EXPECT().GetMesosStreamID(context.Background()).Return(_streamID),
		// Set expectations on scheduler schedulerClient
		suite.schedulerClient.EXPECT().
			Call(
				gomock.Eq(_streamID),
				gomock.Any(),
			).
			Do(func(_ string, msg proto.Message) {
				// Verify clientCall message.
				call := msg.(*sched.Call)
				suite.Equal(sched.Call_ACCEPT, call.GetType())
				suite.Equal(_frameworkID, call.GetFrameworkId().GetValue())

				accept := call.GetAccept()
				suite.NotNil(accept)
				suite.Equal(1, len(accept.GetOfferIds()))
				suite.Equal("offer-0", accept.GetOfferIds()[0].GetValue())
				suite.Equal(3, len(accept.GetOperations()))
				reserveOp := accept.GetOperations()[0]
				createOp := accept.GetOperations()[1]
				launchOp := accept.GetOperations()[2]
				suite.Equal(
					mesos.Offer_Operation_RESERVE,
					reserveOp.GetType())
				suite.Equal(
					mesos.Offer_Operation_CREATE,
					createOp.GetType())
				suite.Equal(
					mesos.Offer_Operation_LAUNCH,
					launchOp.GetType())
				launch := launchOp.GetLaunch()
				suite.NotNil(launch)
				suite.Equal(1, len(launch.GetTaskInfos()))
				suite.Equal(
					fmt.Sprintf(_taskIDFmt, 0),
					launch.GetTaskInfos()[0].GetTaskId().GetValue())
			}).
			Return(nil),
	)

	operationReq := &hostsvc.OfferOperationsRequest{
		Hostname: "hostname-0",
		Operations: []*hostsvc.OfferOperation{
			reserveOperation,
			createOperation,
			launchOperation,
		},
	}

	operationResp, err := suite.handler.OfferOperations(
		rootCtx,
		operationReq,
	)

	suite.NoError(err)
	suite.Nil(operationResp.GetError())
	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["offer_operations+"].Value())
}

func getAcquireHostOffersRequest() *hostsvc.AcquireHostOffersRequest {
	return &hostsvc.AcquireHostOffersRequest{
		Filter: &hostsvc.HostFilter{
			Quantity: &hostsvc.QuantityControl{
				MaxHosts: uint32(1),
			},
			ResourceConstraint: &hostsvc.ResourceConstraint{
				Minimum: &task.ResourceConfig{
					CpuLimit:    _perHostCPU,
					MemLimitMb:  _perHostMem,
					DiskLimitMb: _perHostDisk,
				},
			},
		},
	}
}

func createHostReserveOperation() *hostsvc.OfferOperation {
	reserveOperation := &hostsvc.OfferOperation{
		Type: hostsvc.OfferOperation_RESERVE,
		Reserve: &hostsvc.OfferOperation_Reserve{
			Resources: []*mesos.Resource{
				util.NewMesosResourceBuilder().
					WithName("cpus").
					WithValue(_perHostCPU).
					Build(),
				util.NewMesosResourceBuilder().
					WithName("mem").
					WithValue(11.0).
					Build(),
				util.NewMesosResourceBuilder().
					WithName("disk").
					WithValue(12.0).
					Build(),
			},
		},
		ReservationLabels: createReservationLabels(),
	}
	return reserveOperation
}

func createHostCreateOperation() *hostsvc.OfferOperation {
	createOperation := &hostsvc.OfferOperation{
		Type: hostsvc.OfferOperation_CREATE,
		Create: &hostsvc.OfferOperation_Create{
			Volume: &hostsvc.Volume{
				Resource: util.NewMesosResourceBuilder().
					WithName("disk").
					WithValue(1.0).
					Build(),
				ContainerPath: "test",
				Id: &peloton.VolumeID{
					Value: "volumeid",
				},
			},
		},
		ReservationLabels: createReservationLabels(),
	}
	return createOperation
}

func createHostLaunchOperation() *hostsvc.OfferOperation {
	launchOperation := &hostsvc.OfferOperation{
		Type: hostsvc.OfferOperation_LAUNCH,
		Launch: &hostsvc.OfferOperation_Launch{
			Tasks: generateLaunchableTasks(1),
		},
		ReservationLabels: createReservationLabels(),
	}
	return launchOperation
}

func createReservationLabels() *mesos.Labels {
	return reservation.CreateReservationLabels("testjob", 0, "hostname-0")
}

func makeAgentsResponse(numAgents int) *mesos_master.Response_GetAgents {
	response := &mesos_master.Response_GetAgents{
		Agents: []*mesos_master.Response_GetAgents_Agent{},
	}
	for i := 0; i < numAgents; i++ {
		resVal := float64(_defaultResourceValue)
		tmpID := fmt.Sprintf("id-%d", i)
		resources := []*mesos.Resource{
			util.NewMesosResourceBuilder().
				WithName(_cpuName).
				WithValue(resVal).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(_memName).
				WithValue(resVal).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(_diskName).
				WithValue(resVal).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(_gpuName).
				WithValue(resVal).
				Build(),
		}
		getAgent := &mesos_master.Response_GetAgents_Agent{
			AgentInfo: &mesos.AgentInfo{
				Id: &mesos.AgentID{
					Value: &tmpID,
				},
				Resources: resources,
			},
		}
		response.Agents = append(response.Agents, getAgent)
	}

	return response
}

func TestHostManagerTestSuite(t *testing.T) {
	suite.Run(t, new(HostMgrHandlerTestSuite))
}
