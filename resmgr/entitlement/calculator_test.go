package entitlement

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"

	"code.uber.internal/infra/peloton/common"
	res_common "code.uber.internal/infra/peloton/resmgr/common"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/scalar"
	"code.uber.internal/infra/peloton/resmgr/tasktestutil"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type EntitlementCalculatorTestSuite struct {
	sync.RWMutex
	suite.Suite
	resTree    respool.Tree
	calculator *Calculator
	mockCtrl   *gomock.Controller
}

func (s *EntitlementCalculatorTestSuite) SetupTest() {
	s.mockCtrl = gomock.NewController(s.T())

	s.calculator = &Calculator{
		resPoolTree:       s.resTree,
		runningState:      res_common.RunningStateNotStarted,
		calculationPeriod: 10 * time.Millisecond,
		stopChan:          make(chan struct{}, 1),
		clusterCapacity:   make(map[string]float64),
		metrics:           NewMetrics(tally.NoopScope),
	}
	s.initRespoolTree()
	s.resTree.Start()
}

func (s *EntitlementCalculatorTestSuite) initRespoolTree() {
	mockResPoolStore := store_mocks.NewMockResourcePoolStore(s.mockCtrl)
	gomock.InOrder(
		mockResPoolStore.EXPECT().
			GetAllResourcePools(context.Background()).Return(s.getResPools(), nil).AnyTimes(),
	)
	mockJobStore := store_mocks.NewMockJobStore(s.mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(s.mockCtrl)
	gomock.InOrder(
		mockJobStore.EXPECT().GetJobsByStates(context.Background(), gomock.Any()).
			Return(nil, nil).AnyTimes(),
	)
	respool.InitTree(tally.NoopScope, mockResPoolStore, mockJobStore,
		mockTaskStore, res_common.PreemptionConfig{Enabled: false})

	s.resTree = respool.GetTree()
	s.calculator.resPoolTree = s.resTree
}

func (s *EntitlementCalculatorTestSuite) TearDownTest() {
	err := s.resTree.Stop()
	s.NoError(err)
	s.mockCtrl.Finish()
}

func TestEntitlementCalculator(t *testing.T) {
	suite.Run(t, new(EntitlementCalculatorTestSuite))
}

func (s *EntitlementCalculatorTestSuite) TestPeriodicCalculationWhenStarted() {
	ch := make(chan int, 5)
	i := 0

	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(s.mockCtrl)
	mockHostMgr.EXPECT().
		ClusterCapacity(
			gomock.Any(),
			gomock.Any()).
		Do(func(_, _ interface{}) {
			i = i + 1
			ch <- i
		}).
		Return(&hostsvc.ClusterCapacityResponse{}, nil).
		MinTimes(5)

	calculator := &Calculator{
		resPoolTree:       s.resTree,
		runningState:      res_common.RunningStateNotStarted,
		calculationPeriod: 10 * time.Millisecond,
		stopChan:          make(chan struct{}, 1),
		clusterCapacity:   make(map[string]float64),
		metrics:           NewMetrics(tally.NoopScope),
		hostMgrClient:     mockHostMgr,
	}
	s.NoError(calculator.Start())

	// Wait for at least 5 calculations, and then stop.
out:
	for {
		select {
		case x := <-ch:
			if x == 5 {
				break out
			}
		}
	}

	s.NoError(calculator.Stop())
}

func (s *EntitlementCalculatorTestSuite) getResourceConfig() []*pb_respool.ResourceConfig {
	resConfigs := []*pb_respool.ResourceConfig{
		{
			Share:       1,
			Kind:        "cpu",
			Reservation: 10,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "memory",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "disk",
			Reservation: 1000,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "gpu",
			Reservation: 1,
			Limit:       4,
		},
	}
	return resConfigs
}

func (s *EntitlementCalculatorTestSuite) getStaticResourceConfig() []*pb_respool.ResourceConfig {
	resConfigs := []*pb_respool.ResourceConfig{
		{
			Share:       1,
			Kind:        "cpu",
			Reservation: 10,
			Limit:       1000,
			Type:        pb_respool.ReservationType_STATIC,
		},
		{
			Share:       1,
			Kind:        "memory",
			Reservation: 100,
			Limit:       1000,
			Type:        pb_respool.ReservationType_STATIC,
		},
		{
			Share:       1,
			Kind:        "disk",
			Reservation: 1000,
			Limit:       1000,
			Type:        pb_respool.ReservationType_STATIC,
		},
		{
			Share:       1,
			Kind:        "gpu",
			Reservation: 1,
			Limit:       4,
			Type:        pb_respool.ReservationType_STATIC,
		},
	}
	return resConfigs
}

// Returns resource pools
func (s *EntitlementCalculatorTestSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {
	rootID := peloton.ResourcePoolID{Value: "root"}
	policy := pb_respool.SchedulingPolicy_PriorityFIFO

	return map[string]*pb_respool.ResourcePoolConfig{
		"root": {
			Name:      "root",
			Parent:    nil,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool1": {
			Name:      "respool1",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool2": {
			Name:      "respool2",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool3": {
			Name:      "respool3",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool11": {
			Name:      "respool11",
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool12": {
			Name:      "respool12",
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool21": {
			Name:      "respool21",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool22": {
			Name:      "respool22",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
	}
}

func (s *EntitlementCalculatorTestSuite) getStaticResPools() map[string]*pb_respool.ResourcePoolConfig {
	rootID := peloton.ResourcePoolID{Value: "root"}
	policy := pb_respool.SchedulingPolicy_PriorityFIFO

	return map[string]*pb_respool.ResourcePoolConfig{
		"root": {
			Name:      "roots",
			Parent:    nil,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool1s": {
			Name:      "respool1s",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool2s": {
			Name:      "respool2s",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool3s": {
			Name:      "respool3s",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool11s": {
			Name:      "respool11s",
			Parent:    &peloton.ResourcePoolID{Value: "respool1s"},
			Resources: s.getStaticResourceConfig(),
			Policy:    policy,
		},
		"respool12s": {
			Name:      "respool12s",
			Parent:    &peloton.ResourcePoolID{Value: "respool1s"},
			Resources: s.getStaticResourceConfig(),
			Policy:    policy,
		},
		"respool21s": {
			Name:      "respool21s",
			Parent:    &peloton.ResourcePoolID{Value: "respool2s"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool22s": {
			Name:      "respool22s",
			Parent:    &peloton.ResourcePoolID{Value: "respool2s"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
	}
}

func (s *EntitlementCalculatorTestSuite) TestEntitlement() {
	// Mock LaunchTasks call.
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(s.mockCtrl)
	gomock.InOrder(
		mockHostMgr.EXPECT().
			ClusterCapacity(
				gomock.Any(),
				gomock.Any()).
			Return(&hostsvc.ClusterCapacityResponse{
				PhysicalResources: s.createClusterCapacity(),
			}, nil).
			AnyTimes(),
	)
	s.calculator.hostMgrClient = mockHostMgr
	resPool, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool11"})
	s.NoError(err)
	demand := &scalar.Resources{
		CPU:    20,
		MEMORY: 200,
		DISK:   2000,
		GPU:    0,
	}
	resPool.AddToDemand(demand)
	s.calculator.calculateEntitlement(context.Background())

	res := resPool.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 33, "GPU": 0, "MEMORY": 333, "DISK": 1000}))

	ResPool12, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool12"})
	res = ResPool12.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 13, "GPU": 0, "MEMORY": 133, "DISK": 0}))

	ResPool21, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool21"})
	s.NoError(err)
	ResPool21.AddToDemand(demand)

	s.calculator.calculateEntitlement(context.Background())

	res = resPool.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 30, "GPU": 0, "MEMORY": 300, "DISK": 1000}))

	res = ResPool12.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 10, "GPU": 0, "MEMORY": 100, "DISK": 0}))

	res = ResPool21.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 30, "GPU": 0, "MEMORY": 300, "DISK": 1000}))

	ResPool22, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool22"})
	s.NoError(err)
	ResPool22.AddToDemand(demand)
	s.calculator.calculateEntitlement(context.Background())

	res = resPool.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 26, "GPU": 0, "MEMORY": 266, "DISK": 1000}))

	res = ResPool21.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 26, "GPU": 0, "MEMORY": 266, "DISK": 1000}))

	res = ResPool22.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 26, "GPU": 0, "MEMORY": 266, "DISK": 1000}))

	ResPool2, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool2"})
	s.NoError(err)

	res = ResPool2.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 53, "GPU": 0, "MEMORY": 533, "DISK": 1000}))

	ResPool3, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)

	res = ResPool3.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 13, "GPU": 0, "MEMORY": 133, "DISK": 1000}))

	ResPool1, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool1"})
	s.NoError(err)

	res = ResPool1.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 33, "GPU": 0, "MEMORY": 333, "DISK": 1000}))

	ResPoolRoot, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "root"})
	s.NoError(err)

	res = ResPoolRoot.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 100, "GPU": 0, "MEMORY": 1000, "DISK": 6000}))
}

func (s *EntitlementCalculatorTestSuite) TestUpdateCapacity() {
	// Mock LaunchTasks call.
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(s.mockCtrl)
	gomock.InOrder(
		mockHostMgr.EXPECT().ClusterCapacity(gomock.Any(), gomock.Any()).
			Return(&hostsvc.ClusterCapacityResponse{
				PhysicalResources: s.createClusterCapacity(),
			}, nil).
			Times(1),

		mockHostMgr.EXPECT().ClusterCapacity(gomock.Any(), gomock.Any()).
			Return(&hostsvc.ClusterCapacityResponse{
				PhysicalResources: []*hostsvc.Resource{
					{
						Kind:     common.CPU,
						Capacity: 100,
					},
					{
						Kind:     common.GPU,
						Capacity: 10,
					},
					{
						Kind:     common.MEMORY,
						Capacity: 1000,
					},
					{
						Kind:     common.DISK,
						Capacity: 6000,
					},
				},
			}, nil).
			Times(1),
	)
	s.calculator.hostMgrClient = mockHostMgr

	rootres, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "root"})
	s.NoError(err)
	rootres.SetResourcePoolConfig(s.getResPools()["root"])
	s.Equal(rootres.Resources()[common.CPU].Reservation, float64(10))
	s.Equal(rootres.Resources()[common.GPU].Reservation, float64(1))
	s.Equal(rootres.Resources()[common.MEMORY].Reservation, float64(100))
	s.Equal(rootres.Resources()[common.DISK].Reservation, float64(1000))

	s.calculator.calculateEntitlement(context.Background())

	RootResPool, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "root"})
	s.NoError(err)
	s.Equal(RootResPool.Resources()[common.CPU].Reservation, float64(100))
	s.Equal(RootResPool.Resources()[common.GPU].Reservation, float64(0))
	s.Equal(RootResPool.Resources()[common.MEMORY].Reservation, float64(1000))
	s.Equal(RootResPool.Resources()[common.DISK].Reservation, float64(6000))
	s.Equal(RootResPool.Resources()[common.CPU].Limit, float64(100))
	s.Equal(RootResPool.Resources()[common.GPU].Limit, float64(0))
	s.Equal(RootResPool.Resources()[common.MEMORY].Limit, float64(1000))
	s.Equal(RootResPool.Resources()[common.DISK].Limit, float64(6000))

	// Update the cluster capacity (Add GPU)
	s.calculator.calculateEntitlement(context.Background())
	s.NoError(err)
	s.Equal(RootResPool.Resources()[common.CPU].Reservation, float64(100))
	s.Equal(RootResPool.Resources()[common.GPU].Reservation, float64(10))
	s.Equal(RootResPool.Resources()[common.MEMORY].Reservation, float64(1000))
	s.Equal(RootResPool.Resources()[common.DISK].Reservation, float64(6000))
	s.Equal(RootResPool.Resources()[common.CPU].Limit, float64(100))
	s.Equal(RootResPool.Resources()[common.GPU].Limit, float64(10))
	s.Equal(RootResPool.Resources()[common.MEMORY].Limit, float64(1000))
	s.Equal(RootResPool.Resources()[common.DISK].Limit, float64(6000))
}

func (s *EntitlementCalculatorTestSuite) TestEntitlementWithMoreDemand() {
	// Mock LaunchTasks call.
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(s.mockCtrl)
	gomock.InOrder(
		mockHostMgr.EXPECT().
			ClusterCapacity(
				gomock.Any(),
				gomock.Any()).
			Return(&hostsvc.ClusterCapacityResponse{
				PhysicalResources: s.createClusterCapacity(),
			}, nil).
			AnyTimes(),
	)
	s.calculator.hostMgrClient = mockHostMgr

	ResPoolRoot, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "root"})
	s.NoError(err)

	ResPool1, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool1"})
	s.NoError(err)
	ResPool2, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool2"})
	s.NoError(err)
	ResPool3, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	ResPool11, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool11"})
	s.NoError(err)
	ResPool12, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool12"})
	s.NoError(err)
	ResPool21, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool21"})
	s.NoError(err)
	ResPool22, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool22"})
	s.NoError(err)
	demand := &scalar.Resources{
		CPU:    40,
		MEMORY: 400,
		DISK:   4000,
		GPU:    0,
	}

	ResPool11.AddToDemand(demand)
	ResPool12.AddToDemand(demand)
	ResPool21.AddToDemand(demand)
	ResPool22.AddToDemand(demand)
	ResPool3.AddToDemand(demand)

	s.calculator.calculateEntitlement(context.Background())

	res := ResPoolRoot.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 100, "GPU": 0, "MEMORY": 1000, "DISK": 6000}))

	res = ResPool1.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 33, "GPU": 0, "MEMORY": 333, "DISK": 1000}))

	res = ResPool2.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 33, "GPU": 0, "MEMORY": 333, "DISK": 1000}))

	res = ResPool3.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 33, "GPU": 0, "MEMORY": 333, "DISK": 1000}))

	res = ResPool11.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 16, "GPU": 0, "MEMORY": 166, "DISK": 1000}))

	res = ResPool12.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 16, "GPU": 0, "MEMORY": 166, "DISK": 1000}))

	res = ResPool21.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 16, "GPU": 0, "MEMORY": 166, "DISK": 1000}))

	res = ResPool22.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 16, "GPU": 0, "MEMORY": 166, "DISK": 1000}))
}

func (s *EntitlementCalculatorTestSuite) TestNewCalculator() {
	// This test initializes the entitlement calculation
	// and check if Calculator is not nil
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(s.mockCtrl)
	calc := NewCalculator(
		10*time.Millisecond,
		tally.NoopScope,
		mockHostMgr,
	)
	s.NotNil(calc)
}

func (s *EntitlementCalculatorTestSuite) TestStartCalculatorMultipleTimes() {
	// This test covers if we start entitlement calculation
	// multiple times it will not start the other one if
	// previous is running
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(s.mockCtrl)
	mockHostMgr.EXPECT().
		ClusterCapacity(
			gomock.Any(),
			gomock.Any()).
		Return(&hostsvc.ClusterCapacityResponse{}, nil).
		Times(1)
	s.calculator.hostMgrClient = mockHostMgr

	s.NoError(s.calculator.Start())
	s.NoError(s.calculator.Start())
	s.NoError(s.calculator.Stop())
	s.NoError(s.calculator.Stop())
}

func (s *EntitlementCalculatorTestSuite) TestUpdateCapacityError() {
	// If hostmgr returns error, checking Entitlement fails
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(s.mockCtrl)
	mockHostMgr.EXPECT().ClusterCapacity(gomock.Any(), gomock.Any()).
		Return(&hostsvc.ClusterCapacityResponse{
			PhysicalResources: nil,
		}, errors.New("Capacity Unavailable")).
		Times(1)
	s.calculator.hostMgrClient = mockHostMgr
	err := s.calculator.calculateEntitlement(context.Background())
	s.Error(err)
}

// Testing the Static respools and their entitlement
func (s *EntitlementCalculatorTestSuite) TestStaticRespoolsEntitlement() {

	// Stopping and destroying the test suite's tree as
	// for this test we are building another tree

	err := s.resTree.Stop()

	respool.Destroy()

	s.NoError(err)

	// Building Local Tree for this test suite
	mockCtrl := gomock.NewController(s.T())
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(mockCtrl)
	mockResPoolStore := store_mocks.NewMockResourcePoolStore(mockCtrl)
	gomock.InOrder(
		mockResPoolStore.EXPECT().
			GetAllResourcePools(context.Background()).Return(s.getStaticResPools(), nil).AnyTimes(),
	)
	mockJobStore := store_mocks.NewMockJobStore(s.mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(s.mockCtrl)
	gomock.InOrder(
		mockJobStore.EXPECT().GetJobsByStates(context.Background(), gomock.Any()).
			Return(nil, nil).AnyTimes(),
	)

	respool.InitTree(tally.NoopScope, mockResPoolStore, mockJobStore,
		mockTaskStore, res_common.PreemptionConfig{Enabled: false})

	resTree := respool.GetTree()

	// Creating local Calculator object
	calculator := &Calculator{
		resPoolTree:       resTree,
		runningState:      res_common.RunningStateNotStarted,
		calculationPeriod: 10 * time.Millisecond,
		stopChan:          make(chan struct{}, 1),
		clusterCapacity:   make(map[string]float64),
		hostMgrClient:     mockHostMgr,
		metrics:           NewMetrics(tally.NoopScope),
	}

	resTree.Start()

	// Mock LaunchTasks call.
	gomock.InOrder(
		mockHostMgr.EXPECT().
			ClusterCapacity(
				gomock.Any(),
				gomock.Any()).
			Return(&hostsvc.ClusterCapacityResponse{
				PhysicalResources: s.createClusterCapacity(),
			}, nil).
			AnyTimes(),
	)
	s.calculator.hostMgrClient = mockHostMgr

	resPool, err := resTree.Get(&peloton.ResourcePoolID{Value: "respool11s"})
	s.NoError(err)
	demand := &scalar.Resources{
		CPU:    20,
		MEMORY: 200,
		DISK:   2000,
		GPU:    0,
	}
	resPool.AddToDemand(demand)
	calculator.calculateEntitlement(context.Background())
	res := resPool.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 28, "GPU": 1, "MEMORY": 283, "DISK": 1000}))

	err = resTree.Stop()
	s.NoError(err)
	mockCtrl.Finish()
	respool.Destroy()
	s.initRespoolTree()
}

// createClusterCapacity returns the cluster capacity of the cluster
func (s *EntitlementCalculatorTestSuite) createClusterCapacity() []*hostsvc.Resource {
	return []*hostsvc.Resource{
		{
			Kind:     common.CPU,
			Capacity: 100,
		},
		{
			Kind:     common.GPU,
			Capacity: 0,
		},
		{
			Kind:     common.MEMORY,
			Capacity: 1000,
		},
		{
			Kind:     common.DISK,
			Capacity: 6000,
		},
	}
}
