package entitlement

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/scalar"

	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

type EntitlementCalculatorTestSuite struct {
	sync.RWMutex
	suite.Suite
	resTree     respool.Tree
	calculator  *calculator
	mockCtrl    *gomock.Controller
	mockHostMgr *host_mocks.MockInternalHostServiceYARPCClient
}

func (suite *EntitlementCalculatorTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockHostMgr = host_mocks.NewMockInternalHostServiceYARPCClient(suite.mockCtrl)
	mockResPoolStore := store_mocks.NewMockResourcePoolStore(suite.mockCtrl)
	gomock.InOrder(
		mockResPoolStore.EXPECT().
			GetAllResourcePools(context.Background()).Return(suite.getResPools(), nil).AnyTimes(),
	)
	mockJobStore := store_mocks.NewMockJobStore(suite.mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(suite.mockCtrl)
	gomock.InOrder(
		mockJobStore.EXPECT().GetJobsByStates(context.Background(), gomock.Any()).
			Return(nil, nil).AnyTimes(),
	)
	respool.InitTree(tally.NoopScope, mockResPoolStore, mockJobStore, mockTaskStore)

	suite.resTree = respool.GetTree()

	suite.calculator = &calculator{
		resPoolTree:       suite.resTree,
		runningState:      runningStateNotStarted,
		calculationPeriod: 10 * time.Millisecond,
		stopChan:          make(chan struct{}, 1),
		clusterCapacity:   make(map[string]float64),
		hostMgrClient:     suite.mockHostMgr,
	}
}
func (suite *EntitlementCalculatorTestSuite) SetupTest() {
	fmt.Println("setting up")
	suite.resTree.Start()
}

func (suite *EntitlementCalculatorTestSuite) TearDownTest() {
	fmt.Println("tearing down")
	err := suite.resTree.Stop()
	suite.NoError(err)
	suite.mockCtrl.Finish()
}

func TestEntitlementCalculator(t *testing.T) {
	suite.Run(t, new(EntitlementCalculatorTestSuite))
}

func (suite *EntitlementCalculatorTestSuite) TestPeriodicCalculationWhenStarted() {
	var wg sync.WaitGroup
	wg.Add(5)

	suite.mockHostMgr.EXPECT().
		ClusterCapacity(
			gomock.Any(),
			gomock.Any()).
		Do(func(_, _ interface{}) {
			wg.Done()
		}).
		Return(&hostsvc.ClusterCapacityResponse{}, nil).
		Times(5)

	suite.NoError(suite.calculator.Start())

	// Wait for 5 calculations, and then stop.
	wg.Wait()

	suite.NoError(suite.calculator.Stop())
}

func (suite *EntitlementCalculatorTestSuite) getResourceConfig() []*pb_respool.ResourceConfig {
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

// Returns resource pools
func (suite *EntitlementCalculatorTestSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {
	rootID := pb_respool.ResourcePoolID{Value: "root"}
	policy := pb_respool.SchedulingPolicy_PriorityFIFO

	return map[string]*pb_respool.ResourcePoolConfig{
		"root": {
			Name:      "root",
			Parent:    nil,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool1": {
			Name:      "respool1",
			Parent:    &rootID,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool2": {
			Name:      "respool2",
			Parent:    &rootID,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool3": {
			Name:      "respool3",
			Parent:    &rootID,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool11": {
			Name:      "respool11",
			Parent:    &pb_respool.ResourcePoolID{Value: "respool1"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool12": {
			Name:      "respool12",
			Parent:    &pb_respool.ResourcePoolID{Value: "respool1"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool21": {
			Name:      "respool21",
			Parent:    &pb_respool.ResourcePoolID{Value: "respool2"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool22": {
			Name:      "respool22",
			Parent:    &pb_respool.ResourcePoolID{Value: "respool2"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
	}
}

func (suite *EntitlementCalculatorTestSuite) TestEntitlement() {
	// Mock LaunchTasks call.
	gomock.InOrder(
		suite.mockHostMgr.EXPECT().
			ClusterCapacity(
				gomock.Any(),
				gomock.Any()).
			Return(&hostsvc.ClusterCapacityResponse{
				Resources: []*hostsvc.Resource{
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
				},
			}, nil).
			Times(3),
	)
	ResPool, err := suite.resTree.Get(&pb_respool.ResourcePoolID{Value: "respool11"})
	suite.NoError(err)
	demand := &scalar.Resources{
		CPU:    20,
		MEMORY: 200,
		DISK:   2000,
		GPU:    0,
	}
	ResPool.AddToDemand(demand)
	suite.calculator.calculateEntitlement(context.Background())

	res := ResPool.GetEntitlement()
	suite.Equal(int64(res.CPU), int64(33))
	suite.Equal(int64(res.GPU), int64(0))
	suite.Equal(int64(res.MEMORY), int64(333))
	suite.Equal(int64(res.DISK), int64(2666))

	ResPool21, err := suite.resTree.Get(&pb_respool.ResourcePoolID{Value: "respool21"})
	suite.NoError(err)
	ResPool21.AddToDemand(demand)

	suite.calculator.calculateEntitlement(context.Background())

	res = ResPool.GetEntitlement()
	suite.Equal(int64(res.CPU), int64(30))
	suite.Equal(int64(res.GPU), int64(0))
	suite.Equal(int64(res.MEMORY), int64(300))
	suite.Equal(int64(res.DISK), int64(2333))

	res = ResPool21.GetEntitlement()
	suite.Equal(int64(res.CPU), int64(30))
	suite.Equal(int64(res.GPU), int64(0))
	suite.Equal(int64(res.MEMORY), int64(300))
	suite.Equal(int64(res.DISK), int64(2333))

	ResPool22, err := suite.resTree.Get(&pb_respool.ResourcePoolID{Value: "respool22"})
	suite.NoError(err)
	ResPool22.AddToDemand(demand)
	suite.calculator.calculateEntitlement(context.Background())

	res = ResPool.GetEntitlement()
	suite.Equal(int64(res.CPU), int64(26))
	suite.Equal(int64(res.GPU), int64(0))
	suite.Equal(int64(res.MEMORY), int64(266))
	suite.Equal(int64(res.DISK), int64(2000))

	res = ResPool21.GetEntitlement()
	suite.Equal(int64(res.CPU), int64(26))
	suite.Equal(int64(res.GPU), int64(0))
	suite.Equal(int64(res.MEMORY), int64(266))
	suite.Equal(int64(res.DISK), int64(2000))

	res = ResPool22.GetEntitlement()
	suite.Equal(int64(res.CPU), int64(26))
	suite.Equal(int64(res.GPU), int64(0))
	suite.Equal(int64(res.MEMORY), int64(266))
	suite.Equal(int64(res.DISK), int64(2000))

	ResPool2, err := suite.resTree.Get(&pb_respool.ResourcePoolID{Value: "respool2"})
	suite.NoError(err)

	res = ResPool2.GetEntitlement()
	suite.Equal(int64(res.CPU), int64(53))
	suite.Equal(int64(res.GPU), int64(0))
	suite.Equal(int64(res.MEMORY), int64(533))
	suite.Equal(int64(res.DISK), int64(4000))

	ResPool3, err := suite.resTree.Get(&pb_respool.ResourcePoolID{Value: "respool3"})
	suite.NoError(err)

	res = ResPool3.GetEntitlement()
	suite.Equal(int64(res.CPU), int64(13))
	suite.Equal(int64(res.GPU), int64(0))
	suite.Equal(int64(res.MEMORY), int64(133))
	suite.Equal(int64(res.DISK), int64(0))

	ResPool1, err := suite.resTree.Get(&pb_respool.ResourcePoolID{Value: "respool1"})
	suite.NoError(err)

	res = ResPool1.GetEntitlement()
	suite.Equal(int64(res.CPU), int64(33))
	suite.Equal(int64(res.GPU), int64(0))
	suite.Equal(int64(res.MEMORY), int64(333))
	suite.Equal(int64(res.DISK), int64(2000))

	ResPoolRoot, err := suite.resTree.Get(&pb_respool.ResourcePoolID{Value: "root"})
	suite.NoError(err)

	res = ResPoolRoot.GetEntitlement()
	suite.Equal(res.CPU, float64(100))
	suite.Equal(res.GPU, float64(0))
	suite.Equal(res.MEMORY, float64(1000))
	suite.Equal(res.DISK, float64(6000))

}

func (suite *EntitlementCalculatorTestSuite) TestUpdateCapacity() {
	// Mock LaunchTasks call.
	gomock.InOrder(
		suite.mockHostMgr.EXPECT().ClusterCapacity(gomock.Any(), gomock.Any()).
			Return(&hostsvc.ClusterCapacityResponse{
				Resources: []*hostsvc.Resource{
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
				},
			}, nil).
			Times(1),
	)

	rootres, err := suite.resTree.Get(&pb_respool.ResourcePoolID{Value: "root"})
	suite.NoError(err)
	rootres.SetResourcePoolConfig(suite.getResPools()["root"])
	suite.Equal(rootres.Resources()[common.CPU].Reservation, float64(10))
	suite.Equal(rootres.Resources()[common.GPU].Reservation, float64(1))
	suite.Equal(rootres.Resources()[common.MEMORY].Reservation, float64(100))
	suite.Equal(rootres.Resources()[common.DISK].Reservation, float64(1000))

	suite.calculator.calculateEntitlement(context.Background())

	RootResPool, err := suite.resTree.Get(&pb_respool.ResourcePoolID{Value: "root"})
	suite.NoError(err)
	suite.Equal(RootResPool.Resources()[common.CPU].Reservation, float64(100))
	suite.Equal(RootResPool.Resources()[common.GPU].Reservation, float64(0))
	suite.Equal(RootResPool.Resources()[common.MEMORY].Reservation, float64(1000))
	suite.Equal(RootResPool.Resources()[common.DISK].Reservation, float64(6000))
}
