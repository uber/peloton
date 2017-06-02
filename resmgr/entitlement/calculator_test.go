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

	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

type EntitlementCalculatorTestSuite struct {
	sync.RWMutex
	suite.Suite
	resTree     respool.Tree
	calculator  *calculator
	mockCtrl    *gomock.Controller
	mockHostMgr *host_mocks.MockInternalHostServiceYarpcClient
}

func (suite *EntitlementCalculatorTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockHostMgr = host_mocks.NewMockInternalHostServiceYarpcClient(suite.mockCtrl)
	mockResPoolStore := store_mocks.NewMockResourcePoolStore(suite.mockCtrl)
	gomock.InOrder(
		mockResPoolStore.EXPECT().
			GetAllResourcePools(context.Background()).Return(suite.getResPools(), nil).AnyTimes(),
	)
	mockJobStore := store_mocks.NewMockJobStore(suite.mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(suite.mockCtrl)
	gomock.InOrder(
		mockJobStore.EXPECT().GetAllJobs(context.Background()).Return(nil, nil).AnyTimes(),
	)
	respool.InitTree(tally.NoopScope, mockResPoolStore, mockJobStore, mockTaskStore)

	suite.resTree = respool.GetTree()

	suite.calculator = &calculator{
		resPoolTree:       suite.resTree,
		runningState:      runningStateNotStarted,
		calculationPeriod: time.Duration(1) * time.Second,
		stopChan:          make(chan struct{}, 1),
		clusterCapacity:   make(map[string]float64),
		hostMgrClient:     suite.mockHostMgr,
	}
}
func (suite *EntitlementCalculatorTestSuite) SetupTest() {
	fmt.Println("setting up")
	suite.resTree.Start()
	suite.calculator.Start()
}

func (suite *EntitlementCalculatorTestSuite) TearDownTest() {
	fmt.Println("tearing down")
	err := suite.resTree.Stop()
	suite.NoError(err)
	suite.calculator.Stop()
	suite.mockCtrl.Finish()
}

func TestEntitlementCalculator(t *testing.T) {
	suite.Run(t, new(EntitlementCalculatorTestSuite))
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
			Times(1),
	)
	suite.calculator.calculateEntitlement(context.Background())

	ResPool, err := suite.resTree.Get(&pb_respool.ResourcePoolID{Value: "respool11"})
	suite.NoError(err)
	res := ResPool.GetEntitlement()
	suite.Equal(res[common.CPU], float64(24))
	suite.Equal(res[common.GPU], float64(1))
	suite.Equal(res[common.MEMORY], float64(240))
	suite.Equal(res[common.DISK], float64(1600))
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
