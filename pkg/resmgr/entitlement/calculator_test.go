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

package entitlement

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pb_respool "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/rpc"
	res_common "github.com/uber/peloton/pkg/resmgr/common"
	"github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/scalar"
	"github.com/uber/peloton/pkg/resmgr/tasktestutil"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/yarpcerrors"
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
		resPoolTree:          s.resTree,
		runningState:         res_common.RunningStateNotStarted,
		calculationPeriod:    10 * time.Millisecond,
		stopChan:             make(chan struct{}, 1),
		clusterCapacity:      make(map[string]float64),
		clusterSlackCapacity: make(map[string]float64),
		hostPoolCapacity:     make(map[string]*ResourceCapacity),
		metrics:              newMetrics(tally.NoopScope),
	}
	s.initRespoolTree()
	s.resTree.Start()
}

func (s *EntitlementCalculatorTestSuite) initRespoolTree() {
	mockResPoolOps := objectmocks.NewMockResPoolOps(s.mockCtrl)
	gomock.InOrder(
		mockResPoolOps.EXPECT().
			GetAll(context.Background()).Return(s.getResPools(), nil).AnyTimes(),
	)
	mockJobStore := store_mocks.NewMockJobStore(s.mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(s.mockCtrl)
	s.resTree = respool.NewTree(tally.NoopScope, mockResPoolOps, mockJobStore,
		mockTaskStore, res_common.PreemptionConfig{Enabled: false})

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
		resPoolTree:          s.resTree,
		runningState:         res_common.RunningStateNotStarted,
		calculationPeriod:    10 * time.Millisecond,
		stopChan:             make(chan struct{}, 1),
		clusterCapacity:      make(map[string]float64),
		clusterSlackCapacity: make(map[string]float64),
		metrics:              newMetrics(tally.NoopScope),
		capMgr: &v0CapacityManager{
			hostManagerV0: mockHostMgr,
		},
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
				PhysicalResources:      s.createClusterCapacity(),
				PhysicalSlackResources: s.createSlackClusterCapacity(),
			}, nil).
			AnyTimes(),
	)
	s.calculator.capMgr = &v0CapacityManager{
		hostManagerV0: mockHostMgr,
	}
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

func (s *EntitlementCalculatorTestSuite) TestEntitlementForSlackResources() {
	// Mock LaunchTasks call.
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(s.mockCtrl)
	gomock.InOrder(
		mockHostMgr.EXPECT().
			ClusterCapacity(
				gomock.Any(),
				gomock.Any()).
			Return(&hostsvc.ClusterCapacityResponse{
				PhysicalResources:      s.createClusterCapacity(),
				PhysicalSlackResources: s.createSlackClusterCapacity(),
			}, nil).
			AnyTimes(),
	)
	s.calculator.capMgr = &v0CapacityManager{
		hostManagerV0: mockHostMgr,
	}
	resPool11, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool11"})
	s.NoError(err)
	demand := &scalar.Resources{
		CPU:    20,
		MEMORY: 200,
		DISK:   2000,
		GPU:    0,
	}
	resPool11.AddToSlackDemand(demand)

	resPool21, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool21"})
	s.NoError(err)
	demand = &scalar.Resources{
		CPU:    20,
		MEMORY: 10,
		DISK:   50,
		GPU:    0,
	}
	resPool21.AddToSlackDemand(demand)

	alloc := scalar.NewAllocation()
	alloc.Value[scalar.SlackAllocation] = &scalar.Resources{
		CPU:    5.0,
		MEMORY: 5.0,
		DISK:   25.0,
		GPU:    0.0,
	}
	alloc.Value[scalar.TotalAllocation] = &scalar.Resources{
		CPU:    5.0,
		MEMORY: 5.0,
		DISK:   25.0,
		GPU:    0.0,
	}
	resPool21.AddToAllocation(alloc)

	resPool3, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	demand = &scalar.Resources{
		CPU:    20,
		MEMORY: 10,
		DISK:   100,
		GPU:    0,
	}
	resPool3.AddToSlackDemand(demand)

	s.calculator.calculateEntitlement(context.Background())

	// Slack Limit is 20% of reservation or minimum(slack allocation + demand, slack limit)

	// demand is higher or slack entitlement is capped at 20% default limit.
	resPool11Reservation := tasktestutil.GetReservationFromResourceConfig(resPool11.Resources())
	s.Equal(resPool11Reservation.GetMem()*0.20, resPool11.GetSlackEntitlement().GetMem())
	s.Equal(resPool11Reservation.GetDisk()*0.20, resPool11.GetSlackEntitlement().GetDisk())

	// slack entitlement is capped at slack demand + allocation < slack limit
	s.Equal(float64(15), resPool21.GetSlackEntitlement().GetMem())
	s.Equal(float64(75), resPool21.GetSlackEntitlement().GetDisk())

	// slack entitlement is capped at demand < slack limit
	s.Equal(demand.GetMem(), resPool3.GetSlackEntitlement().GetMem())
	s.Equal(demand.GetDisk(), resPool3.GetSlackEntitlement().GetDisk())

	// Distribution of 80 revocable cpus
	// Demand = 20, Allocation = 0
	s.Equal(float64(22.5), resPool11.GetSlackEntitlement().GetCPU())

	// Demand = 0, Allocation = 0
	resPool12, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool12"})
	s.Equal(float64(2.5), resPool12.GetSlackEntitlement().GetCPU())

	// Demand = 20, Allocation = 5
	s.Equal(float64(27.5), resPool21.GetSlackEntitlement().GetCPU())
	resPool22, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool22"})

	// Demand = 0, Allocation = 0
	s.Equal(float64(2.5), resPool22.GetSlackEntitlement().GetCPU())

	// Demand = 20, Allocation = 0
	s.Equal(float64(25), resPool3.GetSlackEntitlement().GetCPU())

	// demand is more than total physical slack cpus avaiable at cluster
	demand = &scalar.Resources{
		CPU:    30,
		MEMORY: 20,
		DISK:   20,
		GPU:    0,
	}
	resPool12.AddToSlackDemand(demand)
	s.calculator.calculateEntitlement(context.Background())

	// 80 revocable cpus to distribute
	// Root (R) has three childred: R1, R2, R3 with equal share,
	// thereby 26ish each

	// R1 -> demand + allocation = 50  :: satisfied = 35
	// R2 -> demand + allocation = 25  :: satisfied = 25
	// R3 -> demand + allocation = 20  :: satisfied = 20

	// R2 and R3's need was satisfied and remaning were given to R1.
	// Within R1, equal distribution for R11 and R12

	// From previous round of entitlement calculation, demand for R12
	// is increased, so unclaimed resources from other resource pool
	// are taken away

	// Demand = 20
	s.Equal(int(resPool11.GetSlackEntitlement().GetCPU()), 17)

	// Demand = 40
	s.Equal(int(resPool12.GetSlackEntitlement().GetCPU()), 17)

	// Demand = 20, Allocation = 5
	s.Equal(math.Round(resPool21.GetSlackEntitlement().GetCPU()), float64(25))

	// No demand and allocation
	s.Equal(resPool22.GetSlackEntitlement().GetCPU(), float64(0))

	// Demand = 20
	s.Equal(resPool3.GetSlackEntitlement().GetCPU(), float64(20))
}

func (s *EntitlementCalculatorTestSuite) TestZeroSlackEntitlement() {
	// No demand or allocation from revocable tasks should yeild zero slack entitlement
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(s.mockCtrl)
	gomock.InOrder(
		mockHostMgr.EXPECT().
			ClusterCapacity(
				gomock.Any(),
				gomock.Any()).
			Return(&hostsvc.ClusterCapacityResponse{
				PhysicalResources:      s.createClusterCapacity(),
				PhysicalSlackResources: s.createSlackClusterCapacity(),
			}, nil).
			AnyTimes(),
	)
	s.calculator.capMgr = &v0CapacityManager{
		hostManagerV0: mockHostMgr,
	}

	// resource type: reservation, limit
	// CPU: 100, 1000 MEMORY: 100, 1000 DISK: 1000, 1000, GPU: 2, 4
	//
	// Cluster Capacity
	// CPU: 100, rCPU: 80, MEMORY: 1000, DISK: 6000, GPU: 0
	resPool11, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool11"})
	s.NoError(err)

	// Add allocation and demand for non-revocable tasks
	alloc := scalar.NewAllocation()
	alloc.Value[scalar.NonSlackAllocation] = &scalar.Resources{
		CPU:    100,
		MEMORY: 200,
		DISK:   200,
		GPU:    0,
	}
	alloc.Value[scalar.NonPreemptibleAllocation] = &scalar.Resources{
		CPU:    50,
		MEMORY: 20,
		DISK:   100,
		GPU:    0,
	}
	resPool11.AddToAllocation(alloc)

	resPool11.AddToDemand(&scalar.Resources{
		CPU:    10,
		MEMORY: 100,
		DISK:   100,
	})

	s.calculator.calculateEntitlement(context.Background())
	s.Equal(resPool11.GetSlackEntitlement().GetMem(), float64(0))
	s.Equal(resPool11.GetSlackEntitlement().GetGPU(), float64(0))
	s.Equal(resPool11.GetSlackEntitlement().GetDisk(), float64(0))
}

func (s *EntitlementCalculatorTestSuite) TestSlackEntitlementReduces() {
	// Mock LaunchTasks call.
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(s.mockCtrl)
	gomock.InOrder(
		mockHostMgr.EXPECT().
			ClusterCapacity(
				gomock.Any(),
				gomock.Any()).
			Return(&hostsvc.ClusterCapacityResponse{
				PhysicalResources:      s.createClusterCapacity(),
				PhysicalSlackResources: s.createSlackClusterCapacity(),
			}, nil).
			AnyTimes(),
	)
	s.calculator.capMgr = &v0CapacityManager{
		hostManagerV0: mockHostMgr,
	}

	// resource type: reservation, limit
	// CPU: 100, 1000 MEMORY: 100, 1000 DISK: 1000, 1000, GPU: 2, 4
	//
	// Cluster Capacity
	// CPU: 100, rCPU: 80, MEMORY: 1000, DISK: 6000, GPU: 0
	resPool11, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool11"})
	s.NoError(err)

	// 1) slack_allocation + slack_demand < slack_entitlement
	// 2) non_slack_allocation + non_slack_demand < non_slack_entitlement
	// 3) increase the non-slack demand too much
	// 4) Calculate entitlement, non_slack entitlement increases and slack_entitlement decreases
	alloc := scalar.NewAllocation()
	alloc.Value[scalar.TotalAllocation] = &scalar.Resources{
		CPU:    100,
		MEMORY: 200,
		DISK:   200,
		GPU:    0,
	}
	alloc.Value[scalar.NonSlackAllocation] = &scalar.Resources{
		CPU:    50,
		MEMORY: 100,
		DISK:   100,
		GPU:    0,
	}
	alloc.Value[scalar.SlackAllocation] = &scalar.Resources{
		CPU:    50,
		MEMORY: 100,
		DISK:   100,
		GPU:    0,
	}
	resPool11.AddToAllocation(alloc)

	resPool11.SetEntitlement(
		&scalar.Resources{
			CPU:    100,
			MEMORY: 1000,
			DISK:   1000,
			GPU:    4,
		})
	resPool11.SetNonSlackEntitlement(
		&scalar.Resources{
			CPU:    200,
			MEMORY: 980,
			DISK:   900,
			GPU:    4,
		})
	resPool11.SetSlackEntitlement(
		&scalar.Resources{
			CPU:    80, // revocable cpus are counted separate
			MEMORY: 20,
			DISK:   100,
			GPU:    0,
		})

	// Add non_slack_demand very high, which should reduce slack entitlement
	resPool11.AddToDemand(&scalar.Resources{
		CPU:    60,
		MEMORY: 890, // cluster capacity: 1000, non_slack_allocation: 100, slack_allocation: 100
		DISK:   100,
		GPU:    0,
	})

	s.calculator.calculateEntitlement(context.Background())
	log.Info(resPool11.GetEntitlement())
	log.Info(resPool11.GetNonSlackEntitlement())
	log.Info(resPool11.GetSlackEntitlement())

	// slack_limit: MEMORY: 20, DISK: 200
	// MEMORY: before: 20, after: 10
	s.Equal(resPool11.GetSlackEntitlement().GetMem(), float64(10))
	// DISK: before: 100, after: 100 -> Min (slack_limit, slack_available)
	s.Equal(resPool11.GetSlackEntitlement().GetDisk(), float64(100))

	// MEMORY: before: 980, after 990
	s.Equal(resPool11.GetEntitlement().GetMem()-resPool11.GetSlackEntitlement().GetMem(), float64(990))
	s.Equal(resPool11.GetNonSlackEntitlement().GetMem(), float64(990))
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
	s.calculator.capMgr = &v0CapacityManager{
		hostManagerV0: mockHostMgr,
	}

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

func (s *EntitlementCalculatorTestSuite) TestUpdateCapacityWithHostPool() {
	// Mock LaunchTasks call.
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(s.mockCtrl)

	mockHostMgr.EXPECT().ClusterCapacity(gomock.Any(), gomock.Any()).
		Return(&hostsvc.ClusterCapacityResponse{
			PhysicalResources: s.createClusterCapacity(),
		}, nil).
		AnyTimes()

	s.calculator.capMgr = &v0CapacityManager{
		hostManagerV0: mockHostMgr,
	}
	s.calculator.useHostPool = true

	rootres, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "root"})
	s.NoError(err)
	rootres.SetResourcePoolConfig(s.getResPools()["root"])

	testcases := map[string]struct {
		err  error
		resp *hostsvc.GetHostPoolCapacityResponse
	}{
		"unimplemented-error": {
			err: yarpcerrors.UnimplementedErrorf(""),
		},
		"other-error": {
			err: yarpcerrors.NotFoundErrorf(""),
		},
		"success": {
			resp: &hostsvc.GetHostPoolCapacityResponse{
				Pools: []*hostsvc.HostPoolResources{
					{
						PoolName:         "p1",
						PhysicalCapacity: s.createClusterCapacity(),
						SlackCapacity:    s.createSlackClusterCapacity(),
					},
					{
						PoolName:         "p2",
						PhysicalCapacity: s.createClusterCapacity(),
					},
				},
			},
		},
	}
	for tcName, tc := range testcases {
		mockHostMgr.EXPECT().GetHostPoolCapacity(gomock.Any(), gomock.Any()).
			Return(tc.resp, tc.err)

		err := s.calculator.updateClusterCapacity(context.Background(), rootres)

		if tc.err != nil {
			if yarpcerrors.IsUnimplemented(err) {
				s.NoError(err, tcName)
				s.Equal(0, len(s.calculator.hostPoolCapacity), tcName)
			} else {
				s.Error(err, tcName)
			}
		} else {
			s.NoError(err)
			s.Contains(s.calculator.hostPoolCapacity, "p1", tcName)
			s.Contains(s.calculator.hostPoolCapacity, "p2", tcName)
		}
	}
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
				PhysicalResources:      s.createClusterCapacity(),
				PhysicalSlackResources: s.createSlackClusterCapacity(),
			}, nil).
			AnyTimes(),
	)
	s.calculator.capMgr = &v0CapacityManager{
		hostManagerV0: mockHostMgr,
	}

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
	t := rpc.NewTransport()
	outbound := t.NewOutbound(nil)
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     common.PelotonResourceManager,
		Inbounds: nil,
		Outbounds: yarpc.Outbounds{
			common.PelotonHostManager: transport.Outbounds{
				Unary: outbound,
			},
		},
		Metrics: yarpc.MetricsConfig{
			Tally: tally.NoopScope,
		},
	})

	calc := NewCalculator(
		10*time.Millisecond,
		tally.NoopScope,
		dispatcher,
		s.resTree,
		api.V0,
		false,
	)
	s.NotNil(calc)
	calc = NewCalculator(
		10*time.Millisecond,
		tally.NoopScope,
		dispatcher,
		s.resTree,
		api.V1Alpha,
		false,
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
		AnyTimes()
	s.calculator.capMgr = &v0CapacityManager{
		hostManagerV0: mockHostMgr,
	}

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
	s.calculator.capMgr = &v0CapacityManager{
		hostManagerV0: mockHostMgr,
	}
	err := s.calculator.calculateEntitlement(context.Background())
	s.Error(err)
}

// Testing the Static respools and their entitlement
func (s *EntitlementCalculatorTestSuite) TestStaticRespoolsEntitlement() {
	// Building Local Tree for this test suite
	mockCtrl := gomock.NewController(s.T())
	mockHostMgr := host_mocks.NewMockInternalHostServiceYARPCClient(mockCtrl)
	mockResPoolOps := objectmocks.NewMockResPoolOps(s.mockCtrl)
	gomock.InOrder(
		mockResPoolOps.EXPECT().
			GetAll(context.Background()).Return(s.getStaticResPools(), nil).AnyTimes(),
	)
	mockJobStore := store_mocks.NewMockJobStore(s.mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(s.mockCtrl)

	resTree := respool.NewTree(tally.NoopScope, mockResPoolOps, mockJobStore,
		mockTaskStore, res_common.PreemptionConfig{Enabled: false})

	// Creating local Calculator object
	calculator := &Calculator{
		resPoolTree:          resTree,
		runningState:         res_common.RunningStateNotStarted,
		calculationPeriod:    10 * time.Millisecond,
		stopChan:             make(chan struct{}, 1),
		clusterCapacity:      make(map[string]float64),
		clusterSlackCapacity: make(map[string]float64),
		capMgr: &v0CapacityManager{
			hostManagerV0: mockHostMgr,
		},
		metrics: newMetrics(tally.NoopScope),
	}

	resTree.Start()

	// Mock LaunchTasks call.
	gomock.InOrder(
		mockHostMgr.EXPECT().
			ClusterCapacity(
				gomock.Any(),
				gomock.Any()).
			Return(&hostsvc.ClusterCapacityResponse{
				PhysicalResources:      s.createClusterCapacity(),
				PhysicalSlackResources: s.createSlackClusterCapacity(),
			}, nil).
			AnyTimes(),
	)
	s.calculator.capMgr = &v0CapacityManager{
		hostManagerV0: mockHostMgr,
	}

	resPool, err := resTree.Get(&peloton.ResourcePoolID{Value: "respool11s"})
	s.NoError(err)
	demand := &scalar.Resources{
		CPU:    20,
		MEMORY: 200,
		DISK:   2000,
		GPU:    0,
	}
	resPool.AddToDemand(demand)

	demand = &scalar.Resources{
		CPU:    20,
		MEMORY: 10,
		DISK:   100,
		GPU:    0,
	}
	resPool.AddToSlackDemand(demand)

	calculator.calculateEntitlement(context.Background())
	res := resPool.GetEntitlement()
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 28, "GPU": 1, "MEMORY": 291, "DISK": 1000}))

	res = resPool.GetSlackEntitlement()

	// Out of 80 revocable cpus, 20 are to satisfy existing demand and
	// remaining 10 are allocated part of unclaimed resources
	s.True(tasktestutil.ValidateResources(res,
		map[string]int64{"CPU": 30, "GPU": 0, "MEMORY": 10, "DISK": 0}))

	err = resTree.Stop()
	s.NoError(err)
	mockCtrl.Finish()
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

// createSlackClusterCapacity creates the cluster capacity for revocable resources.
func (s *EntitlementCalculatorTestSuite) createSlackClusterCapacity() []*hostsvc.Resource {
	return []*hostsvc.Resource{
		{
			Kind:     common.CPU,
			Capacity: 80,
		},
		{
			Kind:     common.GPU,
			Capacity: 0,
		},
		{
			Kind:     common.MEMORY,
			Capacity: 0,
		},
		{
			Kind:     common.DISK,
			Capacity: 0,
		},
	}
}

// convertV0ToV1HostResource converts v0 hostsvc Resource to v1 Resource
func convertV0ToV1HostResource(
	v0Res []*hostsvc.Resource,
) []*hostmgr.Resource {
	var v1Res []*hostmgr.Resource
	for _, r := range v0Res {
		v1Res = append(v1Res, &hostmgr.Resource{
			Kind:     r.GetKind(),
			Capacity: r.GetCapacity(),
		})
	}
	return v1Res
}
