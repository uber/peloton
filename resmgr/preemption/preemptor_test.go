package preemption

import (
	"container/list"
	"reflect"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/common/queue"
	res_common "code.uber.internal/infra/peloton/resmgr/common"
	"code.uber.internal/infra/peloton/resmgr/respool/mocks"
	"code.uber.internal/infra/peloton/resmgr/scalar"
	rm_task "code.uber.internal/infra/peloton/resmgr/task"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type PreemptorTestSuite struct {
	suite.Suite
	mockCtrl *gomock.Controller

	preemptor preemptor
}

func (suite *PreemptorTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())

	rm_task.InitTaskTracker(tally.NoopScope)
	suite.preemptor = preemptor{
		resTree:                      nil,
		runningState:                 res_common.RunningStateNotStarted,
		preemptionPeriod:             1 * time.Second,
		sustainedOverAllocationCount: 5,
		stopChan:                     make(chan struct{}, 1),
		preemptionQueue: queue.NewQueue(
			"preemption-queue",
			reflect.TypeOf(&rm_task.RMTask{}),
			10000,
		),
		respoolState: make(map[string]int),
		ranker:       newStatePriorityRuntimeRanker(rm_task.GetTracker()),
	}
}

func TestPreemptor(t *testing.T) {
	suite.Run(t, new(PreemptorTestSuite))
}

func (suite *PreemptorTestSuite) TestUpdateResourcePoolsState() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	mockResPool.EXPECT().GetEntitlement().Return(&scalar.Resources{
		CPU:    20,
		MEMORY: 200,
		DISK:   2000,
		GPU:    0,
	}).AnyTimes()
	mockResPool.EXPECT().GetAllocation().Return(&scalar.Resources{
		CPU:    20,
		MEMORY: 200,
		DISK:   2000,
		GPU:    0,
	}).AnyTimes()

	l := list.New()
	l.PushBack(mockResPool)
	mockResTree.EXPECT().GetAllNodes(true).Return(
		l,
	).AnyTimes()

	suite.preemptor.resTree = mockResTree
	suite.preemptor.sustainedOverAllocationCount = 5
	for i := 0; i < 6; i++ {
		suite.preemptor.updateResourcePoolsState()
	}

	respools := suite.preemptor.getEligibleResPoolsForPreemption()
	suite.Equal(1, len(respools))
	suite.Equal("respool-1", respools[0])
}

func (suite *PreemptorTestSuite) TestUpdateResourcePoolsState_Reset() {
	mockResTree := mocks.NewMockTree(suite.mockCtrl)
	mockResPool := mocks.NewMockResPool(suite.mockCtrl)

	// Mocks
	mockResPool.EXPECT().ID().Return("respool-1").AnyTimes()
	mockResPool.EXPECT().GetEntitlement().Return(&scalar.Resources{
		CPU:    20,
		MEMORY: 200,
		DISK:   2000,
		GPU:    0,
	}).AnyTimes()

	// mock allocation going down on compared to the entitlement once
	gomock.InOrder(
		mockResPool.EXPECT().GetAllocation().Return(&scalar.Resources{
			CPU:    20,
			MEMORY: 200,
			DISK:   2000,
			GPU:    0,
		}), mockResPool.EXPECT().GetAllocation().Return(&scalar.Resources{
			CPU:    20,
			MEMORY: 200,
			DISK:   2000,
			GPU:    0,
		}), mockResPool.EXPECT().GetAllocation().Return(&scalar.Resources{
			CPU:    20,
			MEMORY: 200,
			DISK:   2000,
			GPU:    0,
		}), mockResPool.EXPECT().GetAllocation().Return(&scalar.Resources{
			CPU:    20,
			MEMORY: 200,
			DISK:   2000,
			GPU:    0,
		}),
		mockResPool.EXPECT().GetAllocation().Return(&scalar.Resources{
			CPU:    10,
			MEMORY: 100,
			DISK:   1000,
			GPU:    0,
		}),
	)

	mockResPools := list.New()
	mockResPools.PushBack(mockResPool)
	mockResTree.EXPECT().GetAllNodes(true).Return(
		mockResPools,
	).AnyTimes()

	suite.preemptor.resTree = mockResTree
	suite.preemptor.sustainedOverAllocationCount = 5

	// run it 5 times
	for i := 0; i < 5; i++ {
		suite.preemptor.updateResourcePoolsState()
	}

	// no respools should be added since allocation becomes less than entitlement once
	respools := suite.preemptor.getEligibleResPoolsForPreemption()
	suite.Equal(0, len(respools))
}

func (suite *PreemptorTestSuite) TestReconciler_Start() {
	defer suite.preemptor.Stop()
	err := suite.preemptor.Start()
	suite.NoError(err)
	suite.Equal(suite.preemptor.runningState, int32(res_common.RunningStateRunning))
}
