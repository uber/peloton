package respool

import (
	"fmt"
	"testing"

	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"peloton/api/peloton"
	pb_respool "peloton/api/respool"
	"peloton/private/resmgr"
)

type resTreeTestSuite struct {
	suite.Suite
	resourceTree Tree
	mockCtrl     *gomock.Controller
}

func (suite *resTreeTestSuite) SetupSuite() {
	fmt.Println("setting up resTreeTestSuite")
	suite.mockCtrl = gomock.NewController(suite.T())
	mockResPoolStore := store_mocks.NewMockResourcePoolStore(suite.mockCtrl)
	mockResPoolStore.EXPECT().GetAllResourcePools().
		Return(suite.getResPools(), nil).AnyTimes()
	InitTree(tally.NoopScope, mockResPoolStore)
	suite.resourceTree = GetTree()
}

func (suite *resTreeTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
}

func (suite *resTreeTestSuite) SetupTest() {
	err := suite.resourceTree.Start()
	suite.NoError(err)
}

func (suite *resTreeTestSuite) TearDownTest() {
	err := suite.resourceTree.Stop()
	suite.NoError(err)
}

// Returns resource configs
func (suite *resTreeTestSuite) getResourceConfig() []*pb_respool.ResourceConfig {

	resConfigs := []*pb_respool.ResourceConfig{
		{
			Share:       1,
			Kind:        "cpu",
			Reservation: 100,
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
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "gpu",
			Reservation: 2,
			Limit:       4,
		},
	}
	return resConfigs
}

// Returns resource pools
func (suite *resTreeTestSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {

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
		"respool23": {
			Name:   "respool23",
			Parent: &pb_respool.ResourcePoolID{Value: "respool22"},
			Resources: []*pb_respool.ResourceConfig{
				{
					Kind:        "cpu",
					Reservation: 50,
					Limit:       100,
					Share:       1,
				},
			},
			Policy: policy,
		},
		"respool99": {
			Name:   "respool99",
			Parent: &pb_respool.ResourcePoolID{Value: "respool21"},
			Resources: []*pb_respool.ResourceConfig{
				{
					Kind:        "cpu",
					Reservation: 50,
					Limit:       100,
					Share:       1,
				},
			},
			Policy: policy,
		},
	}
}

func TestPelotonResPool(t *testing.T) {
	suite.Run(t, new(resTreeTestSuite))
}

func (suite *resTreeTestSuite) TestPrintTree() {
	// TODO: serialize the tree and compare it
	rt, ok := suite.resourceTree.(*tree)
	suite.Equal(true, ok)
	rt.printTree(rt.root)
}

func (suite *resTreeTestSuite) TestGetChildren() {
	rt, ok := suite.resourceTree.(*tree)
	suite.Equal(true, ok)
	list := rt.root.GetChildren()
	suite.Equal(list.Len(), 3)
	n := rt.allNodes["respool1"]
	list = n.GetChildren()
	suite.Equal(list.Len(), 2)
	n = rt.allNodes["respool2"]
	list = n.GetChildren()
	suite.Equal(list.Len(), 2)
}

func (suite *resTreeTestSuite) TestResourceConfig() {
	rt, ok := suite.resourceTree.(*tree)
	suite.Equal(true, ok)
	n := rt.allNodes["respool1"]
	suite.Equal(n.ID, "respool1")
	for _, res := range n.resourceConfigs {
		if res.Kind == "cpu" {
			assert.Equal(suite.T(), res.Reservation, 100.00, "Reservation is not Equal")
			assert.Equal(suite.T(), res.Limit, 1000.00, "Limit is not equal")
		}
		if res.Kind == "memory" {
			assert.Equal(suite.T(), res.Reservation, 100.00, "Reservation is not Equal")
			assert.Equal(suite.T(), res.Limit, 1000.00, "Limit is not equal")
		}
		if res.Kind == "disk" {
			assert.Equal(suite.T(), res.Reservation, 100.00, "Reservation is not Equal")
			assert.Equal(suite.T(), res.Limit, 1000.00, "Limit is not equal")
		}
		if res.Kind == "gpu" {
			assert.Equal(suite.T(), res.Reservation, 2.00, "Reservation is not Equal")
			assert.Equal(suite.T(), res.Limit, 4.00, "Limit is not equal")
		}
	}
}

func (suite *resTreeTestSuite) TestPendingQueue() {
	rt, ok := suite.resourceTree.(*tree)
	suite.Equal(true, ok)
	// Task -1
	jobID1 := &peloton.JobID{
		Value: "job1",
	}
	taskID1 := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", jobID1.Value, 1),
	}
	taskItem1 := &resmgr.Task{
		Name:     "job1-1",
		Priority: 0,
		JobId:    jobID1,
		Id:       taskID1,
	}
	rt.allNodes["respool11"].EnqueueTask(taskItem1)

	// Task -2
	jobID2 := &peloton.JobID{
		Value: "job1",
	}
	taskID2 := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", jobID2.Value, 2),
	}
	taskItem2 := &resmgr.Task{
		Name:     "job1-2",
		Priority: 0,
		JobId:    jobID2,
		Id:       taskID2,
	}
	rt.allNodes["respool11"].EnqueueTask(taskItem2)

	res, err := rt.allNodes["respool11"].DequeueTasks(1)
	if err != nil {
		assert.Fail(suite.T(), "Dequeue should not fail")
	}
	t1 := res.Front().Value.(*resmgr.Task)
	assert.Equal(suite.T(), t1.JobId.Value, "job1", "Should get Job-1")
	assert.Equal(suite.T(), t1.Id.GetValue(), "job1-1", "Should get Job-1 and Task-1")

	res2, err2 := rt.allNodes["respool11"].DequeueTasks(1)
	t2 := res2.Front().Value.(*resmgr.Task)
	if err2 != nil {
		assert.Fail(suite.T(), "Dequeue should not fail")
	}

	assert.Equal(suite.T(), t2.JobId.Value, "job1", "Should get Job-1")
	assert.Equal(suite.T(), t2.Id.GetValue(), "job1-2", "Should get Job-1 and Task-1")
}

func (suite *resTreeTestSuite) TestTree_UpsertExistingResourcePoolConfig() {
	mockExistingResourcePoolID := &pb_respool.ResourcePoolID{
		Value: "respool23",
	}

	mockParentPoolID := &pb_respool.ResourcePoolID{
		Value: "respool22",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 10,
				Kind:        "cpu",
				Limit:       50,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockParentPoolID.Value,
	}

	err := suite.resourceTree.Upsert(mockExistingResourcePoolID, mockResourcePoolConfig)
	suite.NoError(err)
}

func (suite *resTreeTestSuite) TestTree_UpsertRootResourcePoolConfig() {
	mockExistingResourcePoolID := &pb_respool.ResourcePoolID{
		Value: RootResPoolID,
	}

	mockParentPoolID := &pb_respool.ResourcePoolID{
		Value: "respool22",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 10,
				Kind:        "cpu",
				Limit:       50,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockParentPoolID.Value,
	}

	err := suite.resourceTree.Upsert(mockExistingResourcePoolID, mockResourcePoolConfig)
	suite.EqualError(err, fmt.Sprintf("Cannot override %s", RootResPoolID))
}

func (suite *resTreeTestSuite) TestTree_UpsertNewResourcePoolConfig() {
	mockExistingResourcePoolID := &pb_respool.ResourcePoolID{
		Value: "respool24",
	}

	mockParentPoolID := &pb_respool.ResourcePoolID{
		Value: "respool23",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 10,
				Kind:        "cpu",
				Limit:       50,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockParentPoolID.Value,
	}

	err := suite.resourceTree.Upsert(mockExistingResourcePoolID, mockResourcePoolConfig)
	suite.NoError(err)
}

func (suite *resTreeTestSuite) TestTree_UpsertValidateReservationsExceedLimit() {
	mockResourcePoolID := &pb_respool.ResourcePoolID{Value: "respool33"}
	mockParentPoolID := &pb_respool.ResourcePoolID{Value: "respool11"}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 50,
				Kind:        "cpu",
				Limit:       10,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockResourcePoolID.Value,
	}

	err := suite.resourceTree.Upsert(mockResourcePoolID, mockResourcePoolConfig)
	suite.EqualError(err, "resource cpu, reservation 50 exceeds limit 10")
}

func (suite *resTreeTestSuite) TestTree_UpsertValidateCycle() {
	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: "respool33",
	}
	mockParentPoolID := &pb_respool.ResourcePoolID{
		Value: "respool33",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 50,
				Kind:        "cpu",
				Limit:       100,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockResourcePoolID.Value,
	}

	err := suite.resourceTree.Upsert(mockResourcePoolID, mockResourcePoolConfig)
	suite.EqualError(err, "resource pool ID: respool33 and parent ID: respool33 cannot be same")
}

func (suite *resTreeTestSuite) TestTree_UpsertValidateParentLookupError() {
	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: "respool33",
	}
	mockParentPoolID := &pb_respool.ResourcePoolID{
		Value: "i_do_not_exist",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 50,
				Kind:        "cpu",
				Limit:       100,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockResourcePoolID.Value,
	}

	err := suite.resourceTree.Upsert(mockResourcePoolID, mockResourcePoolConfig)
	suite.EqualError(err, "Resource pool (i_do_not_exist) not found")
}

func (suite *resTreeTestSuite) TestTree_UpsertValidateParentChanged() {
	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: "respool1",
	}
	mockChangedParentPoolID := &pb_respool.ResourcePoolID{
		Value: "respool2",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockChangedParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 50,
				Kind:        "cpu",
				Limit:       100,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockResourcePoolID.Value,
	}

	err := suite.resourceTree.Upsert(mockResourcePoolID, mockResourcePoolConfig)
	suite.EqualError(err, "parent override not allowed, actual root, override respool2")
}

func (suite *resTreeTestSuite) TestTree_UpsertValidateParentExceedLimit() {
	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: "respool33",
	}
	mockParentPoolID := &pb_respool.ResourcePoolID{
		Value: "respool11",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 50,
				Kind:        "cpu",
				Limit:       99999,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockResourcePoolID.Value,
	}

	err := suite.resourceTree.Upsert(mockResourcePoolID, mockResourcePoolConfig)
	suite.EqualError(err, "resource cpu, limit 99999 exceeds parent limit 1")
}

func (suite *resTreeTestSuite) TestTree_UpsertValidateInvalidResourceKind() {
	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: "respool33",
	}
	mockParentPoolID := &pb_respool.ResourcePoolID{
		Value: "respool11",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 50,
				Kind:        "aaa",
				Limit:       99999,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockResourcePoolID.Value,
	}

	err := suite.resourceTree.Upsert(mockResourcePoolID, mockResourcePoolConfig)
	suite.EqualError(err, "parent respool11 doesn't have resource kind aaa")
}

func (suite *resTreeTestSuite) TestTree_UpsertValidateChildrenReservationsError() {
	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: "respool34",
	}
	mockParentPoolID := &pb_respool.ResourcePoolID{
		Value: "respool21",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 51,
				Kind:        "cpu",
				Limit:       100,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockParentPoolID.Value,
	}

	err := suite.resourceTree.Upsert(mockResourcePoolID, mockResourcePoolConfig)
	suite.EqualError(
		err,
		"Aggregated child reservation 101 of kind `cpu` exceed parent `respool21` reservations 100",
	)
}
