package respool

import (
	"context"
	"testing"

	pb_respool "peloton/api/respool"

	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type resPoolHandlerTestSuite struct {
	suite.Suite

	context          context.Context
	resourceTree     Tree
	mockCtrl         *gomock.Controller
	handler          *serviceHandler
	mockResPoolStore *store_mocks.MockResourcePoolStore
}

func (suite *resPoolHandlerTestSuite) SetupTest() {
	suite.context = context.Background()
	suite.mockCtrl = gomock.NewController(suite.T())

	// mock resource pool store
	suite.mockResPoolStore = store_mocks.NewMockResourcePoolStore(suite.mockCtrl)

	if suite.resourceTree == nil {
		// set expectations
		suite.mockResPoolStore.EXPECT().GetAllResourcePools().Return(
			suite.getResPools(),
			nil)

		InitTree(tally.NoopScope, suite.mockResPoolStore)
		suite.resourceTree = GetTree()
		suite.resourceTree.Start()
	}

	suite.handler = &serviceHandler{
		resPoolTree:  suite.resourceTree,
		dispatcher:   nil,
		metrics:      NewMetrics(tally.NoopScope),
		store:        suite.mockResPoolStore,
		runningState: runningStateRunning,
	}
}

func (suite *resPoolHandlerTestSuite) TearDownTest() {
	defer suite.mockCtrl.Finish()
	log.Info("tearing down")
	err := suite.resourceTree.Stop()
	suite.NoError(err)
	err = suite.handler.Stop()
	suite.NoError(err)
}

func TestResPoolHandler(t *testing.T) {
	suite.Run(t, new(resPoolHandlerTestSuite))
}

// Returns resource configs
func (suite *resPoolHandlerTestSuite) getResourceConfig() []*pb_respool.ResourceConfig {

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
func (suite *resPoolHandlerTestSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {

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

func (suite *resPoolHandlerTestSuite) TestServiceHandler_GetResourcePoolLeafNode() {
	log.Info("TestServiceHandler_GetResourcePoolLeafNode called")

	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: "respool22",
	}

	// form request
	getReq := &pb_respool.GetRequest{
		Id: mockResourcePoolID,
	}

	getResp, _, err := suite.handler.GetResourcePool(
		suite.context,
		nil,
		getReq,
	)

	suite.NoError(err)
	suite.NotNil(getResp)
	suite.Equal(mockResourcePoolID.Value, getResp.Poolinfo.Id.Value)
	suite.Len(getResp.Poolinfo.Children, 0)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_GetResourcePoolWithChildNodes() {
	log.Info("TestServiceHandler_GetResourcePoolWithChildNodes called")

	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: "respool2",
	}

	// form request
	getReq := &pb_respool.GetRequest{
		Id: mockResourcePoolID,
	}

	getResp, _, err := suite.handler.GetResourcePool(
		suite.context,
		nil,
		getReq,
	)

	suite.NoError(err)
	suite.NotNil(getResp)
	suite.Equal(mockResourcePoolID.Value, getResp.Poolinfo.Id.Value)
	suite.Len(getResp.Poolinfo.Children, 2)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_GetResourcePoolLookupError() {
	log.Info("TestServiceHandler_GetResourcePoolLookupError called")

	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: "non_exist",
	}

	// form request
	getReq := &pb_respool.GetRequest{
		Id: mockResourcePoolID,
	}

	getResp, _, err := suite.handler.GetResourcePool(
		suite.context,
		nil,
		getReq,
	)

	suite.NoError(err)
	suite.NotNil(getResp)
	suite.NotNil(getResp.Error)

	expectedMsg := "resource pool not found"
	suite.Equal(expectedMsg, getResp.Error.NotFound.Message)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_CreateResourcePool() {
	log.Info("TestServiceHandler_CreateResourcePool called")

	mockResourcePoolName := "respool99"

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Name:      mockResourcePoolName,
		Parent:    &pb_respool.ResourcePoolID{Value: "respool2"},
		Resources: suite.getResourceConfig(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}
	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: mockResourcePoolName,
	}

	// create request
	createReq := &pb_respool.CreateRequest{
		Id:     mockResourcePoolID,
		Config: mockResourcePoolConfig,
	}

	// set expectations
	suite.mockResPoolStore.EXPECT().CreateResourcePool(
		gomock.Eq(mockResourcePoolID),
		gomock.Eq(mockResourcePoolConfig),
		"peloton").Return(nil)

	createResp, _, err := suite.handler.CreateResourcePool(
		suite.context,
		nil,
		createReq)

	suite.NoError(err)
	suite.NotNil(createResp)
	suite.Nil(createResp.Error)
	suite.Equal(mockResourcePoolID.Value, createResp.Result.Value)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_CreateResourcePoolAlreadyExistsError() {
	log.Info("TestServiceHandler_CreateResourcePool called")

	mockResourcePoolName := "respool99"

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Name:      mockResourcePoolName,
		Parent:    &pb_respool.ResourcePoolID{Value: "respool2"},
		Resources: suite.getResourceConfig(),
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
	}
	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: mockResourcePoolName,
	}

	// create request
	createReq := &pb_respool.CreateRequest{
		Id:     mockResourcePoolID,
		Config: mockResourcePoolConfig,
	}

	expectedErrMsg := "resource pool already exits"
	// set expectations
	suite.mockResPoolStore.EXPECT().CreateResourcePool(
		gomock.Eq(mockResourcePoolID),
		gomock.Eq(mockResourcePoolConfig),
		"peloton",
	).Return(errors.New(expectedErrMsg))

	createResp, _, err := suite.handler.CreateResourcePool(
		suite.context,
		nil,
		createReq)

	actualErrResourcePoolID := createResp.Error.AlreadyExists.Id.Value
	suite.NoError(err)
	suite.NotNil(createResp)
	suite.Nil(createResp.Result)
	suite.NotNil(createResp.Error)
	suite.Equal(expectedErrMsg, createResp.Error.AlreadyExists.Message)
	suite.Equal(mockResourcePoolID.Value, actualErrResourcePoolID)
}
