package respool

import (
	"context"
	"testing"

	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	pb_respool "peloton/api/respool"
)

type resPoolHandlerTestSuite struct {
	suite.Suite

	context                     context.Context
	resourceTree                Tree
	mockCtrl                    *gomock.Controller
	handler                     *serviceHandler
	mockResPoolStore            *store_mocks.MockResourcePoolStore
	resourcePoolConfigValidator Validator
}

func (suite *resPoolHandlerTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockResPoolStore = store_mocks.NewMockResourcePoolStore(suite.mockCtrl)
	suite.mockResPoolStore.EXPECT().GetAllResourcePools().
		Return(suite.getResPools(), nil).AnyTimes()
	suite.resourceTree = &tree{
		store:    suite.mockResPoolStore,
		root:     nil,
		metrics:  NewMetrics(tally.NoopScope),
		allNodes: make(map[string]*ResPool),
	}
	resourcePoolConfigValidator, err := NewResourcePoolConfigValidator(suite.resourceTree)
	suite.NoError(err)
	suite.resourcePoolConfigValidator = resourcePoolConfigValidator
}

func (suite *resPoolHandlerTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
}

func (suite *resPoolHandlerTestSuite) SetupTest() {
	suite.context = context.Background()
	err := suite.resourceTree.Start()
	suite.NoError(err)

	suite.handler = &serviceHandler{
		resPoolTree:            suite.resourceTree,
		dispatcher:             nil,
		metrics:                NewMetrics(tally.NoopScope),
		store:                  suite.mockResPoolStore,
		runningState:           runningStateRunning,
		resPoolConfigValidator: suite.resourcePoolConfigValidator,
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
	}
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_GetResourcePoolLeafNode() {
	log.Info("TestServiceHandler_GetResourcePoolLeafNode called")

	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: "respool21",
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
		Name:   mockResourcePoolName,
		Parent: &pb_respool.ResourcePoolID{Value: "respool23"},
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 1,
				Limit:       1,
				Share:       1,
				Kind:        "cpu",
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
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

func (suite *resPoolHandlerTestSuite) TestServiceHandler_CreateResourcePoolValidationError() {
	log.Info("TestServiceHandler_CreateResourcePoolValidationError called")

	mockResourcePoolName := "respool101"

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Name:   mockResourcePoolName,
		Parent: &pb_respool.ResourcePoolID{Value: "respool23"},
		Resources: []*pb_respool.ResourceConfig{
			{
				// reservation exceed limit,  should fail
				Reservation: 5,
				Limit:       1,
				Share:       1,
				Kind:        "cpu",
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
	}
	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: mockResourcePoolName,
	}

	// create request
	createReq := &pb_respool.CreateRequest{
		Id:     mockResourcePoolID,
		Config: mockResourcePoolConfig,
	}

	createResp, _, err := suite.handler.CreateResourcePool(
		suite.context,
		nil,
		createReq)

	suite.NoError(err)
	suite.NotNil(createResp)
	suite.NotNil(createResp.Error)
	expectedMsg := "resource cpu, reservation 5 exceeds limit 1"
	suite.Equal(expectedMsg, createResp.Error.InvalidResourcePoolConfig.Message)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_CreateResourcePoolAlreadyExistsError() {
	log.Info("TestServiceHandler_CreateResourcePoolAlreadyExistsError called")

	mockResourcePoolName := "respool99"

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Name:   mockResourcePoolName,
		Parent: &pb_respool.ResourcePoolID{Value: "respool23"},
		Resources: []*pb_respool.ResourceConfig{
			{
				Kind:        "cpu",
				Share:       1,
				Limit:       1,
				Reservation: 1,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
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

func (suite *resPoolHandlerTestSuite) TestServiceHandler_UpdateResourcePool() {
	log.Info("TestServiceHandler_UpdateResourcePool called")

	mockResourcePoolName := "respool23"

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Name:   mockResourcePoolName,
		Parent: &pb_respool.ResourcePoolID{Value: "respool22"},
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 1,
				Limit:       1,
				Share:       1,
				Kind:        "cpu",
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
	}
	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: mockResourcePoolName,
	}

	// update request
	updateReq := &pb_respool.UpdateRequest{
		Id:     mockResourcePoolID,
		Config: mockResourcePoolConfig,
	}

	// set expectations
	suite.mockResPoolStore.EXPECT().UpdateResourcePool(
		gomock.Eq(mockResourcePoolID),
		gomock.Eq(mockResourcePoolConfig)).Return(nil)

	updateResp, _, err := suite.handler.UpdateResourcePool(
		suite.context,
		nil,
		updateReq)

	suite.NoError(err)
	suite.NotNil(updateResp)
	suite.Nil(updateResp.Error)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_UpdateResourcePoolValidationError() {
	log.Info("TestServiceHandler_UpdateResourcePoolValidationError called")

	mockResourcePoolName := "respool22"

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Name:   mockResourcePoolName,
		Parent: &pb_respool.ResourcePoolID{Value: "respool2"},
		Resources: []*pb_respool.ResourceConfig{
			{
				// reservation exceed limit,  should fail
				Reservation: 5,
				Limit:       1,
				Share:       1,
				Kind:        "cpu",
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
	}
	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: mockResourcePoolName,
	}

	// update request
	updateReq := &pb_respool.UpdateRequest{
		Id:     mockResourcePoolID,
		Config: mockResourcePoolConfig,
	}

	updateResp, _, err := suite.handler.UpdateResourcePool(
		suite.context,
		nil,
		updateReq)

	suite.NoError(err)
	suite.NotNil(updateResp)
	suite.NotNil(updateResp.Error)
	expectedMsg := "resource cpu, reservation 5 exceeds limit 1"
	suite.Equal(expectedMsg, updateResp.Error.InvalidResourcePoolConfig.Message)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_UpdateResourcePoolNotExistsError() {
	log.Info("TestServiceHandler_UpdateResourcePoolNotExistsError called")

	mockResourcePoolName := "respool105"

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Name:   mockResourcePoolName,
		Parent: &pb_respool.ResourcePoolID{Value: "respool23"},
		Resources: []*pb_respool.ResourceConfig{
			{
				Kind:        "cpu",
				Share:       1,
				Limit:       1,
				Reservation: 1,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
	}
	mockResourcePoolID := &pb_respool.ResourcePoolID{
		Value: mockResourcePoolName,
	}

	// update request
	updateReq := &pb_respool.UpdateRequest{
		Id:     mockResourcePoolID,
		Config: mockResourcePoolConfig,
	}

	expectedErrMsg := "Resource pool (respool105) not found"

	updateResp, _, err := suite.handler.UpdateResourcePool(
		suite.context,
		nil,
		updateReq)

	actualErrResourcePoolID := updateResp.Error.NotFound.Id.Value
	suite.NoError(err)
	suite.NotNil(updateResp)
	suite.NotNil(updateResp.Error)
	suite.Equal(expectedErrMsg, updateResp.Error.NotFound.Message)
	suite.Equal(mockResourcePoolID.Value, actualErrResourcePoolID)
}
