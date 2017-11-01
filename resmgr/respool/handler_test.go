package respool

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/respool"

	"code.uber.internal/infra/peloton/common"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
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
	suite.mockResPoolStore.EXPECT().GetAllResourcePools(context.Background()).
		Return(suite.getResPools(), nil).AnyTimes()
	mockJobStore := store_mocks.NewMockJobStore(suite.mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(suite.mockCtrl)
	suite.resourceTree = &tree{
		store:     suite.mockResPoolStore,
		root:      nil,
		metrics:   NewMetrics(tally.NoopScope),
		resPools:  make(map[string]ResPool),
		jobStore:  mockJobStore,
		taskStore: mockTaskStore,
		scope:     tally.NoopScope,
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

	rootID := peloton.ResourcePoolID{Value: "root"}
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
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool12": {
			Name:      "respool12",
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool21": {
			Name:      "respool21",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool22": {
			Name:      "respool22",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool23": {
			Name:   "respool23",
			Parent: &peloton.ResourcePoolID{Value: "respool22"},
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

func (suite *resPoolHandlerTestSuite) TestServiceHandler_GetResourcePoolEmptyID() {

	log.Info("TestServiceHandler_GetResourcePoolInvalidID called")

	// form request
	getReq := &pb_respool.GetRequest{}

	getResp, err := suite.handler.GetResourcePool(
		suite.context,
		getReq,
	)

	suite.NoError(err)
	suite.NotNil(getResp)
	suite.Equal(common.RootResPoolID, getResp.Poolinfo.Id.Value)
	suite.Nil(getResp.Poolinfo.Parent)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_GetResourcePoolLeafNode() {
	log.Info("TestServiceHandler_GetResourcePoolLeafNode called")

	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool21",
	}

	// form request
	getReq := &pb_respool.GetRequest{
		Id: mockResourcePoolID,
	}

	getResp, err := suite.handler.GetResourcePool(
		suite.context,
		getReq,
	)

	suite.NoError(err)
	suite.NotNil(getResp)
	suite.Equal(mockResourcePoolID.Value, getResp.Poolinfo.Id.Value)
	suite.Len(getResp.Poolinfo.Children, 0)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_GetResourcePoolWithChildNodes() {
	log.Info("TestServiceHandler_GetResourcePoolWithChildNodes called")

	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool2",
	}

	// form request
	getReq := &pb_respool.GetRequest{
		Id:                mockResourcePoolID,
		IncludeChildPools: true,
	}

	getResp, err := suite.handler.GetResourcePool(
		suite.context,
		getReq,
	)

	suite.NoError(err)
	suite.NotNil(getResp)
	suite.Equal(mockResourcePoolID.Value, getResp.Poolinfo.Id.Value)
	suite.Len(getResp.Poolinfo.Children, 2)
	suite.Len(getResp.ChildPools, 2)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_GetResourcePoolError() {
	log.Info("TestServiceHandler_GetResourcePoolError called")

	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "non_exist",
	}

	// form request
	getReq := &pb_respool.GetRequest{
		Id: mockResourcePoolID,
	}

	getResp, err := suite.handler.GetResourcePool(
		suite.context,
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
		Parent: &peloton.ResourcePoolID{Value: "respool23"},
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

	// create request
	createReq := &pb_respool.CreateRequest{
		Config: mockResourcePoolConfig,
	}

	// set expectations
	suite.mockResPoolStore.EXPECT().CreateResourcePool(
		context.Background(),
		gomock.Any(),
		gomock.Eq(mockResourcePoolConfig),
		"peloton").Return(nil)

	createResp, err := suite.handler.CreateResourcePool(
		suite.context,
		createReq)

	suite.NoError(err)
	suite.NotNil(createResp)
	suite.Nil(createResp.Error)
	suite.NotNil(uuid.Parse(createResp.Result.Value))
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_CreateResourcePoolValidationError() {
	log.Info("TestServiceHandler_CreateResourcePoolValidationError called")

	mockResourcePoolName := "respool101"

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Name:   mockResourcePoolName,
		Parent: &peloton.ResourcePoolID{Value: "respool23"},
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

	// create request
	createReq := &pb_respool.CreateRequest{
		Config: mockResourcePoolConfig,
	}

	createResp, err := suite.handler.CreateResourcePool(
		suite.context,
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
		Parent: &peloton.ResourcePoolID{Value: "respool23"},
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

	// create request
	createReq := &pb_respool.CreateRequest{
		Config: mockResourcePoolConfig,
	}

	expectedErrMsg := "resource pool already exits"
	// set expectations
	suite.mockResPoolStore.EXPECT().CreateResourcePool(
		context.Background(),
		gomock.Any(),
		gomock.Eq(mockResourcePoolConfig),
		"peloton",
	).Return(errors.New(expectedErrMsg))

	createResp, err := suite.handler.CreateResourcePool(
		suite.context,
		createReq)

	actualErrResourcePoolID := createResp.Error.AlreadyExists.Id.Value
	suite.NoError(err)
	suite.NotNil(createResp)
	suite.Nil(createResp.Result)
	suite.NotNil(createResp.Error)
	suite.Equal(expectedErrMsg, createResp.Error.AlreadyExists.Message)
	suite.NotNil(uuid.Parse(actualErrResourcePoolID))
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_UpdateResourcePool() {
	log.Info("TestServiceHandler_UpdateResourcePool called")

	mockResourcePoolName := "respool23"

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Name:   mockResourcePoolName,
		Parent: &peloton.ResourcePoolID{Value: "respool22"},
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
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: mockResourcePoolName,
	}

	// update request
	updateReq := &pb_respool.UpdateRequest{
		Id:     mockResourcePoolID,
		Config: mockResourcePoolConfig,
	}

	// set expectations
	suite.mockResPoolStore.EXPECT().UpdateResourcePool(
		context.Background(),
		gomock.Eq(mockResourcePoolID),
		gomock.Eq(mockResourcePoolConfig)).Return(nil)

	updateResp, err := suite.handler.UpdateResourcePool(
		suite.context,
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
		Parent: &peloton.ResourcePoolID{Value: "respool2"},
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
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: mockResourcePoolName,
	}

	// update request
	updateReq := &pb_respool.UpdateRequest{
		Id:     mockResourcePoolID,
		Config: mockResourcePoolConfig,
	}

	updateResp, err := suite.handler.UpdateResourcePool(
		suite.context,
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
		Parent: &peloton.ResourcePoolID{Value: "respool23"},
		Resources: []*pb_respool.ResourceConfig{
			{
				Kind:        "cpu",
				Share:       1,
				Limit:       1,
				Reservation: 0,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
	}
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: mockResourcePoolName,
	}

	// update request
	updateReq := &pb_respool.UpdateRequest{
		Id:     mockResourcePoolID,
		Config: mockResourcePoolConfig,
	}

	expectedErrMsg := "Resource pool (respool105) not found"

	updateResp, err := suite.handler.UpdateResourcePool(
		suite.context,
		updateReq)

	actualErrResourcePoolID := updateResp.Error.NotFound.Id.Value
	suite.NoError(err)
	suite.NotNil(updateResp)
	suite.NotNil(updateResp.Error)
	suite.Equal(expectedErrMsg, updateResp.Error.NotFound.Message)
	suite.Equal(mockResourcePoolID.Value, actualErrResourcePoolID)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_Query() {
	// query request
	queryReq := &pb_respool.QueryRequest{}
	updateResp, err := suite.handler.Query(
		suite.context,
		queryReq,
	)

	suite.NoError(err)
	suite.NotNil(updateResp)
	suite.NotNil(updateResp.ResourcePools)
	suite.Len(updateResp.ResourcePools, len(suite.getResPools()))
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_LookupResourcePoolID() {
	log.Info("TestServiceHandler_LookupResourcePoolID called")

	// root
	lookupRequest := &pb_respool.LookupRequest{
		Path: &pb_respool.ResourcePoolPath{
			Value: "/",
		},
	}

	lookupResponse, err := suite.handler.LookupResourcePoolID(suite.context, lookupRequest)
	suite.NoError(err)
	suite.NotNil(lookupResponse)
	suite.Equal("root", lookupResponse.Id.Value)

	// /respool1/respool11
	lookupRequest.Path.Value = "/respool1/respool11"
	lookupResponse, err = suite.handler.LookupResourcePoolID(suite.context, lookupRequest)
	suite.NoError(err)
	suite.NotNil(lookupResponse)
	suite.Equal("respool11", lookupResponse.Id.Value)

	// /respool2/respool22/
	lookupRequest.Path.Value = "/respool2/respool22/"
	lookupResponse, err = suite.handler.LookupResourcePoolID(suite.context, lookupRequest)
	suite.NoError(err)
	suite.NotNil(lookupResponse)
	suite.Equal("respool22", lookupResponse.Id.Value)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_LookupResourcePoolPathDoesNotExist() {
	log.Info("TestServiceHandler_LookupResourcePoolPathDoesNotExist called")

	// root
	lookupRequest := &pb_respool.LookupRequest{
		Path: &pb_respool.ResourcePoolPath{
			Value: "/does/not/exist",
		},
	}

	lookupResponse, err := suite.handler.LookupResourcePoolID(suite.context,
		lookupRequest,
	)

	suite.NoError(err)
	suite.NotNil(lookupResponse)
	suite.NotNil(lookupResponse.Error)
	suite.NotNil(lookupResponse.Error.NotFound)
	suite.Equal("/does/not/exist", lookupResponse.Error.NotFound.Path.Value)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_LookupResourcePoolInvalidPath() {
	log.Info("TestServiceHandler_LookupResourcePoolInvalidPath called")

	// invalid path
	lookupRequest := &pb_respool.LookupRequest{
		Path: &pb_respool.ResourcePoolPath{
			Value: "does/not/begin/with/slash",
		},
	}

	lookupResponse, err := suite.handler.LookupResourcePoolID(suite.context, lookupRequest)

	suite.NoError(err)
	suite.NotNil(lookupResponse)
	suite.NotNil(lookupResponse.Error)
	suite.NotNil(lookupResponse.Error.InvalidPath)
	suite.Equal("does/not/begin/with/slash", lookupResponse.Error.InvalidPath.Path.Value)
	suite.Equal("path should begin with /", lookupResponse.Error.InvalidPath.Message)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_DeleteResourcePool() {
	log.Info("TestServiceHandler_DeleteResourcePool called")

	mockResourcePoolName := "respool11"
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: mockResourcePoolName,
	}
	mockResPoolPath := &pb_respool.ResourcePoolPath{
		Value: "/respool1/respool11",
	}

	// delete request
	deleteReq := &pb_respool.DeleteRequest{
		Path: mockResPoolPath,
	}

	// set expectations
	suite.mockResPoolStore.EXPECT().DeleteResourcePool(
		context.Background(),
		gomock.Eq(mockResourcePoolID)).Return(nil)

	deleteResp, err := suite.handler.DeleteResourcePool(
		suite.context,
		deleteReq)

	suite.NoError(err)
	suite.NotNil(deleteResp)
	suite.Nil(deleteResp.Error)
}

func (suite *resPoolHandlerTestSuite) TestServiceHandler_DeleteResourcePoolIsNotLeaf() {
	log.Info("TestServiceHandler_DeleteResourcePoolIsNotLeaf called")

	mockResPoolPath := &pb_respool.ResourcePoolPath{
		Value: "/respool1",
	}

	// delete request
	deleteReq := &pb_respool.DeleteRequest{
		Path: mockResPoolPath,
	}

	deleteResp, err := suite.handler.DeleteResourcePool(
		suite.context,
		deleteReq)

	suite.NoError(err)
	suite.NotNil(deleteResp)
	suite.Equal("Resource Pool is not leaf", deleteResp.Error.IsNotLeaf.Message)
}
