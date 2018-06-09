package respool

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"

	"code.uber.internal/infra/peloton/common"
	rc "code.uber.internal/infra/peloton/resmgr/common"
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

func (s *resPoolHandlerTestSuite) SetupSuite() {
	s.mockCtrl = gomock.NewController(s.T())
	s.mockResPoolStore = store_mocks.NewMockResourcePoolStore(s.mockCtrl)
	s.mockResPoolStore.EXPECT().GetAllResourcePools(context.Background()).
		Return(s.getResPools(), nil).AnyTimes()
	mockJobStore := store_mocks.NewMockJobStore(s.mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(s.mockCtrl)
	s.resourceTree = &tree{
		store:            s.mockResPoolStore,
		root:             nil,
		metrics:          NewMetrics(tally.NoopScope),
		resPools:         make(map[string]ResPool),
		jobStore:         mockJobStore,
		taskStore:        mockTaskStore,
		scope:            tally.NoopScope,
		preemptionConfig: rc.PreemptionConfig{Enabled: false},
	}
	resourcePoolConfigValidator, err := NewResourcePoolConfigValidator(s.resourceTree)
	s.NoError(err)
	s.resourcePoolConfigValidator = resourcePoolConfigValidator
}

func (s *resPoolHandlerTestSuite) TearDownSuite() {
	s.mockCtrl.Finish()
}

func (s *resPoolHandlerTestSuite) SetupTest() {
	s.context = context.Background()
	err := s.resourceTree.Start()
	s.NoError(err)

	s.handler = &serviceHandler{
		resPoolTree:            s.resourceTree,
		dispatcher:             nil,
		metrics:                NewMetrics(tally.NoopScope),
		store:                  s.mockResPoolStore,
		runningState:           rc.RunningStateRunning,
		resPoolConfigValidator: s.resourcePoolConfigValidator,
	}
}

func (s *resPoolHandlerTestSuite) TearDownTest() {
	log.Info("tearing down")
	err := s.resourceTree.Stop()
	s.NoError(err)
	err = s.handler.Stop()
	s.NoError(err)
}

// Returns resource configs
func (s *resPoolHandlerTestSuite) getResourceConfig() []*pb_respool.ResourceConfig {

	resConfigs := []*pb_respool.ResourceConfig{
		{
			Share:       1,
			Kind:        "cpu",
			Reservation: 100,
			Limit:       1000,
			Type:        pb_respool.ReservationType_ELASTIC,
		},
		{
			Share:       1,
			Kind:        "memory",
			Reservation: 100,
			Limit:       1000,
			Type:        pb_respool.ReservationType_ELASTIC,
		},
		{
			Share:       1,
			Kind:        "disk",
			Reservation: 100,
			Limit:       1000,
			Type:        pb_respool.ReservationType_ELASTIC,
		},
		{
			Share:       1,
			Kind:        "gpu",
			Reservation: 2,
			Limit:       4,
			Type:        pb_respool.ReservationType_ELASTIC,
		},
	}
	return resConfigs
}

// Returns resource pools
func (s *resPoolHandlerTestSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {

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
		"respool23": {
			Name:   "respool23",
			Parent: &peloton.ResourcePoolID{Value: "respool22"},
			Resources: []*pb_respool.ResourceConfig{
				{
					Kind:        "cpu",
					Reservation: 50,
					Limit:       100,
					Share:       1,
					Type:        pb_respool.ReservationType_ELASTIC,
				},
				{
					Kind:        "memory",
					Reservation: 50,
					Limit:       100,
					Share:       1,
					Type:        pb_respool.ReservationType_ELASTIC,
				},
				{
					Kind:        "disk",
					Reservation: 50,
					Limit:       100,
					Share:       1,
					Type:        pb_respool.ReservationType_ELASTIC,
				},
				{
					Kind:        "gpu",
					Reservation: 50,
					Limit:       100,
					Share:       1,
					Type:        pb_respool.ReservationType_ELASTIC,
				},
			},
			Policy: policy,
		},
	}
}

func (s *resPoolHandlerTestSuite) TestGetResourcePoolEmptyID() {
	// form request
	getReq := &pb_respool.GetRequest{}

	getResp, err := s.handler.GetResourcePool(
		s.context,
		getReq,
	)

	s.NoError(err)
	s.NotNil(getResp)
	s.Equal(common.RootResPoolID, getResp.Poolinfo.Id.Value)
	s.Nil(getResp.Poolinfo.Parent)
}

func (s *resPoolHandlerTestSuite) TestGetResourcePoolLeafNode() {
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool21",
	}

	// form request
	getReq := &pb_respool.GetRequest{
		Id: mockResourcePoolID,
	}

	getResp, err := s.handler.GetResourcePool(
		s.context,
		getReq,
	)

	s.NoError(err)
	s.NotNil(getResp)
	s.Equal(mockResourcePoolID.Value, getResp.Poolinfo.Id.Value)
	s.Len(getResp.Poolinfo.Children, 0)
}

func (s *resPoolHandlerTestSuite) TestGetResourcePoolWithChildNodes() {
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool2",
	}

	// form request
	getReq := &pb_respool.GetRequest{
		Id:                mockResourcePoolID,
		IncludeChildPools: true,
	}

	getResp, err := s.handler.GetResourcePool(
		s.context,
		getReq,
	)

	s.NoError(err)
	s.NotNil(getResp)
	s.Equal(mockResourcePoolID.Value, getResp.Poolinfo.Id.Value)
	s.Len(getResp.Poolinfo.Children, 2)
	s.Len(getResp.ChildPools, 2)
}

func (s *resPoolHandlerTestSuite) TestGetResourcePoolError() {
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "non_exist",
	}

	// form request
	getReq := &pb_respool.GetRequest{
		Id: mockResourcePoolID,
	}

	getResp, err := s.handler.GetResourcePool(
		s.context,
		getReq,
	)

	s.NoError(err)
	s.NotNil(getResp)
	s.NotNil(getResp.Error)

	expectedMsg := "Resource pool not found"
	s.Equal(expectedMsg, getResp.Error.NotFound.Message)
}

func (s *resPoolHandlerTestSuite) TestCreateResourcePool() {
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
				Type:        pb_respool.ReservationType_ELASTIC,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	// create request
	createReq := &pb_respool.CreateRequest{
		Config: mockResourcePoolConfig,
	}

	// set expectations
	s.mockResPoolStore.EXPECT().CreateResourcePool(
		context.Background(),
		gomock.Any(),
		gomock.Eq(mockResourcePoolConfig),
		"peloton").Return(nil)

	createResp, err := s.handler.CreateResourcePool(
		s.context,
		createReq)

	s.NoError(err)
	s.NotNil(createResp)
	s.Nil(createResp.Error)
	s.NotNil(uuid.Parse(createResp.Result.Value))
}

func (s *resPoolHandlerTestSuite) TestCreateStaticResourcePool() {
	mockResourcePoolName := "respool109"
	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Name:   mockResourcePoolName,
		Parent: &peloton.ResourcePoolID{Value: "respool23"},
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 1,
				Limit:       1,
				Share:       1,
				Kind:        "cpu",
				Type:        pb_respool.ReservationType_STATIC,
			},
			{
				Reservation: 1,
				Limit:       1,
				Share:       1,
				Kind:        "memory",
				Type:        pb_respool.ReservationType_STATIC,
			},
			{
				Reservation: 1,
				Limit:       1,
				Share:       1,
				Kind:        "disk",
				Type:        pb_respool.ReservationType_STATIC,
			},
			{
				Reservation: 1,
				Limit:       1,
				Share:       1,
				Kind:        "gpu",
				Type:        pb_respool.ReservationType_STATIC,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	// create request
	createReq := &pb_respool.CreateRequest{
		Config: mockResourcePoolConfig,
	}

	// set expectations
	s.mockResPoolStore.EXPECT().CreateResourcePool(
		context.Background(),
		gomock.Any(),
		gomock.Eq(mockResourcePoolConfig),
		"peloton").Return(nil)

	createResp, err := s.handler.CreateResourcePool(
		s.context,
		createReq)

	s.NoError(err)
	s.NotNil(createResp)
	s.Nil(createResp.Error)
	s.NotNil(uuid.Parse(createResp.Result.Value))

	// query request to get all the resourcepool configs
	queryReq := &pb_respool.QueryRequest{}
	queryResp, err := s.handler.Query(
		s.context,
		queryReq,
	)
	var respoolConfig *pb_respool.ResourcePoolConfig
	for _, respoolInfo := range queryResp.GetResourcePools() {
		if respoolInfo.GetId().Value == createResp.Result.Value {
			respoolConfig = respoolInfo.GetConfig()
		}
	}
	s.NotNil(respoolConfig)
	s.EqualValues(pb_respool.ReservationType_STATIC, respoolConfig.GetResources()[0].Type)
	s.EqualValues(pb_respool.ReservationType_STATIC, respoolConfig.GetResources()[1].Type)
	s.EqualValues(pb_respool.ReservationType_STATIC, respoolConfig.GetResources()[2].Type)
	s.EqualValues(pb_respool.ReservationType_STATIC, respoolConfig.GetResources()[3].Type)
}

func (s *resPoolHandlerTestSuite) TestCreateResourcePoolValidationError() {
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
				Type:        pb_respool.ReservationType_ELASTIC,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
	}

	// create request
	createReq := &pb_respool.CreateRequest{
		Config: mockResourcePoolConfig,
	}

	createResp, err := s.handler.CreateResourcePool(
		s.context,
		createReq)

	s.NoError(err)
	s.NotNil(createResp)
	s.NotNil(createResp.Error)
	expectedMsg := "resource cpu, reservation 5 exceeds limit 1"
	s.Equal(expectedMsg, createResp.Error.InvalidResourcePoolConfig.Message)
}

func (s *resPoolHandlerTestSuite) TestCreateResourcePoolAlreadyExistsError() {
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
				Type:        pb_respool.ReservationType_ELASTIC,
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
	s.mockResPoolStore.EXPECT().CreateResourcePool(
		context.Background(),
		gomock.Any(),
		gomock.Eq(mockResourcePoolConfig),
		"peloton",
	).Return(errors.New(expectedErrMsg))

	createResp, err := s.handler.CreateResourcePool(
		s.context,
		createReq)

	actualErrResourcePoolID := createResp.Error.AlreadyExists.Id.Value
	s.NoError(err)
	s.NotNil(createResp)
	s.Nil(createResp.Result)
	s.NotNil(createResp.Error)
	s.Equal(expectedErrMsg, createResp.Error.AlreadyExists.Message)
	s.NotNil(uuid.Parse(actualErrResourcePoolID))
}

func (s *resPoolHandlerTestSuite) TestUpdateResourcePool() {
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
				Type:        pb_respool.ReservationType_ELASTIC,
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
	s.mockResPoolStore.EXPECT().UpdateResourcePool(
		context.Background(),
		gomock.Eq(mockResourcePoolID),
		gomock.Eq(mockResourcePoolConfig)).Return(nil)

	updateResp, err := s.handler.UpdateResourcePool(
		s.context,
		updateReq)

	s.NoError(err)
	s.NotNil(updateResp)
	s.Nil(updateResp.Error)
}

func (s *resPoolHandlerTestSuite) TestUpdateResourcePoolValidationError() {
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
				Type:        pb_respool.ReservationType_ELASTIC,
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

	updateResp, err := s.handler.UpdateResourcePool(
		s.context,
		updateReq)

	s.NoError(err)
	s.NotNil(updateResp)
	s.NotNil(updateResp.Error)
	expectedMsg := "resource cpu, reservation 5 exceeds limit 1"
	s.Equal(expectedMsg, updateResp.Error.InvalidResourcePoolConfig.Message)
}

func (s *resPoolHandlerTestSuite) TestUpdateResourcePoolNotExistsError() {
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
				Type:        pb_respool.ReservationType_ELASTIC,
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

	expectedErrMsg := "resource pool (respool105) not found"

	updateResp, err := s.handler.UpdateResourcePool(
		s.context,
		updateReq)

	actualErrResourcePoolID := updateResp.Error.NotFound.Id.Value
	s.NoError(err)
	s.NotNil(updateResp)
	s.NotNil(updateResp.Error)
	s.Equal(expectedErrMsg, updateResp.Error.NotFound.Message)
	s.Equal(mockResourcePoolID.Value, actualErrResourcePoolID)
}

func (s *resPoolHandlerTestSuite) TestQuery() {
	// query request
	queryReq := &pb_respool.QueryRequest{}
	updateResp, err := s.handler.Query(
		s.context,
		queryReq,
	)

	s.NoError(err)
	s.NotNil(updateResp)
	s.NotNil(updateResp.ResourcePools)
	s.Len(updateResp.ResourcePools, len(s.getResPools()))
}

func (s *resPoolHandlerTestSuite) TestLookupResourcePoolID() {
	// root
	lookupRequest := &pb_respool.LookupRequest{
		Path: &pb_respool.ResourcePoolPath{
			Value: "/",
		},
	}

	lookupResponse, err := s.handler.LookupResourcePoolID(s.context, lookupRequest)
	s.NoError(err)
	s.NotNil(lookupResponse)
	s.Equal("root", lookupResponse.Id.Value)

	// /respool1/respool11
	lookupRequest.Path.Value = "/respool1/respool11"
	lookupResponse, err = s.handler.LookupResourcePoolID(s.context, lookupRequest)
	s.NoError(err)
	s.NotNil(lookupResponse)
	s.Equal("respool11", lookupResponse.Id.Value)

	// /respool2/respool22/
	lookupRequest.Path.Value = "/respool2/respool22/"
	lookupResponse, err = s.handler.LookupResourcePoolID(s.context, lookupRequest)
	s.NoError(err)
	s.NotNil(lookupResponse)
	s.Equal("respool22", lookupResponse.Id.Value)
}

func (s *resPoolHandlerTestSuite) TestLookupResourcePoolPathDoesNotExist() {
	// root
	lookupRequest := &pb_respool.LookupRequest{
		Path: &pb_respool.ResourcePoolPath{
			Value: "/does/not/exist",
		},
	}

	lookupResponse, err := s.handler.LookupResourcePoolID(s.context,
		lookupRequest,
	)

	s.NoError(err)
	s.NotNil(lookupResponse)
	s.NotNil(lookupResponse.Error)
	s.NotNil(lookupResponse.Error.NotFound)
	s.Equal("/does/not/exist", lookupResponse.Error.NotFound.Path.Value)
}

func (s *resPoolHandlerTestSuite) TestLookupResourcePoolInvalidPath() {
	// invalid path
	lookupRequest := &pb_respool.LookupRequest{
		Path: &pb_respool.ResourcePoolPath{
			Value: "does/not/begin/with/slash",
		},
	}

	lookupResponse, err := s.handler.LookupResourcePoolID(s.context, lookupRequest)

	s.NoError(err)
	s.NotNil(lookupResponse)
	s.NotNil(lookupResponse.Error)
	s.NotNil(lookupResponse.Error.InvalidPath)
	s.Equal("does/not/begin/with/slash", lookupResponse.Error.InvalidPath.Path.Value)
	s.Equal("path should begin with /", lookupResponse.Error.InvalidPath.Message)
}

func (s *resPoolHandlerTestSuite) TestDeleteResourcePool() {
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
	s.mockResPoolStore.EXPECT().DeleteResourcePool(
		context.Background(),
		gomock.Eq(mockResourcePoolID)).Return(nil)

	deleteResp, err := s.handler.DeleteResourcePool(
		s.context,
		deleteReq)

	s.NoError(err)
	s.NotNil(deleteResp)
	s.Nil(deleteResp.Error)
}

func (s *resPoolHandlerTestSuite) TestDeleteResourcePoolIsNotLeaf() {
	mockResPoolPath := &pb_respool.ResourcePoolPath{
		Value: "/respool1",
	}

	// delete request
	deleteReq := &pb_respool.DeleteRequest{
		Path: mockResPoolPath,
	}

	deleteResp, err := s.handler.DeleteResourcePool(
		s.context,
		deleteReq)

	s.NoError(err)
	s.NotNil(deleteResp)
	s.Equal("Resource Pool is not leaf", deleteResp.Error.IsNotLeaf.Message)
}

func TestResPoolHandler(t *testing.T) {
	suite.Run(t, new(resPoolHandlerTestSuite))
}
