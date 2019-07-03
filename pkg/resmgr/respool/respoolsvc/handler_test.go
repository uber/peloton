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

package respoolsvc

import (
	"context"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pb_respool "github.com/uber/peloton/.gen/peloton/api/v0/respool"

	"github.com/uber/peloton/pkg/common"
	rc "github.com/uber/peloton/pkg/resmgr/common"
	res "github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/respool/mocks"
	"github.com/uber/peloton/pkg/resmgr/scalar"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

type resPoolHandlerTestSuite struct {
	suite.Suite

	context                     context.Context
	resourceTree                res.Tree
	mockCtrl                    *gomock.Controller
	handler                     *ServiceHandler
	mockResPoolOps              *objectmocks.MockResPoolOps
	resourcePoolConfigValidator res.Validator
}

func (s *resPoolHandlerTestSuite) SetupSuite() {
	s.mockCtrl = gomock.NewController(s.T())
	s.mockResPoolOps = objectmocks.NewMockResPoolOps(s.mockCtrl)
	s.mockResPoolOps.
		EXPECT().
		GetAll(context.Background()).
		Return(s.getResPools(), nil).
		AnyTimes()
	mockJobStore := store_mocks.NewMockJobStore(s.mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(s.mockCtrl)
	s.resourceTree = res.NewTree(
		tally.NoopScope,
		s.mockResPoolOps,
		mockJobStore,
		mockTaskStore,
		rc.PreemptionConfig{Enabled: false},
	)
	resourcePoolConfigValidator, err := res.NewResourcePoolConfigValidator(s.resourceTree)
	s.NoError(err)
	s.resourcePoolConfigValidator = resourcePoolConfigValidator
}

func (s *resPoolHandlerTestSuite) TearDownSuite() {
	s.mockCtrl.Finish()
}

func (s *resPoolHandlerTestSuite) SetupTest() {
	s.context = context.Background()

	s.handler = &ServiceHandler{
		resPoolTree:            s.resourceTree,
		metrics:                res.NewMetrics(tally.NoopScope),
		resPoolOps:             s.mockResPoolOps,
		resPoolConfigValidator: s.resourcePoolConfigValidator,
	}
	s.NoError(s.resourceTree.Start())
}

func (s *resPoolHandlerTestSuite) TearDownTest() {
	s.NoError(s.resourceTree.Stop())
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

func (s *resPoolHandlerTestSuite) TestNewServiceHandler() {
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonResourceManager,
		Inbounds:  nil,
		Outbounds: nil,
		Metrics: yarpc.MetricsConfig{
			Tally: tally.NoopScope,
		},
	})

	handler := InitServiceHandler(
		dispatcher,
		tally.NoopScope,
		s.resourceTree,
		s.mockResPoolOps,
	)
	s.NotNil(handler)
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

	s.Equal(resPoolNotFoundErrString, getResp.Error.NotFound.Message)
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
	s.mockResPoolOps.EXPECT().Create(
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
	s.mockResPoolOps.EXPECT().Create(
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
	s.mockResPoolOps.EXPECT().Create(
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
	updateReq := s.getUpdateRequest()
	// set expectations
	s.mockResPoolOps.EXPECT().Update(
		gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	updateResp, err := s.handler.UpdateResourcePool(
		s.context,
		updateReq)

	s.NoError(err)
	s.NotNil(updateResp)
	s.Nil(updateResp.Error)
}

func (s *resPoolHandlerTestSuite) TestUpdateError() {
	handler, resTree, respool := s.getMockHandlerWithResTreeAndRespool()

	updateReq := s.getUpdateRequest()
	resTree.EXPECT().Get(gomock.Any()).Return(respool, nil)
	// set expectations
	s.mockResPoolOps.EXPECT().Update(
		gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("Error"))

	updateResp, err := handler.UpdateResourcePool(
		s.context,
		updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(updateResp.GetError().GetNotFound().Message, "Error")
}

func (s *resPoolHandlerTestSuite) TestUpsertError() {
	handler, resTree, respool := s.getMockHandlerWithResTreeAndRespool()
	updateReq := s.getUpdateRequest()

	resTree.EXPECT().Get(gomock.Any()).Return(respool, nil)
	// set expectations
	s.mockResPoolOps.EXPECT().Update(
		gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	resTree.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(errors.New("error in upsert"))
	respool.EXPECT().ResourcePoolConfig().Return(nil)

	s.mockResPoolOps.EXPECT().Update(
		gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("Error"))

	_, err := handler.UpdateResourcePool(
		s.context,
		updateReq)
	s.Error(err)
	s.Equal(err.Error(), "Error")
}

func (s *resPoolHandlerTestSuite) getMockHandlerWithResTreeAndRespool() (*ServiceHandler, *mocks.MockTree, *mocks.MockResPool) {
	resTree := mocks.NewMockTree(s.mockCtrl)
	return &ServiceHandler{
		resPoolTree:            resTree,
		metrics:                res.NewMetrics(tally.NoopScope),
		resPoolOps:             s.mockResPoolOps,
		resPoolConfigValidator: s.resourcePoolConfigValidator,
	}, resTree, mocks.NewMockResPool(s.mockCtrl)
}

func (s *resPoolHandlerTestSuite) getUpdateRequest() *pb_respool.UpdateRequest {
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
	return &pb_respool.UpdateRequest{
		Id:     mockResourcePoolID,
		Config: mockResourcePoolConfig,
	}
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
	s.mockResPoolOps.EXPECT().Delete(
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
	s.Equal(resPoolIsNotLeafErrString, deleteResp.Error.IsNotLeaf.Message)
}

func (s *resPoolHandlerTestSuite) TestDeleteResourcePoolNotExistant() {
	mockResPoolPath := &pb_respool.ResourcePoolPath{
		Value: "/respool1333",
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
	s.Equal(resPoolNotFoundErrString, deleteResp.GetError().GetNotFound().GetMessage())
}

func (s *resPoolHandlerTestSuite) TestDeleteblahblah() {
	tt := []struct {
		expectedFunctions func(resTree *mocks.MockTree, respool *mocks.MockResPool)
		err               error
		respError         string
		msg               string
	}{
		{
			msg:               "respool not found",
			expectedFunctions: s.DeleteResourcePoolNilID,
			err:               nil,
			respError:         resPoolNotFoundErrString,
		},
		{
			msg:               "respool not found",
			expectedFunctions: s.DeleteResourcePoolNotFoundinTree,
			err:               nil,
			respError:         resPoolNotFoundErrString,
		},
		{
			msg:               "respool is busy",
			expectedFunctions: s.DeleteResourceAllocationIsLess,
			err:               nil,
			respError:         resPoolIsBusyErrString,
		},
		{
			msg:               "respool not deleted",
			expectedFunctions: s.DeleteResourcePoolDeleteError,
			err:               nil,
			respError:         resPoolDeleteErrString,
		},
		{
			msg:               "respool not deleted",
			expectedFunctions: s.DeleteResourcePoolOpsError,
			err:               nil,
			respError:         resPoolDeleteErrString,
		},
	}

	for _, t := range tt {
		resTree := mocks.NewMockTree(s.mockCtrl)
		respool := mocks.NewMockResPool(s.mockCtrl)

		handler := &ServiceHandler{
			resPoolTree:            resTree,
			metrics:                res.NewMetrics(tally.NoopScope),
			resPoolOps:             s.mockResPoolOps,
			resPoolConfigValidator: s.resourcePoolConfigValidator,
		}
		t.expectedFunctions(resTree, respool)

		mockResPoolPath := &pb_respool.ResourcePoolPath{
			Value: "/respool1",
		}

		// delete request
		deleteReq := &pb_respool.DeleteRequest{
			Path: mockResPoolPath,
		}

		deleteResp, err := handler.DeleteResourcePool(
			s.context,
			deleteReq)
		if t.err == nil {
			s.NoError(err, t.msg)
		}
		if t.respError != "" {
			s.NotNil(deleteResp.GetError())
			switch t.respError {
			case resPoolNotFoundErrString:
				s.Contains(deleteResp.GetError().GetNotFound().Message, t.respError, t.msg)
			case resPoolIsBusyErrString:
				s.Contains(deleteResp.GetError().GetIsBusy().GetMessage(), t.respError, t.msg)
			case resPoolDeleteErrString:
				s.Contains(deleteResp.GetError().GetNotDeleted().GetMessage(), t.respError, t.msg)
			}
		}
	}
}

func (s *resPoolHandlerTestSuite) DeleteResourcePoolNilID(resTree *mocks.MockTree, respool *mocks.MockResPool) {
	resTree.EXPECT().GetByPath(gomock.Any()).Return(nil, nil)
}

func (s *resPoolHandlerTestSuite) DeleteResourcePoolNotFoundinTree(resTree *mocks.MockTree, respool *mocks.MockResPool) {
	resTree.EXPECT().GetByPath(gomock.Any()).Return(respool, nil)
	respool.EXPECT().ID().Return("")
	resTree.EXPECT().Get(gomock.Any()).Return(nil, errors.New("Error in geTree"))
}

func (s *resPoolHandlerTestSuite) DeleteResourceAllocationIsLess(resTree *mocks.MockTree, respool *mocks.MockResPool) {
	resTree.EXPECT().GetByPath(gomock.Any()).Return(respool, nil)
	respool.EXPECT().ID().Return("")
	resTree.EXPECT().Get(gomock.Any()).Return(respool, nil)
	respool.EXPECT().IsLeaf().Return(true)
	respool.EXPECT().GetTotalAllocatedResources().Return(&scalar.Resources{
		CPU: 1,
	})
	respool.EXPECT().GetDemand().Return(scalar.ZeroResource)
}

func (s *resPoolHandlerTestSuite) DeleteResourcePoolDeleteError(resTree *mocks.MockTree, respool *mocks.MockResPool) {
	resTree.EXPECT().GetByPath(gomock.Any()).Return(respool, nil)

	respool.EXPECT().ID().Return("")

	resTree.EXPECT().Get(gomock.Any()).Return(respool, nil)
	respool.EXPECT().IsLeaf().Return(true)
	respool.EXPECT().GetTotalAllocatedResources().Return(scalar.ZeroResource)
	respool.EXPECT().GetDemand().Return(scalar.ZeroResource)
	resTree.EXPECT().Delete(gomock.Any()).Return(errors.New("Error in Delete"))
}

func (s *resPoolHandlerTestSuite) DeleteResourcePoolOpsError(
	resTree *mocks.MockTree,
	respool *mocks.MockResPool,
) {
	resTree.EXPECT().GetByPath(gomock.Any()).Return(respool, nil)

	respool.EXPECT().ID().Return("")

	resTree.EXPECT().Get(gomock.Any()).Return(respool, nil)
	respool.EXPECT().IsLeaf().Return(true)
	respool.EXPECT().GetTotalAllocatedResources().Return(scalar.ZeroResource)
	respool.EXPECT().GetDemand().Return(scalar.ZeroResource)
	resTree.EXPECT().Delete(gomock.Any()).Return(nil)
	s.mockResPoolOps.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(errors.New("Error in DB"))
}

func TestResPoolHandler(t *testing.T) {
	suite.Run(t, new(resPoolHandlerTestSuite))
}
