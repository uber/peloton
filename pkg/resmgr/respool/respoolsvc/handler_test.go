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
	"github.com/stretchr/testify/assert"
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

func (s *resPoolHandlerTestSuite) TestGetResourcePools() {
	tt := []struct {
		msg          string
		req          *pb_respool.GetRequest
		wantId       string
		wantChildren int
		wantErr      string
	}{
		{
			msg: "get leaf pool",
			req: &pb_respool.GetRequest{
				Id: &peloton.ResourcePoolID{
					Value: "respool21",
				},
				IncludeChildPools: true,
			},
			wantId:       "respool21",
			wantChildren: 0,
		},
		{
			msg: "get root pool",
			req: &pb_respool.GetRequest{
				IncludeChildPools: true,
			},
			wantId:       common.RootResPoolID,
			wantChildren: 3,
		},
		{
			msg: "get non leaf pool",
			req: &pb_respool.GetRequest{
				Id: &peloton.ResourcePoolID{
					Value: "respool2",
				},
				IncludeChildPools: true,
			},
			wantId:       "respool2",
			wantChildren: 2,
		},
		{
			msg: "get non existent pool",
			req: &pb_respool.GetRequest{
				Id: &peloton.ResourcePoolID{
					Value: "/does/not/exist",
				},
				IncludeChildPools: true,
			},
			wantId:       "",
			wantChildren: 0,
			wantErr:      resPoolNotFoundErrString,
		},
	}

	for _, t := range tt {
		s.T().Run(t.msg, func(test *testing.T) {
			resp, err := s.handler.GetResourcePool(
				s.context,
				t.req,
			)
			s.NoError(err)

			if t.wantErr != "" {
				s.Equal(t.wantErr, resp.Error.NotFound.Message)
				return
			}

			s.NotNil(resp)
			s.Equal(t.wantId, resp.Poolinfo.Id.Value)
			s.Len(resp.Poolinfo.Children, t.wantChildren)
			s.Len(resp.ChildPools, t.wantChildren)
		})
	}
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
	for _, r := range respoolConfig.GetResources() {
		s.EqualValues(pb_respool.ReservationType_STATIC, r.Type)
	}
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
		gomock.Any(), gomock.Any(), gomock.Any()).Return(assert.AnError)

	updateResp, err := handler.UpdateResourcePool(
		s.context,
		updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(updateResp.GetError().GetNotFound().Message, assert.AnError.Error())
}

func (s *resPoolHandlerTestSuite) TestUpsertError() {
	handler, resTree, respool := s.getMockHandlerWithResTreeAndRespool()
	updateReq := s.getUpdateRequest()

	resTree.EXPECT().Get(gomock.Any()).Return(respool, nil)
	// set expectations
	s.mockResPoolOps.EXPECT().Update(
		gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	resTree.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(assert.AnError)
	respool.EXPECT().ResourcePoolConfig().Return(nil)

	s.mockResPoolOps.EXPECT().Update(
		gomock.Any(), gomock.Any(), gomock.Any()).Return(assert.AnError)

	_, err := handler.UpdateResourcePool(
		s.context,
		updateReq)
	s.Error(err)
	s.Equal(err.Error(), assert.AnError.Error())
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
	mockResPoolOps := objectmocks.NewMockResPoolOps(s.mockCtrl)
	mockResPoolOps.EXPECT().Update(s.context, mockResourcePoolID, mockResourcePoolConfig).Return(nil)
	s.handler.resPoolOps = mockResPoolOps

	tt := []struct {
		msg     string
		req     *pb_respool.UpdateRequest
		wantErr string
	}{
		{
			msg: "validation should fail",
			req: &pb_respool.UpdateRequest{
				Id:     mockResourcePoolID,
				Config: mockResourcePoolConfig,
			},
			wantErr: "resource cpu, reservation 5 exceeds limit 1",
		},
		{
			msg: "force should bypass validation",
			req: &pb_respool.UpdateRequest{
				Id:     mockResourcePoolID,
				Config: mockResourcePoolConfig,
				Force:  true,
			},
			wantErr: "",
		},
	}

	for _, test := range tt {
		s.T().Run(test.msg, func(t *testing.T) {
			updateResp, err := s.handler.UpdateResourcePool(
				s.context,
				test.req)
			s.NoError(err)
			s.NotNil(updateResp)

			if test.wantErr != "" {
				s.NotNil(updateResp.Error)
				s.Equal(test.wantErr, updateResp.Error.InvalidResourcePoolConfig.Message)
			} else {
				s.Nil(updateResp.Error)
			}
		})
	}
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

	s.NotNil(updateResp)

	actualErrResourcePoolID := updateResp.GetError().GetNotFound().GetId().GetValue()
	s.Equal(mockResourcePoolID.Value, actualErrResourcePoolID)

	s.NoError(err)
	s.NotNil(updateResp)
	s.NotNil(updateResp.Error)
	s.Equal(expectedErrMsg, updateResp.Error.NotFound.Message)
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
	tt := []struct {
		msg      string
		path     string
		wantResp *pb_respool.LookupResponse
	}{
		{
			msg:  "lookup root",
			path: "/",
			wantResp: &pb_respool.LookupResponse{
				Error: nil,
				Id: &peloton.ResourcePoolID{
					Value: "root",
				},
			},
		},
		{
			msg:  "lookup leaf with trailing slash",
			path: "/respool2/respool22/",
			wantResp: &pb_respool.LookupResponse{
				Error: nil,
				Id: &peloton.ResourcePoolID{
					Value: "respool22",
				},
			},
		},
		{
			msg:  "lookup leaf without trailing slash",
			path: "/respool1/respool11",
			wantResp: &pb_respool.LookupResponse{
				Error: nil,
				Id: &peloton.ResourcePoolID{
					Value: "respool11",
				},
			},
		},
		{
			msg:  "path does not exist",
			path: "/does/not/exist",
			wantResp: &pb_respool.LookupResponse{
				Error: &pb_respool.LookupResponse_Error{
					NotFound: &pb_respool.ResourcePoolPathNotFound{
						Path: &pb_respool.ResourcePoolPath{
							Value: "/does/not/exist",
						},
						Message: "resource pool not found",
					},
				},
			},
		},
		{
			msg:  "invalid path",
			path: "does/not/begin/with/slash",
			wantResp: &pb_respool.LookupResponse{
				Error: &pb_respool.LookupResponse_Error{
					InvalidPath: &pb_respool.InvalidResourcePoolPath{
						Path: &pb_respool.ResourcePoolPath{
							Value: "does/not/begin/with/slash",
						},
						Message: "path should begin with /",
					},
				},
			},
		},
	}

	for _, t := range tt {
		s.T().Run(t.msg, func(test *testing.T) {
			lookupRequest := &pb_respool.LookupRequest{
				Path: &pb_respool.ResourcePoolPath{
					Value: t.path,
				},
			}
			lookupResponse, err := s.handler.LookupResourcePoolID(s.context, lookupRequest)
			s.NoError(err)
			s.NotNil(lookupResponse)

			if t.wantResp.Id != nil {
				s.Equal(t.wantResp.Id.Value, lookupResponse.Id.Value)
			} else {
				s.Equal(t.wantResp.Error, lookupResponse.Error)
			}
		})
	}
}

func (s *resPoolHandlerTestSuite) TestDeleteResourcePoolErrors() {
	tt := []struct {
		msg         string
		expectation func(resTree *mocks.MockTree, respool *mocks.MockResPool)
		wantErr     string
	}{
		{
			msg:         "respool deleted",
			expectation: s.deleteResourcePool,
			wantErr:     "",
		},
		{
			msg:         "respool not found",
			expectation: s.deleteResourcePoolNilID,
			wantErr:     resPoolNotFoundErrString,
		},
		{
			msg:         "respool not found in tree",
			expectation: s.deleteResourcePoolNotFoundInTree,
			wantErr:     resPoolNotFoundErrString,
		},
		{
			msg:         "respool is busy",
			expectation: s.deleteResourceAllocationIsLess,
			wantErr:     resPoolIsBusyErrString,
		},
		{
			msg:         "respool not deleted",
			expectation: s.deleteResourcePoolDeleteError,
			wantErr:     resPoolDeleteErrString,
		},
		{
			msg:         "respool not deleted",
			expectation: s.deleteResourcePoolOpsError,
			wantErr:     resPoolDeleteErrString,
		},
		{
			msg:         "respool is non leaf",
			expectation: s.deleteResourcePoolNonLeaf,
			wantErr:     resPoolIsNotLeafErrString,
		},
	}

	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()

	for _, t := range tt {
		s.T().Run(t.msg, func(test *testing.T) {
			resTree := mocks.NewMockTree(mockCtrl)
			respool := mocks.NewMockResPool(mockCtrl)

			handler := &ServiceHandler{
				resPoolTree:            resTree,
				metrics:                res.NewMetrics(tally.NoopScope),
				resPoolOps:             s.mockResPoolOps,
				resPoolConfigValidator: s.resourcePoolConfigValidator,
			}
			// set expectations
			t.expectation(resTree, respool)

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
			s.NoError(err)

			if t.wantErr == "" {
				s.Nil(deleteResp.Error)
				return
			}

			// error testing
			s.NotNil(deleteResp)
			s.NotNil(deleteResp.GetError())
			switch t.wantErr {
			case resPoolNotFoundErrString:
				s.Contains(
					deleteResp.GetError().GetNotFound().GetMessage(),
					t.wantErr,
				)
			case resPoolIsBusyErrString:
				s.Contains(
					deleteResp.GetError().GetIsBusy().GetMessage(),
					t.wantErr,
				)
			case resPoolDeleteErrString:
				s.Contains(
					deleteResp.GetError().GetNotDeleted().GetMessage(),
					t.wantErr,
				)
			case resPoolIsNotLeafErrString:
				s.Contains(
					deleteResp.GetError().GetIsNotLeaf().GetMessage(),
					t.wantErr,
				)
			}
		})
	}
}

func TestResPoolHandler(t *testing.T) {
	suite.Run(t, new(resPoolHandlerTestSuite))
}

// Test Helpers
// -------------

func (s *resPoolHandlerTestSuite) deleteResourcePool(
	resTree *mocks.MockTree,
	respool *mocks.MockResPool) {
	resTree.EXPECT().GetByPath(gomock.Any()).Return(respool, nil)
	respool.EXPECT().ID().Return("")
	resTree.EXPECT().Get(gomock.Any()).Return(respool, nil)
	respool.EXPECT().IsLeaf().Return(true)
	respool.EXPECT().GetTotalAllocatedResources().Return(scalar.ZeroResource)
	respool.EXPECT().GetDemand().Return(scalar.ZeroResource)
	resTree.EXPECT().Delete(gomock.Any()).Return(nil)
	s.mockResPoolOps.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(nil)
}

func (s *resPoolHandlerTestSuite) deleteResourcePoolNonLeaf(
	resTree *mocks.MockTree,
	respool *mocks.MockResPool) {
	resTree.EXPECT().GetByPath(gomock.Any()).Return(respool, nil)
	resTree.EXPECT().Get(gomock.Any()).Return(respool, nil)
	respool.EXPECT().ID().Return("")
	respool.EXPECT().IsLeaf().Return(false)
}

func (s *resPoolHandlerTestSuite) deleteResourcePoolNilID(
	resTree *mocks.MockTree,
	respool *mocks.MockResPool) {
	resTree.EXPECT().GetByPath(gomock.Any()).Return(nil, nil)
}

func (s *resPoolHandlerTestSuite) deleteResourcePoolNotFoundInTree(
	resTree *mocks.MockTree,
	respool *mocks.MockResPool) {
	resTree.EXPECT().GetByPath(gomock.Any()).Return(respool, nil)
	respool.EXPECT().ID().Return("")
	resTree.EXPECT().Get(gomock.Any()).Return(nil, assert.AnError)
}

func (s *resPoolHandlerTestSuite) deleteResourceAllocationIsLess(
	resTree *mocks.MockTree,
	respool *mocks.MockResPool) {
	resTree.EXPECT().GetByPath(gomock.Any()).Return(respool, nil)
	respool.EXPECT().ID().Return("")
	resTree.EXPECT().Get(gomock.Any()).Return(respool, nil)
	respool.EXPECT().IsLeaf().Return(true)
	respool.EXPECT().GetTotalAllocatedResources().Return(&scalar.Resources{
		CPU: 1,
	})
	respool.EXPECT().GetDemand().Return(scalar.ZeroResource)
}

func (s *resPoolHandlerTestSuite) deleteResourcePoolDeleteError(
	resTree *mocks.MockTree,
	respool *mocks.MockResPool) {
	resTree.EXPECT().GetByPath(gomock.Any()).Return(respool, nil)

	respool.EXPECT().ID().Return("")

	resTree.EXPECT().Get(gomock.Any()).Return(respool, nil)
	respool.EXPECT().IsLeaf().Return(true)
	respool.EXPECT().GetTotalAllocatedResources().Return(scalar.ZeroResource)
	respool.EXPECT().GetDemand().Return(scalar.ZeroResource)
	resTree.EXPECT().Delete(gomock.Any()).Return(assert.AnError)
}

func (s *resPoolHandlerTestSuite) deleteResourcePoolOpsError(
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
	s.mockResPoolOps.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(assert.AnError)
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
