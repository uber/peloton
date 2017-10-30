package cli

import (
	"context"
	"io/ioutil"
	"testing"

	respool_mocks "code.uber.internal/infra/peloton/.gen/peloton/api/respool/mocks"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v2"
)

const _defaultResPoolConfig = "../example/default_respool.yaml"

type resPoolActions struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	mockRespool *respool_mocks.MockResourceManagerYARPCClient
	ctx         context.Context
}

func (suite *resPoolActions) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockRespool = respool_mocks.NewMockResourceManagerYARPCClient(suite.mockCtrl)
	suite.ctx = context.Background()
}

func (suite *resPoolActions) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *resPoolActions) getRespoolInfos() []*respool.ResourcePoolInfo {
	return []*respool.ResourcePoolInfo{
		{
			Id: &peloton.ResourcePoolID{
				Value: "root",
			},
			Children: []*peloton.ResourcePoolID{
				{
					Value: "respool1",
				},
			},
			Config: nil,
			Parent: nil,
		},
		{
			Id: &peloton.ResourcePoolID{
				Value: "respool1",
			},
			Children: []*peloton.ResourcePoolID{
				{
					Value: "respool2",
				},
			},
			Config: &respool.ResourcePoolConfig{
				Name: "respool1",
				Resources: []*respool.ResourceConfig{
					{
						Kind:        "cpu",
						Share:       1,
						Limit:       100,
						Reservation: 75,
					},
				},
				Policy: respool.SchedulingPolicy_PriorityFIFO,
				Parent: &peloton.ResourcePoolID{
					Value: "root",
				},
				Description: "respool1 desc",
				LdapGroups:  []string{"g1", "g2"},
				OwningTeam:  "t1",
			},
			Parent: &peloton.ResourcePoolID{
				Value: "root",
			},
		},
		{
			Id: &peloton.ResourcePoolID{
				Value: "respool2",
			},
			Config: nil,
			Parent: &peloton.ResourcePoolID{
				Value: "respool1",
			},
		},
	}
}

func (suite *resPoolActions) TestClient_ResPoolDumpAction() {
	client := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	for _, tt := range []struct {
		request  *respool.QueryRequest
		response *respool.QueryResponse
		format   string
		err      error
	}{
		{
			request: &respool.QueryRequest{},
			response: &respool.QueryResponse{
				ResourcePools: suite.getRespoolInfos(),
				Error:         nil,
			},
			format: "yaml",
			err:    nil,
		},
		{
			request: &respool.QueryRequest{},
			response: &respool.QueryResponse{
				Error: &respool.QueryResponse_Error{},
			},
			format: "yaml",
			err:    errors.New("error dumping resource pools"),
		},
	} {
		// Set expectations
		suite.mockRespool.EXPECT().Query(
			suite.ctx,
			gomock.Eq(tt.request)).
			Return(tt.response, tt.err)

		err := client.ResPoolDumpAction(tt.format)
		if tt.err != nil {
			suite.EqualError(err, tt.err.Error())
		} else {
			suite.NoError(err)
		}
	}
}

func (suite *resPoolActions) getConfig() *respool.ResourcePoolConfig {
	var config respool.ResourcePoolConfig
	buffer, err := ioutil.ReadFile(_defaultResPoolConfig)
	suite.NoError(err)
	err = yaml.Unmarshal(buffer, &config)
	suite.NoError(err)
	return &config
}

func (suite *resPoolActions) TestClient_ResPoolCreateAction() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		dispatcher: nil,
		ctx:        suite.ctx,
	}
	ID := uuid.New()
	parentID := uuid.New()
	path := "/DefaultResPool"
	config := suite.getConfig()
	config.Parent = &peloton.ResourcePoolID{
		Value: parentID,
	}

	tt := []struct {
		createRequest  *respool.CreateRequest
		createResponse *respool.CreateResponse
		lookupRequest  *respool.LookupRequest
		lookupResponse *respool.LookupResponse
		err            error
	}{
		{
			createRequest: &respool.CreateRequest{
				Config: config,
			},
			createResponse: &respool.CreateResponse{
				Result: &peloton.ResourcePoolID{
					Value: ID,
				},
			},
			lookupRequest: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: "/",
				},
			},
			lookupResponse: &respool.LookupResponse{
				Id: &peloton.ResourcePoolID{
					Value: parentID,
				},
			},
		},
		{
			createRequest: &respool.CreateRequest{
				Config: config,
			},
			createResponse: &respool.CreateResponse{
				Result: &peloton.ResourcePoolID{
					Value: ID,
				},
			},
			lookupRequest: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: "/",
				},
			},
			lookupResponse: &respool.LookupResponse{
				Id: &peloton.ResourcePoolID{
					Value: parentID,
				},
			},
			err: errors.New("cannot create root resource pool"),
		},
	}

	withMockCreateResponse := func(
		req *respool.CreateRequest,
		resp *respool.CreateResponse,
		err error,
	) {
		suite.mockRespool.EXPECT().
			CreateResourcePool(suite.ctx, gomock.Eq(req)).
			Return(resp, err)
	}

	for _, t := range tt {
		suite.withMockResourcePoolLookup(t.lookupRequest, t.lookupResponse)
		withMockCreateResponse(t.createRequest, t.createResponse, t.err)
		err := c.ResPoolCreateAction(path, _defaultResPoolConfig)
		if t.err != nil {
			suite.EqualError(err, t.err.Error())
		} else {
			suite.NoError(err)
		}
	}
}

func (suite *resPoolActions) TestClient_ResPoolUpdateAction() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	respoolID := uuid.New()
	parentID := uuid.New()
	path := "/DefaultResPool"
	config := suite.getConfig()
	config.Parent = &peloton.ResourcePoolID{
		Value: parentID,
	}

	tt := []struct {
		updateRequest         *respool.UpdateRequest
		updateResponse        *respool.UpdateResponse
		parentLookupRequest   *respool.LookupRequest
		parentLookupResponse  *respool.LookupResponse
		resPoolLookupRequest  *respool.LookupRequest
		resPoolLookupResponse *respool.LookupResponse
		err                   error
	}{
		{
			updateRequest: &respool.UpdateRequest{
				Id: &peloton.ResourcePoolID{
					Value: respoolID,
				},
				Config: config,
			},
			updateResponse: &respool.UpdateResponse{},
			parentLookupRequest: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: "/",
				},
			},
			parentLookupResponse: &respool.LookupResponse{
				Id: &peloton.ResourcePoolID{
					Value: parentID,
				},
			},
			resPoolLookupRequest: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: path,
				},
			},
			resPoolLookupResponse: &respool.LookupResponse{
				Id: &peloton.ResourcePoolID{
					Value: respoolID,
				},
			},
		},
		{
			updateRequest: &respool.UpdateRequest{
				Id: &peloton.ResourcePoolID{
					Value: respoolID,
				},
				Config: config,
			},
			updateResponse: &respool.UpdateResponse{},
			parentLookupRequest: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: "/",
				},
			},
			parentLookupResponse: &respool.LookupResponse{
				Id: &peloton.ResourcePoolID{
					Value: parentID,
				},
			},
			resPoolLookupRequest: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: path,
				},
			},
			resPoolLookupResponse: &respool.LookupResponse{
				Id: &peloton.ResourcePoolID{
					Value: respoolID,
				},
			},
			err: errors.New("cannot update root resource pool"),
		},
	}

	withMockUpdateResponse := func(
		req *respool.UpdateRequest,
		resp *respool.UpdateResponse,
		err error,
	) {
		suite.mockRespool.EXPECT().
			UpdateResourcePool(suite.ctx, gomock.Eq(req)).
			Return(resp, err)
	}

	for _, t := range tt {
		suite.withMockResourcePoolLookup(t.parentLookupRequest, t.parentLookupResponse)
		suite.withMockResourcePoolLookup(t.resPoolLookupRequest, t.resPoolLookupResponse)
		withMockUpdateResponse(t.updateRequest, t.updateResponse, t.err)
		err := c.ResPoolUpdateAction(path, _defaultResPoolConfig)
		if t.err != nil {
			suite.EqualError(err, t.err.Error())
		} else {
			suite.NoError(err)
		}
	}
}

func (suite *resPoolActions) TestClient_ResPoolDeleteAction() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	path := "/DefaultResPool"

	testCases := []struct {
		deleteRequest  *respool.DeleteRequest
		deleteResponse *respool.DeleteResponse
		err            error
	}{
		{
			deleteRequest: &respool.DeleteRequest{
				Path: &respool.ResourcePoolPath{
					Value: path,
				},
			},
			deleteResponse: &respool.DeleteResponse{},
		},
		{
			deleteRequest: &respool.DeleteRequest{
				Path: &respool.ResourcePoolPath{
					Value: path,
				},
			},
			deleteResponse: &respool.DeleteResponse{},
			err:            errors.New("cannot delete resource pool"),
		},
	}

	for _, t := range testCases {
		suite.withMockDeleteResponse(t.deleteRequest, t.deleteResponse, t.err)
		err := c.ResPoolDeleteAction(path)
		if t.err != nil {
			suite.EqualError(err, t.err.Error())
		} else {
			suite.NoError(err)
		}
	}
}

func (suite *resPoolActions) withMockUpdateResponse(
	req *respool.UpdateRequest,
	resp *respool.UpdateResponse,
	err error,
) {
	suite.mockRespool.EXPECT().
		UpdateResourcePool(suite.ctx, gomock.Eq(req)).
		Return(resp, err)
}

func (suite *resPoolActions) withMockCreateResponse(
	req *respool.CreateRequest,
	resp *respool.CreateResponse,
	err error,
) {
	suite.mockRespool.EXPECT().
		CreateResourcePool(suite.ctx, gomock.Eq(req)).
		Return(resp, err)
}

func (suite *resPoolActions) withMockDeleteResponse(
	req *respool.DeleteRequest,
	resp *respool.DeleteResponse,
	err error,
) {
	suite.mockRespool.EXPECT().
		DeleteResourcePool(suite.ctx, gomock.Eq(req)).
		Return(resp, err)
}

func (suite *resPoolActions) withMockResourcePoolLookup(
	req *respool.LookupRequest,
	resp *respool.LookupResponse,
) {
	suite.mockRespool.EXPECT().
		LookupResourcePoolID(suite.ctx, gomock.Eq(req)).
		Return(resp, nil)
}

func (suite *resPoolActions) TestParseResourcePath() {
	tt := []struct {
		resourcePoolPath string
		parentPath       string
		resourcePoolName string
	}{
		{"/a/b/c", "/a/b/", "c"},
		{"/a/b/c/", "/a/b/", "c"},
		{"/a", "/", "a"},
	}

	for _, test := range tt {
		suite.Equal(test.parentPath, parseParentPath(test.resourcePoolPath))
		suite.Equal(test.resourcePoolName, parseRespoolName(test.resourcePoolPath))
	}
}

func (suite *resPoolActions) TestResPoolCreateActionInvalidPath() {
	c := Client{}
	resourcePoolPath := "/"
	err := c.ResPoolCreateAction(resourcePoolPath, "")
	suite.Error(err)
	suite.Equal("cannot create root resource pool", err.Error())
}

func (suite *resPoolActions) TestResPoolCreateActionInvalidName() {
	c := Client{}
	resourcePoolPath := "/respool1"
	err := c.ResPoolCreateAction(resourcePoolPath, _defaultResPoolConfig)
	suite.Error(err)
	suite.Equal("resource pool name in path:respool1 and "+
		"config:DefaultResPool don't match", err.Error())
}

func (suite *resPoolActions) TestResPoolCreateActionInvalidConfig() {
	c := Client{}
	resourcePoolPath := "/respool1"
	err := c.ResPoolCreateAction(resourcePoolPath, "testdata/test_respool_parent.yaml")
	suite.Error(err)
	suite.Equal("parent should not be supplied in the config", err.Error())
}

func TestResPoolHandler(t *testing.T) {
	suite.Run(t, new(resPoolActions))
}
