package client

import (
	"context"
	"testing"

	client_mocks "code.uber.internal/infra/peloton/vendor_mocks/go.uber.org/yarpc/encoding/json/mocks"

	"peloton/api/respool"

	"go.uber.org/yarpc"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

type resPoolActions struct {
	suite.Suite
	mockCtrl       *gomock.Controller
	mockBaseClient *client_mocks.MockClient
	ctx            context.Context
}

func (suite *resPoolActions) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockBaseClient = client_mocks.NewMockClient(suite.mockCtrl)
	suite.ctx = context.Background()
}

func (suite *resPoolActions) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *resPoolActions) getRespoolInfos() []*respool.ResourcePoolInfo {
	return []*respool.ResourcePoolInfo{
		{
			Id: &respool.ResourcePoolID{
				Value: "root",
			},
			Children: []*respool.ResourcePoolID{
				{
					Value: "respool1",
				},
			},
			Config: nil,
			Parent: nil,
		},
		{
			Id: &respool.ResourcePoolID{
				Value: "respool1",
			},
			Children: []*respool.ResourcePoolID{
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
				Parent: &respool.ResourcePoolID{
					Value: "root",
				},
				Description: "respool1 desc",
				LdapGroups:  []string{"g1", "g2"},
				OwningTeam:  "t1",
			},
			Parent: &respool.ResourcePoolID{
				Value: "root",
			},
		},
		{
			Id: &respool.ResourcePoolID{
				Value: "respool2",
			},
			Config: nil,
			Parent: &respool.ResourcePoolID{
				Value: "respool1",
			},
		},
	}
}

func (suite *resPoolActions) TestClient_ResPoolDumpAction() {
	client := Client{
		Debug:      false,
		resClient:  suite.mockBaseClient,
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
		suite.mockBaseClient.EXPECT().Call(
			suite.ctx,
			gomock.Eq(
				yarpc.NewReqMeta().Procedure("ResourceManager.Query"),
			),
			gomock.Eq(tt.request),
			gomock.Eq(&respool.QueryResponse{}),
		).Do(func(_ context.Context, _ yarpc.CallReqMeta, _ interface{}, resBodyOut interface{}) {
			o := resBodyOut.(*respool.QueryResponse)
			*o = *tt.response
		}).Return(nil, nil)

		err := client.ResPoolDumpAction(tt.format)
		if tt.err != nil {
			suite.EqualError(err, tt.err.Error())
		} else {
			suite.NoError(err)
		}
	}
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
	err := c.ResPoolCreateAction(resourcePoolPath, "../example/test_respool.yaml")
	suite.Error(err)
	suite.Equal("resource pool name in path:respool1 and "+
		"config:TestPelotonResPool_123 don't match", err.Error())
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
