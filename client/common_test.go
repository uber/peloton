package client

import (
	"context"
	"errors"
	"testing"

	client_mocks "code.uber.internal/infra/peloton/vendor_mocks/go.uber.org/yarpc/encoding/json/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc"

	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
)

type commonTestSuite struct {
	suite.Suite
	mockCtrl       *gomock.Controller
	mockBaseClient *client_mocks.MockClient
	ctx            context.Context
}

func (suite *commonTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockBaseClient = client_mocks.NewMockClient(suite.mockCtrl)
	suite.ctx = context.Background()
}

func (suite *commonTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *commonTestSuite) TestClient_LookupResourcePoolID() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockBaseClient,
		dispatcher: nil,
		ctx:        suite.ctx,
	}
	id := uuid.New()

	for _, tt := range []struct {
		request  *respool.LookupRequest
		response *respool.LookupResponse
		path     string
		err      error
	}{
		{
			request: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: "/a/b/c",
				},
			},
			response: &respool.LookupResponse{
				Id: &respool.ResourcePoolID{
					Value: id,
				},
				Error: nil,
			},
			path: "/a/b/c",
			err:  nil,
		},
		{
			request: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: "/a/b/c",
				},
			},
			response: &respool.LookupResponse{
				Error: &respool.LookupResponse_Error{},
			},
			path: "/a/b/c",

			err: errors.New("error looking up ID"),
		},
	} {
		suite.mockBaseClient.EXPECT().Call(
			suite.ctx,
			gomock.Eq(
				yarpc.NewReqMeta().Procedure("ResourceManager.LookupResourcePoolID"),
			),
			gomock.Eq(tt.request),
			gomock.Eq(&respool.LookupResponse{}),
		).Do(func(_ context.Context, _ yarpc.CallReqMeta, _ interface{}, resBodyOut interface{}) {
			o := resBodyOut.(*respool.LookupResponse)
			*o = *tt.response
		}).Return(nil, tt.err)

		resPoolID, err := c.LookupResourcePoolID(tt.path)
		if tt.err != nil {
			suite.EqualError(err, tt.err.Error())
		} else {
			suite.Equal(id, resPoolID.Value)
			suite.NoError(err)
		}
	}
}

func TestCommon(t *testing.T) {
	suite.Run(t, new(commonTestSuite))
}
