package cli

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	respool_mocks "code.uber.internal/infra/peloton/.gen/peloton/api/respool/mocks"
)

type commonTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	mockRespool *respool_mocks.MockResourceManagerYARPCClient
	ctx         context.Context
}

func (suite *commonTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockRespool = respool_mocks.NewMockResourceManagerYARPCClient(suite.mockCtrl)
	suite.ctx = context.Background()
}

func (suite *commonTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *commonTestSuite) TestClient_LookupResourcePoolID() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
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
				Id: &peloton.ResourcePoolID{
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
		suite.mockRespool.EXPECT().LookupResourcePoolID(
			suite.ctx,
			gomock.Eq(tt.request)).
			Return(tt.response, tt.err)

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
