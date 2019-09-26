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

package cli

import (
	"context"
	"errors"
	"io/ioutil"
	"sort"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	respool_mocks "github.com/uber/peloton/.gen/peloton/api/v0/respool/mocks"
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

func (suite *commonTestSuite) TestClient_ExtractHostnames() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	// input should be trimmed
	hosts, err := c.ExtractHostnames("a, b,c ", ",")
	suite.NoError(err)
	sort.Strings(hosts)
	suite.Equal("a", hosts[0])
	suite.Equal("b", hosts[1])
	suite.Equal("c", hosts[2])

	// empty input
	_, err = c.ExtractHostnames("", ",")
	suite.Error(err)
	suite.Equal(errors.New("host cannot be empty"), err)

	// duplicate input
	_, err = c.ExtractHostnames("a,a", ",")
	suite.Error(err)
	suite.Equal(errors.New("invalid input. Duplicate entry for host a found"), err)

	// input should be sorted
	hosts, err = c.ExtractHostnames("b, c,a ", ",")
	suite.NoError(err)
	sort.Strings(hosts)
	suite.Equal("a", hosts[0])
	suite.Equal("b", hosts[1])
	suite.Equal("c", hosts[2])
}

func (suite *commonTestSuite) TestConfirm() {
	tt := []struct {
		input       string
		wantConfirm bool
	}{
		{
			input:       "y",
			wantConfirm: true,
		},
		{
			input:       "Y",
			wantConfirm: true,
		},
		{
			input:       "yes",
			wantConfirm: true,
		},
		{
			input:       "n",
			wantConfirm: false,
		},
		{
			input:       "No",
			wantConfirm: false,
		},
		{
			input:       "false",
			wantConfirm: false,
		},
	}

	for _, t := range tt {
		resp := confirm("", strings.NewReader(t.input), ioutil.Discard)
		suite.Equal(t.wantConfirm, resp)
	}
}

func TestCommon(t *testing.T) {
	suite.Run(t, new(commonTestSuite))
}
