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
	"strconv"
	"testing"

	log "github.com/sirupsen/logrus"
	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/pkg/common/util"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	hostMocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
)

var (
	_cpuName     = "cpus"
	_pelotonRole = "peloton"
	_memName     = "mem"
	_diskName    = "disk"
	_gpuName     = "gpus"
	_portsName   = "ports"
	_testAgent   = "agent"

	_cpuRes = util.NewMesosResourceBuilder().
		WithName(_cpuName).
		WithValue(1.0).
		Build()
	_memRes = util.NewMesosResourceBuilder().
		WithName(_memName).
		WithValue(1.0).
		Build()
	_diskRes = util.NewMesosResourceBuilder().
			WithName(_diskName).
			WithValue(1.0).
			Build()
	_gpuRes = util.NewMesosResourceBuilder().
		WithName(_gpuName).
		WithValue(1.0).
		Build()
	_portsRes = util.NewMesosResourceBuilder().
			WithName(_portsName).
			WithRanges(util.CreatePortRanges(
			map[uint32]bool{1: true, 2: true})).
		Build()
)

type offersActionsTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	mockHostMgr *hostMocks.MockInternalHostServiceYARPCClient
	ctx         context.Context
}

func (suite *offersActionsTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockHostMgr = hostMocks.NewMockInternalHostServiceYARPCClient(suite.mockCtrl)
	suite.ctx = context.Background()
}

func (suite *offersActionsTestSuite) SetupTest() {
	log.Debug("SetupTest")
}

func (suite *offersActionsTestSuite) TearDownTest() {
	log.Debug("TearDownTest")
}

func (suite *offersActionsTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *offersActionsTestSuite) TestNoOutstandingOffers() {
	c := Client{
		Debug:         false,
		hostMgrClient: suite.mockHostMgr,
		dispatcher:    nil,
		ctx:           suite.ctx,
	}

	resp := &hostsvc.GetOutstandingOffersResponse{
		Offers: nil,
		Error: &hostsvc.GetOutstandingOffersResponse_Error{
			NoOffers: &hostsvc.NoOffersError{
				Message: "no offers present in offer pool",
			},
		},
	}

	suite.mockHostMgr.EXPECT().GetOutstandingOffers(
		gomock.Any(),
		&hostsvc.GetOutstandingOffersRequest{}).Return(resp, nil)

	suite.NoError(c.OffersGetAction())
}

func (suite *offersActionsTestSuite) TestGetOutstandingOffers() {
	c := Client{
		Debug:         false,
		hostMgrClient: suite.mockHostMgr,
		dispatcher:    nil,
		ctx:           suite.ctx,
	}

	resp := &hostsvc.GetOutstandingOffersResponse{
		Offers: suite.createUnreservedMesosOffers(5),
		Error:  nil,
	}

	suite.mockHostMgr.EXPECT().GetOutstandingOffers(
		gomock.Any(),
		&hostsvc.GetOutstandingOffersRequest{}).Return(resp, nil)

	suite.NoError(c.OffersGetAction())
}

func TestOffersAction(t *testing.T) {
	suite.Run(t, new(offersActionsTestSuite))
}

func (suite *offersActionsTestSuite) createUnreservedMesosOffer(
	offerID string) *mesos.Offer {
	rs := []*mesos.Resource{
		_cpuRes,
		_memRes,
		_diskRes,
		_gpuRes,
	}

	return &mesos.Offer{
		Id: &mesos.OfferID{
			Value: &offerID,
		},
		AgentId: &mesos.AgentID{
			Value: &_testAgent,
		},
		Hostname:  &_testAgent,
		Resources: rs,
	}
}

func (suite *offersActionsTestSuite) createUnreservedMesosOffers(count int) []*mesos.Offer {
	var offers []*mesos.Offer
	for i := 0; i < count; i++ {
		offers = append(offers, suite.createUnreservedMesosOffer("offer-id-"+strconv.Itoa(i)))
	}
	return offers
}
