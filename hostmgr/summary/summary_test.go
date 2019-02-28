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

package summary

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/volume"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/common"
	"github.com/uber/peloton/common/constraints"
	constraint_mocks "github.com/uber/peloton/common/constraints/mocks"
	"github.com/uber/peloton/hostmgr/scalar"
	store_mocks "github.com/uber/peloton/storage/mocks"
	"github.com/uber/peloton/util"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

var (
	_testKey0     = "testkey0"
	_testKey1     = "testkey1"
	_testValue0   = "testvalue0"
	_testValue1   = "testvalue1"
	_testAgent    = "agent"
	_testAgent1   = "agent1"
	_testAgent2   = "agent2"
	_dummyOfferID = "dummyofferid"

	_defaultResValue = 1.0

	_cpuRes = util.NewMesosResourceBuilder().
		WithName(common.MesosCPU).
		WithValue(1.0).
		Build()
	_cpuRevocableRes = util.NewMesosResourceBuilder().
				WithName(common.MesosCPU).
				WithValue(1.0).
				WithRevocable(&mesos.Resource_RevocableInfo{}).
				Build()
	_memRes = util.NewMesosResourceBuilder().
		WithName(common.MesosMem).
		WithValue(1.0).
		Build()
	_memRevocableRes = util.NewMesosResourceBuilder().
				WithName(common.MesosMem).
				WithValue(1.0).
				WithRevocable(&mesos.Resource_RevocableInfo{}).
				Build()
	_diskRes = util.NewMesosResourceBuilder().
			WithName(common.MesosDisk).
			WithValue(1.0).
			Build()
	_gpuRes = util.NewMesosResourceBuilder().
		WithName(common.MesosGPU).
		WithValue(1.0).
		Build()
	_portsRes = util.NewMesosResourceBuilder().
			WithName(common.MesosPorts).
			WithRanges(util.CreatePortRanges(
			map[uint32]bool{1: true, 2: true})).
		Build()
	supportedSlackResourceTypes = []string{common.MesosCPU}
)

type HostOfferSummaryTestSuite struct {
	suite.Suite

	offer           *mesos.Offer
	labels1         *mesos.Labels
	labels2         *mesos.Labels
	ctrl            *gomock.Controller
	mockVolumeStore *store_mocks.MockPersistentVolumeStore
}

func (suite *HostOfferSummaryTestSuite) SetupSuite() {
	suite.labels1 = &mesos.Labels{
		Labels: []*mesos.Label{
			{
				Key:   &_testKey0,
				Value: &_testValue0,
			},
		},
	}
	suite.labels2 = &mesos.Labels{
		Labels: []*mesos.Label{
			{
				Key:   &_testKey1,
				Value: &_testValue1,
			},
		},
	}
}

func (suite *HostOfferSummaryTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.mockVolumeStore = store_mocks.NewMockPersistentVolumeStore(suite.ctrl)
}

func (suite *HostOfferSummaryTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func (suite *HostOfferSummaryTestSuite) createResourceConfig(cpus, gpus, mem, disk float64) *task.ResourceConfig {
	return &task.ResourceConfig{
		CpuLimit:    cpus,
		MemLimitMb:  mem,
		DiskLimitMb: disk,
		GpuLimit:    gpus,
	}
}

func (suite *HostOfferSummaryTestSuite) createResources(cpus, gpus, mem, disk float64) []*mesos.Resource {
	return []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName(common.MesosCPU).
			WithValue(cpus).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosCPU).
			WithValue(cpus).
			WithRevocable(&mesos.Resource_RevocableInfo{}).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosMem).
			WithValue(mem).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosDisk).
			WithValue(disk).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosGPU).
			WithValue(gpus).
			Build(),
	}
}

func (suite *HostOfferSummaryTestSuite) createAgentInfo(agentID string, cpus, gpus, mem, disk float64) *mesos.AgentInfo {
	return &mesos.AgentInfo{
		Id: &mesos.AgentID{
			Value: &agentID,
		},
		Resources: suite.createResources(cpus, gpus, mem, disk),
	}
}

func (suite *HostOfferSummaryTestSuite) createReservedMesosOffer(
	offerID string, hasPersistentVolume bool) *mesos.Offer {

	reservation1 := &mesos.Resource_ReservationInfo{
		Labels: suite.labels1,
	}
	reservation2 := &mesos.Resource_ReservationInfo{
		Labels: suite.labels2,
	}
	diskInfo := &mesos.Resource_DiskInfo{
		Persistence: &mesos.Resource_DiskInfo_Persistence{
			Id: &offerID,
		},
	}
	rs := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName(common.MesosCPU).
			WithValue(1.0).
			WithRole(common.PelotonRole).
			WithReservation(reservation1).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosMem).
			WithValue(2.0).
			WithReservation(reservation2).
			WithRole(common.PelotonRole).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosGPU).
			WithValue(5.0).
			Build(),
	}

	if hasPersistentVolume {
		rs = append(
			rs,
			util.NewMesosResourceBuilder().
				WithName(common.MesosDisk).
				WithValue(3.0).
				WithRole(common.PelotonRole).
				WithReservation(reservation1).
				WithDisk(diskInfo).
				Build())
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

func (suite *HostOfferSummaryTestSuite) createReservedMesosOffers(count int, hasPersistentVolume bool) []*mesos.Offer {
	var offers []*mesos.Offer
	for i := 0; i < count; i++ {
		offers = append(offers, suite.createReservedMesosOffer("offer-id-"+strconv.Itoa(i), hasPersistentVolume))
	}
	return offers
}

func (suite *HostOfferSummaryTestSuite) createUnreservedMesosOffer(
	offerID string) *mesos.Offer {
	rs := []*mesos.Resource{
		_cpuRes,
		_memRes,
		_diskRes,
		_gpuRes,
		_cpuRevocableRes,
		_memRevocableRes,
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

func (suite *HostOfferSummaryTestSuite) createUnreservedMesosOffers(count int) []*mesos.Offer {
	var offers []*mesos.Offer
	for i := 0; i < count; i++ {
		offers = append(offers, suite.createUnreservedMesosOffer("offer-id-"+strconv.Itoa(i)))
	}
	return offers
}

func TestHostOfferSummaryTestSuite(t *testing.T) {
	suite.Run(t, new(HostOfferSummaryTestSuite))
}

func (suite *HostOfferSummaryTestSuite) TestScarceResourcesConstraint() {

	scarceResourceType1 := []string{"GPU"}
	scarceResourceType2 := []string{}
	scarceResourceType3 := []string{"GPU", "DUMMY_RES"}
	agent1 := suite.createAgentInfo(_testAgent1, 5.0, 5.0, 5.0, 5.0)
	agent2 := suite.createAgentInfo(_testAgent2, 5.0, 0, 5.0, 5.0)

	testTable := []struct {
		msg                string
		expected           hostsvc.HostFilterResult
		filter             *hostsvc.HostFilter
		agent              *mesos.AgentInfo
		offer              *mesos.Offer
		scarceResourceType []string
	}{
		{
			msg:      "Not Enough CPU Resources.",
			expected: hostsvc.HostFilterResult_INSUFFICIENT_OFFER_RESOURCES,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					NumPorts: uint32(2),
					Minimum:  suite.createResourceConfig(2.0, 1.0, 1.0, 1.0),
				},
			},
			agent: agent1,
			offer: &mesos.Offer{
				AgentId:   agent1.Id,
				Resources: []*mesos.Resource{_cpuRes, _memRes, _diskRes, _gpuRes, _portsRes},
			},
			scarceResourceType: scarceResourceType1,
		},
		{
			msg:      "Not enough memory",
			expected: hostsvc.HostFilterResult_INSUFFICIENT_OFFER_RESOURCES,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: suite.createResourceConfig(1.0, 1.0, 2.0, 1.0),
				},
			},
			agent: agent1,
			offer: &mesos.Offer{
				AgentId:   agent1.Id,
				Resources: []*mesos.Resource{_cpuRes, _memRes, _diskRes, _gpuRes},
			},
			scarceResourceType: scarceResourceType1,
		},
		{
			msg:      "Not enough ports",
			expected: hostsvc.HostFilterResult_INSUFFICIENT_OFFER_RESOURCES,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					NumPorts: uint32(3),
					Minimum:  suite.createResourceConfig(1.0, 0, 1.0, 1.0),
				},
			},
			agent: agent1,
			offer: &mesos.Offer{
				AgentId:   agent1.Id,
				Resources: []*mesos.Resource{_cpuRes, _memRes, _diskRes, _portsRes},
			},
			scarceResourceType: scarceResourceType2,
		},
		{
			msg:      "not enough GPU",
			expected: hostsvc.HostFilterResult_INSUFFICIENT_OFFER_RESOURCES,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					NumPorts: uint32(2),
					Minimum:  suite.createResourceConfig(1.0, 1.0, 1.0, 1.0),
				},
			},
			agent: agent1,
			offer: &mesos.Offer{
				AgentId:   agent1.Id,
				Resources: []*mesos.Resource{_cpuRes, _memRes, _diskRes, _portsRes},
			},
			scarceResourceType: scarceResourceType1,
		},
		{
			msg:      "Enough resource with GPU, with scarce_resource_types set",
			expected: hostsvc.HostFilterResult_MATCH,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					NumPorts: uint32(2),
					Minimum:  suite.createResourceConfig(1.0, 1.0, 1.0, 1.0),
				},
			},
			agent: agent1,
			offer: &mesos.Offer{
				AgentId:   agent1.Id,
				Resources: []*mesos.Resource{_cpuRes, _memRes, _diskRes, _gpuRes, _portsRes},
			},
			scarceResourceType: scarceResourceType1,
		},
		{
			msg:      "Enough resource with GPU, without scarce_resource_types set",
			expected: hostsvc.HostFilterResult_MATCH,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					NumPorts: uint32(2),
					Minimum:  suite.createResourceConfig(1.0, 1.0, 1.0, 1.0),
				},
			},
			agent: agent1,
			offer: &mesos.Offer{
				AgentId:   agent1.Id,
				Resources: []*mesos.Resource{_cpuRes, _memRes, _diskRes, _gpuRes, _portsRes},
			},
			scarceResourceType: scarceResourceType2,
		},
		{
			msg:      "Enough resource without GPU, with scarce_resource_types set",
			expected: hostsvc.HostFilterResult_MATCH,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: suite.createResourceConfig(1.0, 0, 1.0, 1.0),
				},
			},
			agent: agent2,
			offer: &mesos.Offer{
				AgentId:   agent2.Id,
				Resources: []*mesos.Resource{_cpuRes, _memRes, _diskRes},
			},
			scarceResourceType: scarceResourceType1,
		},
		{
			msg:      "Enough resource without GPU, without scarce_resource_types set",
			expected: hostsvc.HostFilterResult_MATCH,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: suite.createResourceConfig(1.0, 0, 1.0, 1.0),
				},
			},
			agent: agent2,
			offer: &mesos.Offer{
				AgentId:   agent2.Id,
				Resources: []*mesos.Resource{_cpuRes, _memRes, _diskRes},
			},
			scarceResourceType: scarceResourceType2,
		},
		{
			msg:      "GPU machines are exclusive",
			expected: hostsvc.HostFilterResult_SCARCE_RESOURCES,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: suite.createResourceConfig(1.0, 0, 1.0, 1.0),
				},
			},
			agent: agent1,
			offer: &mesos.Offer{
				AgentId:   agent1.Id,
				Resources: []*mesos.Resource{_cpuRes, _memRes, _diskRes, _gpuRes},
			},
			scarceResourceType: scarceResourceType1,
		},
		{
			msg:      "Adding DUMMY_RES for non-GPU task does not impact scheduling",
			expected: hostsvc.HostFilterResult_MATCH,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: suite.createResourceConfig(1.0, 0, 1.0, 1.0),
				},
			},
			agent: agent2,
			offer: &mesos.Offer{
				AgentId:   agent2.Id,
				Resources: []*mesos.Resource{_cpuRes, _memRes, _diskRes},
			},
			scarceResourceType: scarceResourceType3,
		},
		{
			msg:      "Adding DUMMY_RES for GPU task does not impact scheduling",
			expected: hostsvc.HostFilterResult_MATCH,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: suite.createResourceConfig(1.0, 1.0, 1.0, 1.0),
				},
			},
			agent: agent1,
			offer: &mesos.Offer{
				AgentId:   agent1.Id,
				Resources: []*mesos.Resource{_cpuRes, _memRes, _diskRes, _gpuRes},
			},
			scarceResourceType: scarceResourceType3,
		},
		{
			msg:      "Adding DUMMY_RES does not impact scheduling",
			expected: hostsvc.HostFilterResult_MATCH,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: suite.createResourceConfig(1.0, 0, 1.0, 1.0),
				},
			},
			agent: agent2,
			offer: &mesos.Offer{
				AgentId:   agent2.Id,
				Resources: []*mesos.Resource{_cpuRes, _memRes, _diskRes},
			},
			scarceResourceType: scarceResourceType3,
		},
		{
			msg:      "Empty offer map",
			expected: hostsvc.HostFilterResult_NO_OFFER,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: suite.createResourceConfig(1.0, 0, 1.0, 1.0),
				},
			},
			agent:              agent1,
			offer:              nil,
			scarceResourceType: scarceResourceType1,
		},
	}

	for _, tt := range testTable {
		offerMap := make(map[string]*mesos.Offer)

		if tt.offer != nil {
			offerMap["o1"] = tt.offer
		}

		suite.Equal(
			tt.expected,
			matchHostFilter(
				offerMap,
				tt.filter,
				nil,
				scalar.FromMesosResources(tt.agent.GetResources()),
				tt.scarceResourceType),
			tt.msg,
		)
	}
}

func (suite *HostOfferSummaryTestSuite) TestSlackResourcesConstraint() {
	defer suite.ctrl.Finish()

	seqIDGenerator := func(i string) func() string {
		return func() string {
			return i
		}
	}

	testTable := map[string]struct {
		initialStatus HostStatus
		afterStatus   HostStatus
		revocable     bool
		resMultiplier float64
		wantResult    hostsvc.HostFilterResult
		offerID       string
	}{
		"matched-revocable-resources": {
			wantResult: hostsvc.
				HostFilterResult_MATCH,
			initialStatus: ReadyHost,
			afterStatus:   PlacingHost,
			revocable:     true,
			resMultiplier: 1.0,
			offerID:       "1",
		},
		"matched-nonrevocable-resources": {
			wantResult: hostsvc.
				HostFilterResult_MATCH,
			initialStatus: ReadyHost,
			afterStatus:   PlacingHost,
			revocable:     false,
			resMultiplier: 1.0,
			offerID:       "2",
		},
		"not-matched-revocable-resources": {
			wantResult: hostsvc.
				HostFilterResult_INSUFFICIENT_OFFER_RESOURCES,
			initialStatus: ReadyHost,
			afterStatus:   ReadyHost,
			revocable:     true,
			resMultiplier: 7.0,
			offerID:       emptyOfferID,
		},
		"not-matched-nonrevocable-resources": {
			wantResult: hostsvc.
				HostFilterResult_INSUFFICIENT_OFFER_RESOURCES,
			initialStatus: ReadyHost,
			afterStatus:   ReadyHost,
			revocable:     false,
			resMultiplier: 7.0,
			offerID:       emptyOfferID,
		},
	}

	for ttName, tt := range testTable {
		offers := suite.createUnreservedMesosOffers(5)
		s := New(
			suite.mockVolumeStore,
			nil,
			offers[0].GetHostname(),
			supportedSlackResourceTypes,
		).(*hostSummary)
		s.offerIDgenerator = seqIDGenerator(tt.offerID)
		s.status = tt.initialStatus

		suite.Equal(tt.initialStatus, s.AddMesosOffers(context.Background(), offers))

		filter := &hostsvc.HostFilter{
			Quantity: &hostsvc.QuantityControl{
				MaxHosts: uint32(1),
			},
			ResourceConstraint: &hostsvc.ResourceConstraint{
				Minimum: &task.ResourceConfig{
					CpuLimit:    _defaultResValue * tt.resMultiplier,
					MemLimitMb:  _defaultResValue,
					DiskLimitMb: _defaultResValue,
				},
				Revocable: tt.revocable,
			},
		}

		match := s.TryMatch(filter, nil)
		suite.Equal(tt.wantResult, match.Result,
			"test case is %s", ttName)

		suite.Equal(
			tt.offerID,
			s.hostOfferID,
			"test case is %s", ttName)

		if match.Result != hostsvc.HostFilterResult_MATCH {
			suite.Nil(match.Offer, "test case is %s", ttName)
		}

		_, _, afterStatus := s.UnreservedAmount()
		suite.Equal(tt.afterStatus, afterStatus, "test case is %s", ttName)
	}
}

func (suite *HostOfferSummaryTestSuite) TestTryMatchSchedulingConstraint() {
	defer suite.ctrl.Finish()
	offer := suite.createUnreservedMesosOffer("offer-id")
	offers := suite.createUnreservedMesosOffers(5)

	seqIDGenerator := func(i string) func() string {
		return func() string {
			return i
		}
	}

	testTable := map[string]struct {
		wantResult     hostsvc.HostFilterResult
		expectedOffers []*mesos.Offer
		offerID        string

		evaluateRes constraints.EvaluateResult
		evaluateErr error

		initialStatus HostStatus
		afterStatus   HostStatus
		noMock        bool

		initialOffers []*mesos.Offer
	}{
		"matched-correctly": {
			wantResult:     hostsvc.HostFilterResult_MATCH,
			expectedOffers: offers,
			evaluateRes:    constraints.EvaluateResultMatch,
			initialStatus:  ReadyHost,
			afterStatus:    PlacingHost,
			initialOffers:  offers,
			offerID:        "1",
		},
		"matched-not-applicable": {
			wantResult:     hostsvc.HostFilterResult_MATCH,
			expectedOffers: []*mesos.Offer{offer},
			evaluateRes:    constraints.EvaluateResultNotApplicable,
			initialStatus:  ReadyHost,
			afterStatus:    PlacingHost,
			initialOffers:  []*mesos.Offer{offer},
			offerID:        "2",
		},
		"mismatched-constraint": {
			wantResult:    hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS,
			evaluateRes:   constraints.EvaluateResultMismatch,
			initialStatus: ReadyHost,
			afterStatus:   ReadyHost,
			initialOffers: []*mesos.Offer{offer},
			offerID:       emptyOfferID,
		},
		"mismatched-error": {
			wantResult:    hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS,
			evaluateErr:   errors.New("some error"),
			initialStatus: ReadyHost,
			afterStatus:   ReadyHost,
			initialOffers: []*mesos.Offer{offer},
			offerID:       emptyOfferID,
		},
		"mismatched-no-offer-placing-status": {
			wantResult:    hostsvc.HostFilterResult_MISMATCH_STATUS,
			initialStatus: PlacingHost,
			afterStatus:   PlacingHost,
			noMock:        true, // mockEvaluator should not be called in this case.
			initialOffers: []*mesos.Offer{},
			offerID:       emptyOfferID,
		},
		"mismatched-no-offer-ready-status": {
			wantResult:    hostsvc.HostFilterResult_NO_OFFER,
			initialStatus: ReadyHost,
			afterStatus:   ReadyHost,
			noMock:        true, // mockEvaluator should not be called in this case.
			initialOffers: []*mesos.Offer{},
			offerID:       emptyOfferID,
		},
		"mismatched-mismatch-status": {
			wantResult:    hostsvc.HostFilterResult_MISMATCH_STATUS,
			initialStatus: PlacingHost,
			afterStatus:   PlacingHost,
			noMock:        true, // mockEvaluator should not be called in this case.
			initialOffers: []*mesos.Offer{offer},
			offerID:       emptyOfferID,
		},
	}

	for ttName, tt := range testTable {
		ctrl := gomock.NewController(suite.T())
		mockEvaluator := constraint_mocks.NewMockEvaluator(ctrl)

		s := New(
			suite.mockVolumeStore,
			nil,
			offer.GetHostname(),
			supportedSlackResourceTypes).(*hostSummary)
		s.status = tt.initialStatus
		s.offerIDgenerator = seqIDGenerator(tt.offerID)

		suite.Equal(
			tt.initialStatus,
			s.AddMesosOffers(context.Background(),
				tt.initialOffers),
		)

		filter := &hostsvc.HostFilter{
			SchedulingConstraint: &task.Constraint{
				Type: task.Constraint_LABEL_CONSTRAINT,
				LabelConstraint: &task.LabelConstraint{
					Kind: task.LabelConstraint_TASK,
				},
			},
		}

		lv := constraints.GetHostLabelValues(_testAgent, offer.Attributes)

		if !tt.noMock {
			mockEvaluator.
				EXPECT().
				Evaluate(
					gomock.Eq(filter.SchedulingConstraint),
					gomock.Eq(lv)).
				Return(tt.evaluateRes, tt.evaluateErr)
		}

		match := s.TryMatch(filter, mockEvaluator)
		suite.Equal(tt.wantResult, match.Result,
			"test case is %s", ttName)

		suite.Equal(
			tt.offerID,
			s.hostOfferID,
			"test case is %s", ttName)

		if tt.wantResult != hostsvc.HostFilterResult_MATCH {
			suite.Nil(match.Offer, "test case is %s", ttName)
		}
		_, _, afterStatus := s.UnreservedAmount()
		suite.Equal(tt.afterStatus, afterStatus, "test case is %s", ttName)
	}
}

func (suite *HostOfferSummaryTestSuite) TestAddRemoveHybridOffers() {
	defer suite.ctrl.Finish()
	// Add offer concurrently.
	reservedOffers := 5
	unreservedOffers := 5
	nOffers := reservedOffers + unreservedOffers
	wg := sync.WaitGroup{}
	wg.Add(nOffers)

	hybridSummary := New(suite.mockVolumeStore, nil, _testAgent, supportedSlackResourceTypes).(*hostSummary)

	suite.False(hybridSummary.HasOffer())
	suite.False(hybridSummary.HasAnyOffer())
	suite.Equal(hybridSummary.readyCount.Load(), int32(0))

	// Try to remove non-existent offer.
	status, offer := hybridSummary.RemoveMesosOffer(_dummyOfferID, "Offer is expired")
	suite.Equal(status, ReadyHost)
	suite.Nil(offer)

	var offers []*mesos.Offer
	for i := 0; i < reservedOffers; i++ {
		offerID := fmt.Sprintf("reserved-%d", i)
		offers = append(offers, suite.createReservedMesosOffer(offerID, true /* hasPersistentVolume */))
	}
	for i := 0; i < unreservedOffers; i++ {
		offerID := fmt.Sprintf("unreserved-%d", i)
		offers = append(offers, suite.createUnreservedMesosOffer(offerID))
	}

	volumeInfo := &volume.PersistentVolumeInfo{}

	suite.mockVolumeStore.EXPECT().
		GetPersistentVolume(context.Background(), gomock.Any()).
		AnyTimes().
		Return(volumeInfo, nil)
	suite.mockVolumeStore.EXPECT().
		UpdatePersistentVolume(context.Background(), gomock.Any()).
		AnyTimes().
		Return(nil)

	status = hybridSummary.AddMesosOffers(context.Background(), offers)

	// Verify aggregated resources for reserved part.
	suite.Equal(reservedOffers, len(hybridSummary.reservedOffers))
	suite.Equal(unreservedOffers, len(hybridSummary.unreservedOffers))

	// Verify resources for unreserved part.
	suite.True(hybridSummary.HasOffer())
	suite.True(hybridSummary.HasAnyOffer())
	suite.Equal(hybridSummary.readyCount.Load(), int32(5))
	unreservedAmount, revocableUnreservedAmount, status := hybridSummary.UnreservedAmount()
	suite.Equal(5.0, unreservedAmount.CPU)
	suite.Equal(5.0, revocableUnreservedAmount.CPU)
	suite.Equal(5.0, unreservedAmount.Mem)
	suite.Equal(5.0, unreservedAmount.Disk)
	suite.Equal(5.0, unreservedAmount.GPU)

	suite.Equal(ReadyHost, status)

	// Remove offer concurrently.
	wg = sync.WaitGroup{}
	wg.Add(nOffers)
	for _, offer := range offers {
		go func(offer *mesos.Offer) {
			defer wg.Done()

			status, offer := hybridSummary.RemoveMesosOffer(*offer.Id.Value, "Offer is rescinded")
			suite.Equal(ReadyHost, status)
			suite.NotNil(offer)
		}(offer)
	}
	wg.Wait()

	// Verify aggregated resources.
	suite.Empty(hybridSummary.reservedOffers)
	suite.Empty(hybridSummary.unreservedOffers)
	suite.Equal(hybridSummary.readyCount.Load(), int32(0))
	suite.Equal(ReadyHost, hybridSummary.status)

	hybridSummary.AddMesosOffers(context.Background(), offers)

	// Verify aggregated resources for reserved part.
	suite.Equal(reservedOffers, len(hybridSummary.reservedOffers))
	suite.Equal(unreservedOffers, len(hybridSummary.unreservedOffers))

	// Verify resources for unreserved part.
	suite.True(hybridSummary.HasOffer())
	suite.True(hybridSummary.HasAnyOffer())
	suite.Equal(hybridSummary.readyCount.Load(), int32(5))
	unreservedAmount, revocableUnreservedAmount, status = hybridSummary.UnreservedAmount()
	suite.Equal(5.0, unreservedAmount.CPU)
	suite.Equal(5.0, revocableUnreservedAmount.CPU)
	suite.Equal(5.0, unreservedAmount.Mem)
	suite.Equal(5.0, unreservedAmount.Disk)
	suite.Equal(5.0, unreservedAmount.GPU)
	summaryOffers := hybridSummary.GetOffers(Reserved)
	suite.Equal(len(summaryOffers), 5)
	summaryOffers = hybridSummary.GetOffers(Unreserved)
	suite.Equal(len(summaryOffers), 5)
	summaryOffers = hybridSummary.GetOffers(All)
	suite.Equal(len(summaryOffers), 10)

	suite.Equal(ReadyHost, status)
}

func (suite *HostOfferSummaryTestSuite) TestResetExpiredPlacingOfferStatus() {
	defer suite.ctrl.Finish()

	now := time.Now()
	offers := suite.createUnreservedMesosOffers(5)
	hostname := offers[0].GetHostname()

	testTable := []struct {
		initialStatus                HostStatus
		statusPlacingOfferExpiration time.Time
		resetExpected                bool
		readyCount                   int
		msg                          string
	}{
		{
			initialStatus:                ReadyHost,
			statusPlacingOfferExpiration: now,
			resetExpected:                false,
			readyCount:                   5,
			msg:                          "HostSummary in ReadyOffer status",
		},
		{
			initialStatus:                PlacingHost,
			statusPlacingOfferExpiration: now.Add(10 * time.Minute),
			resetExpected:                false,
			readyCount:                   0,
			msg:                          "HostSummary in PlacingOffer status, has not timed out",
		},
		{
			initialStatus:                PlacingHost,
			statusPlacingOfferExpiration: now.Add(-10 * time.Minute),
			resetExpected:                true,
			readyCount:                   5,
			msg:                          "HostSummary in PlacingOffer status, has timed out",
		},
	}

	for _, tt := range testTable {
		s := New(suite.mockVolumeStore, nil, hostname, supportedSlackResourceTypes).(*hostSummary)
		s.status = tt.initialStatus
		s.statusPlacingOfferExpiration = tt.statusPlacingOfferExpiration
		s.AddMesosOffers(context.Background(), offers)

		reset, _ := s.ResetExpiredPlacingOfferStatus(now)
		suite.Equal(tt.resetExpected, reset, tt.msg)
		suite.Equal(s.readyCount.Load(), int32(tt.readyCount), tt.msg)
		if tt.resetExpected {
			suite.Equal(emptyOfferID, s.hostOfferID)
		}
	}

	s := New(suite.mockVolumeStore, nil, hostname, supportedSlackResourceTypes).(*hostSummary)
	s.AddMesosOffers(context.Background(), offers)
	s.statusPlacingOfferExpiration = now.Add(-10 * time.Minute)
	invalidCacheStatus := s.CasStatus(PlacingHost, ReadyHost)
	suite.NotNil(invalidCacheStatus)

	// Setting placing offers, without resetting readyCount (represents outstanding unreserved offers) to zero
	s.CasStatus(s.status, PlacingHost)
	suite.NotEqual(emptyOfferID, s.hostOfferID)
	suite.Equal(s.readyCount.Load(), int32(0))
	s.readyCount.Store(int32(5))
	reset, _ := s.ResetExpiredPlacingOfferStatus(now)
	suite.Equal(false, reset,
		"This is negative test, were time has elapsed but Cache Status "+
			"for Host Summary is not reset from Placing -> Ready")
}

func (suite *HostOfferSummaryTestSuite) TestClaimForUnreservedOffersForLaunch() {
	defer suite.ctrl.Finish()
	offers := suite.createUnreservedMesosOffers(5)
	offers = append(offers, suite.createReservedMesosOffer(
		"reserved-offerid-1", false))

	testTable := []struct {
		name               string
		initialStatus      HostStatus
		afterStatus        HostStatus
		offerID            string
		expectedReadyCount int32
		err                error
	}{
		{
			name:               "host in ready state should not return offers",
			initialStatus:      ReadyHost,
			afterStatus:        ReadyHost,
			offerID:            offers[0].GetId().GetValue(),
			expectedReadyCount: 5,
			err:                errors.New("host status is not Placing"),
		},
		{
			name:               "host in placing state should return offers",
			initialStatus:      PlacingHost,
			afterStatus:        ReadyHost,
			offerID:            offers[0].GetId().GetValue(),
			expectedReadyCount: 0,
			err:                nil,
		},
		{
			name: "host in placing state with different offer id should not " +
				"return offers",
			initialStatus:      PlacingHost,
			afterStatus:        PlacingHost,
			offerID:            "does-not-exist",
			expectedReadyCount: 5,
			err:                errors.New("host offer id does not match"),
		},
	}

	for _, tt := range testTable {
		s := New(
			suite.mockVolumeStore,
			nil,
			offers[0].GetHostname(),
			supportedSlackResourceTypes).(*hostSummary)
		s.AddMesosOffers(context.Background(), offers)
		suite.Equal(s.readyCount.Load(), int32(len(offers)-1))
		s.status = tt.initialStatus
		s.hostOfferID = tt.offerID

		_, err := s.ClaimForLaunch(offers[0].GetId().GetValue())
		if err != nil {
			suite.Equal(err.Error(), tt.err.Error(), tt.name)
		}
		suite.Equal(s.status, tt.afterStatus, tt.name)
		suite.Equal(s.readyCount.Load(), tt.expectedReadyCount, tt.name)
		summaryOffers := s.GetOffers(Unreserved)
		suite.Equal(int32(len(summaryOffers)), tt.expectedReadyCount, tt.name)
		s.RemoveMesosOffer("reserved-offerid-1",
			"Removing reserved offer")
		summaryOffers = s.GetOffers(Reserved)
		suite.Equal(len(summaryOffers), 0, tt.name)
	}
}

func (suite *HostOfferSummaryTestSuite) TestClaimForReservedOffersForLaunch() {
	defer suite.ctrl.Finish()
	offers := suite.createReservedMesosOffers(5, true)
	offers = append(offers, suite.createUnreservedMesosOffer("unreserved-offerid-1"))

	s := New(suite.mockVolumeStore, nil, offers[0].GetHostname(), supportedSlackResourceTypes).(*hostSummary)

	s.AddMesosOffers(context.Background(), offers)
	suite.Equal(int(s.readyCount.Load()), 1)

	s.ClaimReservedOffersForLaunch()
	suite.Equal(s.GetHostStatus(), ReadyHost)
	suite.Equal(int(s.readyCount.Load()), 1)
	summaryOffers := s.GetOffers(Reserved)
	suite.Equal(len(summaryOffers), 0)
}
