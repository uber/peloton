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

package util

import (
	"strconv"
	"testing"

	mesos "github.com/uber/peloton/.gen/mesos/v1"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"

	"github.com/stretchr/testify/assert"
)

const (
	_cpuName  = "cpus"
	_memName  = "mem"
	_diskName = "disk"
	_gpuName  = "gpus"
)

var (
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
	_testAgent = "agent"
)

// TestLabelKeyToEnvVarName tests LabelKeyToEnvVarName
func TestLabelKeyToEnvVarName(t *testing.T) {
	assert.Equal(t, "PELOTON_JOB_ID", LabelKeyToEnvVarName("peloton.job_id"))
}

// TestMesosOffersToHostOffers tests MesosOffersToHostOffers where taking
// the host to offer map and returning the hostsvc.HostOffer
func TestMesosOffersToHostOffers(t *testing.T) {
	offers := createUnreservedMesosOffers(2)
	var offerList []*mesos.Offer

	hostOfferID := "uniq_offer_id"
	offerList = make([]*mesos.Offer, 0)
	hostOffer := MesosOffersToHostOffer(hostOfferID, offerList)
	assert.Nil(t, hostOffer)

	for _, o := range offers {
		offerList = append(offerList, o)
	}
	hostOffer = MesosOffersToHostOffer(hostOfferID, offerList)
	assert.NotNil(t, hostOffer)
}

func TestIsSlackResourceType(t *testing.T) {
	slackResourceType := []string{common.MesosCPU, common.MesosMem}
	assert.False(t, IsSlackResourceType(common.MesosDisk, slackResourceType))

	assert.True(t, IsSlackResourceType(common.MesosCPU, slackResourceType))
}

func TestGetResourcesFromOffers(t *testing.T) {
	offers := createUnreservedMesosOffers(1)
	resource := GetResourcesFromOffers(offers)
	assert.Equal(t, resource.GetCPU(), float64(1))
	assert.Equal(t, resource.GetMem(), float64(1))
}

func createUnreservedMesosOffer(
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

func createUnreservedMesosOffers(count int) map[string]*mesos.Offer {
	offers := make(map[string]*mesos.Offer)
	for i := 0; i < count; i++ {
		offerID := "offer-id-" + strconv.Itoa(i)
		offers[offerID] = createUnreservedMesosOffer(offerID)
	}
	return offers
}

// TestHasExclusiveAttribute tests function HasExclusiveAttribute
func TestHasExclusiveAttribute(t *testing.T) {
	exclName := common.PelotonExclusiveAttributeName
	otherName := "rack"
	textType := mesos.Value_TEXT
	tv1 := "redis"
	tv2 := "docker"

	excl1 := &mesos.Attribute{
		Name: &exclName,
		Type: &textType,
		Text: &mesos.Value_Text{
			Value: &tv1,
		},
	}
	excl2 := &mesos.Attribute{
		Name: &exclName,
		Type: &textType,
		Text: &mesos.Value_Text{
			Value: &tv2,
		},
	}
	other1 := &mesos.Attribute{
		Name: &otherName,
		Type: &textType,
		Text: &mesos.Value_Text{
			Value: &tv1,
		},
	}
	other2 := &mesos.Attribute{
		Name: &otherName,
		Type: &textType,
		Text: &mesos.Value_Text{
			Value: &tv2,
		},
	}

	testTable := []struct {
		msg        string
		attributes []*mesos.Attribute
		expected   bool
	}{
		{
			msg:        "all exclusive attributes",
			attributes: []*mesos.Attribute{excl1, excl2},
			expected:   true,
		},
		{
			msg:        "some exclusive attributes",
			attributes: []*mesos.Attribute{other1, other2, excl2},
			expected:   true,
		},
		{
			msg:        "no exclusive attributes",
			attributes: []*mesos.Attribute{other1, other2},
			expected:   false,
		},
	}
	for _, tc := range testTable {
		assert.Equal(
			t,
			tc.expected,
			HasExclusiveAttribute(tc.attributes),
			tc.msg)
	}
}
