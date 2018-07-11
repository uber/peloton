package util

import (
	"strconv"
	"testing"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"

	"code.uber.internal/infra/peloton/util"

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

	hostOfferMap := make(map[string][]*mesos.Offer)
	hostOfferMap["agent"] = offerList
	hostOffer := MesosOffersToHostOffers(hostOfferMap)
	assert.Equal(t, len(hostOffer), 0)

	for _, o := range offers {
		offerList = append(offerList, o)
	}
	hostOfferMap["agent"] = offerList
	hostOffer = MesosOffersToHostOffers(hostOfferMap)
	assert.Equal(t, len(hostOffer), 1)
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
