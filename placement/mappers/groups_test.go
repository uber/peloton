package mappers

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"github.com/stretchr/testify/assert"
)

func TestGroupMapper_Convert(t *testing.T) {
	attribute := "attribute"
	text := "text"
	cpus := "cpus"
	scalar := 42.0
	textValueType := mesos_v1.Value_TEXT
	offer := &hostsvc.HostOffer{
		Hostname: "hostname",
		Attributes: []*mesos_v1.Attribute{
			{
				Name: &attribute,
				Type: &textValueType,
				Text: &mesos_v1.Value_Text{
					Value: &text,
				},
			},
		},
		Resources: []*mesos_v1.Resource{
			{
				Name: &cpus,
				Scalar: &mesos_v1.Value_Scalar{
					Value: &scalar,
				},
			},
		},
	}
	offerGroup, err := OfferToOfferGroup(offer)
	assert.NoError(t, err)
	assert.Equal(t, "hostname", offerGroup.Group.Name)
	assert.Equal(t, 42.0, offerGroup.Group.Metrics.Get(metrics.CPUTotal))
	assert.Equal(t, 1, offerGroup.Group.Labels.Count(labels.NewLabel("attribute", "text")))
}
