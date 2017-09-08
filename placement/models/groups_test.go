package models

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
)

func TestGroupMapper_Convert(t *testing.T) {
	attribute := "attribute"
	text := "text"
	cpus := "cpus"
	mem := "mem"
	disk := "disk"
	gpus := "gpus"
	ports := "ports"
	scalar := 1.0
	begin1 := uint64(31000)
	end1 := uint64(31002)
	begin2 := uint64(31003)
	end2 := uint64(31004)
	textType := mesos_v1.Value_TEXT
	scalarType := mesos_v1.Value_SCALAR
	rangesType := mesos_v1.Value_RANGES
	offer := &hostsvc.HostOffer{
		Hostname: "hostname",
		Attributes: []*mesos_v1.Attribute{
			{
				Name: &attribute,
				Type: &textType,
				Text: &mesos_v1.Value_Text{
					Value: &text,
				},
			},
			{
				Name: &attribute,
				Type: &scalarType,
				Scalar: &mesos_v1.Value_Scalar{
					Value: &scalar,
				},
			},
			{
				Name: &attribute,
				Type: &rangesType,
				Ranges: &mesos_v1.Value_Ranges{
					Range: []*mesos_v1.Value_Range{
						{
							Begin: &begin1,
							End:   &end1,
						},
						{
							Begin: &begin2,
							End:   &end2,
						},
					},
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
			{
				Name: &mem,
				Scalar: &mesos_v1.Value_Scalar{
					Value: &scalar,
				},
			},
			{
				Name: &disk,
				Scalar: &mesos_v1.Value_Scalar{
					Value: &scalar,
				},
			},
			{
				Name: &gpus,
				Scalar: &mesos_v1.Value_Scalar{
					Value: &scalar,
				},
			},
			{
				Name: &ports,
				Ranges: &mesos_v1.Value_Ranges{
					Range: []*mesos_v1.Value_Range{
						{
							Begin: &begin1,
							End:   &end1,
						},
					},
				},
			},
		},
	}
	group := OfferToGroup(offer)
	assert.Equal(t, "hostname", group.Name)
	assert.Equal(t, 100.0, group.Metrics.Get(metrics.CPUTotal))
	assert.Equal(t, 1.0*metrics.MiB, group.Metrics.Get(metrics.MemoryTotal))
	assert.Equal(t, 1.0*metrics.MiB, group.Metrics.Get(metrics.DiskTotal))
	assert.Equal(t, 100.0, group.Metrics.Get(metrics.GPUTotal))
	assert.Equal(t, 3.0, group.Metrics.Get(metrics.PortsTotal))
	assert.Equal(t, 1, group.Labels.Count(labels.NewLabel("attribute", "text")))
	assert.Equal(t, 1, group.Labels.Count(labels.NewLabel("attribute", "1")))
	assert.Equal(t, 1, group.Labels.Count(labels.NewLabel("attribute", "[31000-31002];[31003-31004]")))
}
