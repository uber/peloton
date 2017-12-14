package testutil

import (
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/placement/models"
)

// SetupHost creates an offer.
func SetupHost() *models.Host {
	attribute := "attribute"
	text := "text"
	cpuName := "cpus"
	memoryName := "mem"
	diskName := "disk"
	gpuName := "gpus"
	ports := "ports"
	cpuValue := 48.0
	gpuValue := 128.0
	memoryValue := 128.0 * 1024.0
	diskValue := 6.0 * 1024.0 * 1024.0
	scalar := 1.0
	begin := uint64(31000)
	end := uint64(31009)
	textType := mesos_v1.Value_TEXT
	scalarType := mesos_v1.Value_SCALAR
	rangesType := mesos_v1.Value_RANGES
	hostOffer := &hostsvc.HostOffer{
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
							Begin: &begin,
							End:   &end,
						},
					},
				},
			},
		},
		Resources: []*mesos_v1.Resource{
			{
				Name: &cpuName,
				Scalar: &mesos_v1.Value_Scalar{
					Value: &cpuValue,
				},
			},
			{
				Name: &memoryName,
				Scalar: &mesos_v1.Value_Scalar{
					Value: &memoryValue,
				},
			},
			{
				Name: &diskName,
				Scalar: &mesos_v1.Value_Scalar{
					Value: &diskValue,
				},
			},
			{
				Name: &gpuName,
				Scalar: &mesos_v1.Value_Scalar{
					Value: &gpuValue,
				},
			},
			{
				Name: &ports,
				Ranges: &mesos_v1.Value_Ranges{
					Range: []*mesos_v1.Value_Range{
						{
							Begin: &begin,
							End:   &end,
						},
					},
				},
			},
		},
	}
	return models.NewHost(hostOffer, []*resmgr.Task{}, time.Now())
}
