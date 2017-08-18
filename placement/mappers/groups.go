package mappers

import (
	"strings"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/common/constraints"
	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"code.uber.internal/infra/peloton/placement/models"
)

// OfferToOfferGroup will convert an offer to an offer group.
func OfferToOfferGroup(offer *hostsvc.HostOffer) (*models.OfferGroup, error) {
	return _groupMapper.Convert(offer)
}

type groupMapper struct {
	deriver metrics.Deriver
}

func (mapper *groupMapper) makeMetrics(resources scalar.Resources) *metrics.MetricSet {
	result := metrics.NewMetricSet()
	result.Add(metrics.CPUTotal, resources.CPU)
	result.Add(metrics.MemoryTotal, resources.Mem)
	result.Add(metrics.DiskTotal, resources.Disk)
	result.Add(metrics.GPUTotal, resources.GPU)
	// ToDo: parse metrics.FileDescriptorsTotal
	mapper.deriver.Derive(result)
	return result
}

func (mapper *groupMapper) makeLabels(bag constraints.LabelValues) *labels.LabelBag {
	result := labels.NewLabelBag()
	for key, valueCounts := range bag {
		for value := range valueCounts {
			result.Add(labels.NewLabel(strings.Split(key+"."+value, ".")...))
		}
	}
	return result
}

func (mapper *groupMapper) Convert(offer *hostsvc.HostOffer) (*models.OfferGroup, error) {
	group := placement.NewGroup(offer.Hostname)
	group.Metrics = mapper.makeMetrics(scalar.FromMesosResources(offer.GetResources()))
	group.Labels = mapper.makeLabels(constraints.GetHostLabelValues(offer.Hostname, offer.Attributes))
	offerGroup := &models.OfferGroup{
		Group: group,
		Offer: &models.Offer{
			Offer: offer,
		},
	}
	return offerGroup, nil
}

var _groupMapper = &groupMapper{
	deriver: metrics.NewDeriver([]metrics.FreeMetricTuple{
		{metrics.CPUFree, metrics.CPUUsed, metrics.CPUTotal},
		{metrics.MemoryFree, metrics.MemoryUsed, metrics.MemoryTotal},
		{metrics.DiskFree, metrics.DiskUsed, metrics.DiskTotal},
		{metrics.GPUFree, metrics.GPUUsed, metrics.GPUTotal},
	}),
}
