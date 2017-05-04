package util

import (
	"sort"

	mesos "mesos/v1"
)

// ExtractPortSet is helper function to extract available port set
// from a Mesos resource.
func ExtractPortSet(resource *mesos.Resource) map[uint32]bool {
	res := make(map[uint32]bool)

	if resource.GetName() != "ports" {
		return res
	}

	for _, r := range resource.GetRanges().GetRange() {
		// Remember that end is inclusive
		for i := r.GetBegin(); i <= r.GetEnd(); i++ {
			res[uint32(i)] = true
		}
	}

	return res
}

// GetPortsSetFromResources is helper function to extract ports resources.
func GetPortsSetFromResources(resources []*mesos.Resource) map[uint32]bool {
	res := make(map[uint32]bool)
	for _, rs := range resources {
		portSet := ExtractPortSet(rs)
		for port := range portSet {
			res[port] = true
		}
	}
	return res
}

// GetPortsNumFromOfferMap is helper function to get number of available ports
// from given id to offer map.
func GetPortsNumFromOfferMap(offerMap map[string]*mesos.Offer) uint32 {
	numPorts := 0
	for _, offer := range offerMap {
		numPorts += len(GetPortsSetFromResources(offer.GetResources()))
	}
	return uint32(numPorts)
}

// MergePortSets merges two portset map into one.
func MergePortSets(m1, m2 map[uint32]bool) map[uint32]bool {
	m := make(map[uint32]bool)
	for i1, ok := range m1 {
		if ok {
			m[i1] = true
		}
	}
	for i2, ok := range m2 {
		if ok {
			m[i2] = true
		}
	}
	return m
}

// CreatePortRanges create Mesos Ranges type from given port set.
func CreatePortRanges(portSet map[uint32]bool) *mesos.Value_Ranges {
	var sorted []int
	for p, ok := range portSet {
		if ok {
			sorted = append(sorted, int(p))
		}
	}
	sort.Ints(sorted)

	res := mesos.Value_Ranges{
		Range: []*mesos.Value_Range{},
	}
	for _, p := range sorted {
		tmp := uint64(p)
		res.Range = append(
			res.Range,
			&mesos.Value_Range{Begin: &tmp, End: &tmp},
		)
	}
	return &res
}
