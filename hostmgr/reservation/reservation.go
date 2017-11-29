package reservation

import (
	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"

	"code.uber.internal/infra/peloton/hostmgr/scalar"
)

const (
	unreservedRole = "*"
)

// ReservedResources has both reserved resources and volumes.
type ReservedResources struct {
	Resources *scalar.Resources
	// volumes has list of volume IDs.
	Volumes []string
}

// GetReservationLabelsWithoutVolume returns reservation labels for resources that
// need to reconciled, i.e. unreserve the resource contains no persistent volume resources.
// returns map of labels -> true if need to be reconciled.
func GetReservationLabelsWithoutVolume(resources []*mesos.Resource) map[string]bool {
	reservedResources := GetLabeledReservedResourcesFromResources(resources)
	result := make(map[string]bool)
	for labels, res := range reservedResources {
		if len(res.Volumes) == 0 {
			result[labels] = true
		}
	}
	return result
}

// GetLabeledReservedResources extracts reserved resources from given list of
// offers, returns map from reservationLabelID -> ReservedResources.
func GetLabeledReservedResources(
	offers []*mesos.Offer) map[string]*ReservedResources {

	resources := []*mesos.Resource{}
	for _, offer := range offers {
		resources = append(resources, offer.GetResources()...)
	}
	return GetLabeledReservedResourcesFromResources(resources)
}

// GetLabeledReservedResourcesFromResources extracts reserved resources from given list of
// mesos resources, returns map from reservationLabelID -> ReservedResources.
func GetLabeledReservedResourcesFromResources(
	resources []*mesos.Resource) map[string]*ReservedResources {

	reservedResources := make(map[string]*ReservedResources)
	for _, res := range resources {
		if res.GetRole() == "" ||
			res.GetRole() == unreservedRole ||
			res.GetReservation().GetLabels() == nil {
			continue
		}

		// TODO: only extract uuid field as reservation key.
		resLabels := res.GetReservation().GetLabels().String()
		if _, ok := reservedResources[resLabels]; !ok {
			reservedResources[resLabels] = &ReservedResources{
				Resources: &scalar.Resources{},
			}
		}

		if res.GetDisk() != nil {
			volumeID := res.GetDisk().GetPersistence().GetId()
			reservedResources[resLabels].Volumes = append(
				reservedResources[resLabels].Volumes,
				volumeID)
			continue
		}

		resResource := scalar.FromMesosResource(res)
		reservedResources[resLabels].Resources = (reservedResources[resLabels].Resources.Add(
			&resResource))
	}
	return reservedResources
}

// HasLabeledReservedResources returns if given offer has labeled
// reserved resources.
func HasLabeledReservedResources(offer *mesos.Offer) bool {
	for _, res := range offer.GetResources() {
		if res.GetRole() != "" &&
			res.GetRole() != unreservedRole &&
			res.GetReservation().GetLabels() != nil {
			return true
		}
	}
	return false
}
