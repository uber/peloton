package reservation

import (
	mesos "mesos/v1"

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

// GetLabeledReservedResources extracts reserved resources from given list of
// offers, returns map from reservationLabelID -> ReservedResources.
func GetLabeledReservedResources(
	offers []*mesos.Offer) map[string]*ReservedResources {

	reservedResources := make(map[string]*ReservedResources)
	for _, offer := range offers {
		for _, res := range offer.Resources {
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
	}
	return reservedResources
}

// HasLabeledReservedResources returns if given offer has labeled
// reserved resources.
func HasLabeledReservedResources(offer *mesos.Offer) bool {
	for _, res := range offer.Resources {
		if res.GetRole() != "" &&
			res.GetRole() != unreservedRole &&
			res.GetReservation().GetLabels() != nil {
			return true
		}
	}
	return false
}
