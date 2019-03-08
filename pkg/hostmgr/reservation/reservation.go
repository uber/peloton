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

package reservation

import (
	mesos "github.com/uber/peloton/.gen/mesos/v1"

	"github.com/uber/peloton/pkg/hostmgr/scalar"
)

const (
	unreservedRole = "*"
)

// ReservedResources has both reserved resources and volumes.
type ReservedResources struct {
	Resources scalar.Resources
	// volumes has list of volume IDs.
	Volumes []string
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
				Resources: scalar.Resources{},
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
		reservedResources[resLabels].Resources = reservedResources[resLabels].Resources.Add(
			resResource)
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
