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
	"strings"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/hostmgr/scalar"

	log "github.com/sirupsen/logrus"
)

// LabelKeyToEnvVarName converts a task label key to an env var name
// Example: label key 'peloton.job_id' converted to env var name 'PELOTON_JOB_ID'
func LabelKeyToEnvVarName(labelKey string) string {
	return strings.ToUpper(strings.Replace(labelKey, ".", "_", 1))
}

// MesosOffersToHostOffers takes the Mesos Offer and returns the Host Offer
func MesosOffersToHostOffers(hostoffers map[string][]*mesos.Offer) []*hostsvc.HostOffer {
	hostOffers := make([]*hostsvc.HostOffer, 0, len(hostoffers))
	for hostname, offers := range hostoffers {
		if len(offers) <= 0 {
			log.WithField("host", hostname).
				Warn("Empty offer slice from host")
			continue
		}

		var resources []*mesos.Resource
		var attributes []*mesos.Attribute
		for _, offer := range offers {
			resources = append(resources, offer.GetResources()...)
			attributes = append(attributes, offer.GetAttributes()...)
		}

		hostOffer := hostsvc.HostOffer{
			Hostname:   hostname,
			AgentId:    offers[0].GetAgentId(),
			Attributes: attributes,
			Resources:  resources,
		}

		hostOffers = append(hostOffers, &hostOffer)
	}
	return hostOffers
}

// IsSlackResourceType validates is given resource type is supported slack resource.
func IsSlackResourceType(resourceType string, slackResourceTypes []string) bool {
	for _, rType := range slackResourceTypes {
		if strings.ToLower(rType) == strings.ToLower(resourceType) {
			return true
		}
	}
	return false
}

// GetResourcesFromOffers returns the combined number of scalar.resources
// passed as a map of offerid->mesos.offer map.
func GetResourcesFromOffers(offers map[string]*mesos.Offer) scalar.Resources {
	var resources []*mesos.Resource
	for _, offer := range offers {
		resources = append(resources, offer.GetResources()...)
	}
	return scalar.FromMesosResources(resources)
}

// HasExclusiveAttribute returns true if the provided attributes contains
// the "peloton/exclusive" attribute.
func HasExclusiveAttribute(attributes []*mesos.Attribute) bool {
	for _, attr := range attributes {
		if common.PelotonExclusiveAttributeName == attr.GetName() {
			return true
		}
	}
	return false
}
