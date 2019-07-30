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

package procedure

import (
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	_procedureSeparator = "::"
	_matchAllRule       = "*"
	_ruleSeparator      = ":"
)

// LabelManager provides mechanism to query method/label
// information
type LabelManager struct {
	// serviceToProcedureLabels uses service name as key and
	// methodLabels as labels
	serviceToProcedureLabels map[string]*methodLabels
}

type LabelManagerConfig struct {
	Entries []*LabelManagerConfigEntry
}

type LabelManagerConfigEntry struct {
	Procedures []string
	Labels     []string
}

// NewLabelManager creates a new instance of LabelManager
func NewLabelManager(config *LabelManagerConfig) *LabelManager {
	serviceToProcedureLabels := make(map[string]*methodLabels)

	for _, entry := range config.Entries {
		for _, procedure := range entry.Procedures {
			results := strings.Split(procedure, _ruleSeparator)
			service := results[0]
			method := results[1]
			serviceToProcedureLabels[service] = constructProcedureLabels(
				service, method, entry.Labels, serviceToProcedureLabels)
		}
	}

	return &LabelManager{
		serviceToProcedureLabels: serviceToProcedureLabels,
	}
}

// HasLabel returns if a method has a particular label
func (m *LabelManager) HasLabel(procedure string, label string) bool {
	results := strings.Split(procedure, _procedureSeparator)

	if len(results) != 2 {
		log.WithField("procedure", procedure).Error("Fail to parse procedure")
		return false
	}

	service := results[0]
	method := results[1]

	// if LabelManager has config which is not service specific (for example *::Get*, *::Create*),
	// check if that config has the label we want
	procedureLabels, ok := m.serviceToProcedureLabels[_matchAllRule]
	if ok && procedureLabels.hasLabel(method, label) {
		return true
	}

	// check if the service is defined for LabelManager, if not
	// the method must not have the label
	procedureLabels, ok = m.serviceToProcedureLabels[service]
	if !ok {
		return false
	}

	return procedureLabels.hasLabel(method, label)
}

// if there is already an entry in serviceToProcedureLabels,
// add labels in the entry and return.
// if there is no entry in serviceToProcedureLabels,
// create a new entry with labels and return.
func constructProcedureLabels(
	service string,
	method string,
	labels []string,
	serviceToProcedureLabels map[string]*methodLabels,
) *methodLabels {
	if _, ok := serviceToProcedureLabels[service]; !ok {
		serviceToProcedureLabels[service] = &methodLabels{}
	}

	methodLabels := serviceToProcedureLabels[service]
	for _, methodToLabel := range methodLabels.entries {
		// the method already has an entry, add labels in the entry
		if methodToLabel.method == method {
			for _, label := range labels {
				methodToLabel.labels[label] = struct{}{}
			}
			return methodLabels
		}
	}

	// the method is not seen before
	labelsMap := make(map[string]struct{})
	for _, label := range labels {
		labelsMap[label] = struct{}{}
	}
	methodLabels.entries = append(methodLabels.entries, &struct {
		method string
		labels map[string]struct{}
	}{method: method, labels: labelsMap})
	return methodLabels
}

// methodLabels contains the labels related to a particular method
type methodLabels struct {
	entries []*struct {
		// method rule of the entry
		method string
		// labels associated with the method
		labels map[string]struct{}
	}
}

// hasLabel returns if a particular method has a particular label
func (p *methodLabels) hasLabel(method string, label string) bool {
	for _, procedureLabel := range p.entries {
		// check if any matching method has the label
		if matchRule(method, procedureLabel.method) {
			if _, ok := procedureLabel.labels[label]; ok {
				return true
			}
		}
	}

	// no matching method has the label
	return false
}

func matchRule(method string, rule string) bool {
	if len(rule) == 0 {
		return false
	}

	if rule == _matchAllRule {
		return true
	}

	if rule[len(rule)-1:] == _matchAllRule {
		return strings.HasPrefix(method, rule[0:len(rule)-1])
	}

	return rule == method
}
