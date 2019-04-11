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

package taskconfig

import (
	"github.com/gogo/protobuf/proto"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	v1peloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
)

type label interface {
	GetKey() string
	GetValue() string
}

func hasLabelsChanged(
	prevLabels []label,
	newLabels []label) bool {
	for _, label := range newLabels {
		found := false
		for _, prevLabel := range prevLabels {
			if label.GetKey() == prevLabel.GetKey() &&
				label.GetValue() == prevLabel.GetValue() {
				found = true
				break
			}
		}

		// label not found
		if found == false {
			return true
		}
	}

	// all old labels found in new config as well
	return false
}

// HasPelotonLabelsChanged returns true if v0 peloton labels have changed
func HasPelotonLabelsChanged(
	prevLabels []*peloton.Label,
	newLabels []*peloton.Label) bool {
	if len(prevLabels) != len(newLabels) {
		return true
	}

	plabel := make([]label, 0, len(prevLabels))
	for _, l := range prevLabels {
		plabel = append(plabel, l)
	}

	nlabel := make([]label, 0, len(newLabels))
	for _, l := range newLabels {
		nlabel = append(nlabel, l)
	}

	return hasLabelsChanged(plabel, nlabel)
}

// HasPelotonV1LabelsChanged returns true if v1 peloton labels have changed
func HasPelotonV1LabelsChanged(
	prevLabels []*v1peloton.Label,
	newLabels []*v1peloton.Label) bool {
	if len(prevLabels) != len(newLabels) {
		return true
	}

	plabel := make([]label, 0, len(prevLabels))
	for _, l := range prevLabels {
		plabel = append(plabel, l)
	}

	nlabel := make([]label, 0, len(newLabels))
	for _, l := range newLabels {
		nlabel = append(nlabel, l)
	}

	return hasLabelsChanged(plabel, nlabel)
}

type port interface {
	GetName() string
	GetValue() uint32
	GetEnvName() string
}

func hasPortsChanged(
	prevPorts []port,
	newPorts []port) bool {
	for _, newPort := range newPorts {
		found := false
		for _, prevPort := range prevPorts {
			if newPort.GetName() == prevPort.GetName() &&
				newPort.GetValue() == prevPort.GetValue() &&
				newPort.GetEnvName() == prevPort.GetEnvName() {
				found = true
				break
			}
		}

		// ports not found
		if found == false {
			return true
		}
	}

	// all old ports found in new config as well
	return false
}

// HasPortConfigsChanged returns true if the port configs have changed
func HasPortConfigsChanged(
	prevPorts []*task.PortConfig,
	newPorts []*task.PortConfig) bool {
	if len(prevPorts) != len(newPorts) {
		return true
	}

	pport := make([]port, 0, len(prevPorts))
	for _, p := range prevPorts {
		pport = append(pport, p)
	}

	nport := make([]port, 0, len(newPorts))
	for _, p := range newPorts {
		nport = append(nport, p)
	}

	return hasPortsChanged(pport, nport)
}

// HasPortSpecsChanged returns true if the port specs have changed
func HasPortSpecsChanged(
	prevPorts []*pod.PortSpec,
	newPorts []*pod.PortSpec) bool {
	if len(prevPorts) != len(newPorts) {
		return true
	}

	pport := make([]port, 0, len(prevPorts))
	for _, p := range prevPorts {
		pport = append(pport, p)
	}

	nport := make([]port, 0, len(newPorts))
	for _, p := range newPorts {
		nport = append(nport, p)
	}

	return hasPortsChanged(pport, nport)
}

// HasTaskConfigChanged returns true if the task config (other than the name)
// has changed.
func HasTaskConfigChanged(
	prevTaskConfig *task.TaskConfig,
	newTaskConfig *task.TaskConfig,
) bool {
	if prevTaskConfig == nil ||
		newTaskConfig == nil ||
		HasPelotonLabelsChanged(prevTaskConfig.GetLabels(), newTaskConfig.GetLabels()) ||
		HasPortConfigsChanged(prevTaskConfig.GetPorts(), newTaskConfig.GetPorts()) {
		return true
	}

	oldName := prevTaskConfig.GetName()
	newName := newTaskConfig.GetName()
	prevTaskConfig.Name = ""
	newTaskConfig.Name = ""

	oldLabels := prevTaskConfig.GetLabels()
	newLabels := newTaskConfig.GetLabels()
	prevTaskConfig.Labels = nil
	newTaskConfig.Labels = nil

	oldPorts := prevTaskConfig.GetPorts()
	newPorts := newTaskConfig.GetPorts()
	prevTaskConfig.Ports = nil
	newTaskConfig.Ports = nil

	changed := !proto.Equal(prevTaskConfig, newTaskConfig)

	prevTaskConfig.Name = oldName
	newTaskConfig.Name = newName
	prevTaskConfig.Labels = oldLabels
	newTaskConfig.Labels = newLabels
	prevTaskConfig.Ports = oldPorts
	newTaskConfig.Ports = newPorts

	return changed
}

// HasContainerSpecChanged returns ture if the container spec has changed.
func HasContainerSpecChanged(
	prevContainer *pod.ContainerSpec,
	newContainer *pod.ContainerSpec) bool {
	if prevContainer == nil && newContainer == nil {
		return false
	}

	if prevContainer == nil ||
		newContainer == nil ||
		HasPortSpecsChanged(prevContainer.GetPorts(), newContainer.GetPorts()) {
		return true
	}

	oldPorts := prevContainer.GetPorts()
	newPorts := newContainer.GetPorts()
	prevContainer.Ports = nil
	newContainer.Ports = nil

	changed := !proto.Equal(prevContainer, newContainer)

	prevContainer.Ports = oldPorts
	newContainer.Ports = newPorts

	return changed
}

// HasPodSpecChanged returns true if the pod spec (other than the name)
// has changed.
func HasPodSpecChanged(
	prevPodSpec *pod.PodSpec,
	newPodSpec *pod.PodSpec) bool {
	if prevPodSpec == nil ||
		newPodSpec == nil ||
		HasPelotonV1LabelsChanged(prevPodSpec.GetLabels(), newPodSpec.GetLabels()) {
		return true
	}

	if len(prevPodSpec.GetInitContainers()) != len(newPodSpec.GetInitContainers()) {
		return true
	}

	for i := 0; i < len(prevPodSpec.GetInitContainers()); i++ {
		if HasContainerSpecChanged(
			prevPodSpec.GetInitContainers()[i],
			newPodSpec.GetInitContainers()[i]) {
			return true
		}
	}

	if len(prevPodSpec.GetContainers()) != len(newPodSpec.GetContainers()) {
		return true
	}

	for i := 0; i < len(prevPodSpec.GetContainers()); i++ {
		if HasContainerSpecChanged(
			prevPodSpec.GetContainers()[i],
			newPodSpec.GetContainers()[i]) {
			return true
		}
	}

	oldPodName := prevPodSpec.GetPodName()
	newPodName := newPodSpec.GetPodName()
	prevPodSpec.PodName = nil
	newPodSpec.PodName = nil

	oldLabels := prevPodSpec.GetLabels()
	newLabels := newPodSpec.GetLabels()
	prevPodSpec.Labels = nil
	newPodSpec.Labels = nil

	oldInitContainers := prevPodSpec.GetInitContainers()
	newInitContainers := newPodSpec.GetInitContainers()
	prevPodSpec.InitContainers = nil
	newPodSpec.InitContainers = nil

	oldContainers := prevPodSpec.GetContainers()
	newContainers := newPodSpec.GetContainers()
	prevPodSpec.Containers = nil
	newPodSpec.Containers = nil

	changed := !proto.Equal(prevPodSpec, newPodSpec)

	prevPodSpec.PodName = oldPodName
	newPodSpec.PodName = newPodName
	prevPodSpec.Labels = oldLabels
	newPodSpec.Labels = newLabels
	prevPodSpec.InitContainers = oldInitContainers
	newPodSpec.InitContainers = newInitContainers
	prevPodSpec.Containers = oldContainers
	newPodSpec.Containers = newContainers

	return changed
}
