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

	mesosv1 "github.com/uber/peloton/.gen/mesos/v1"
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
		if !found {
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
		if !found {
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

// hasMesosCommandChanged checks whether two mesos CommandInfo URIs have
// difference, with special handling for some of the fields.
func hasMesosCommandUriChanged(
	prevUri, newUri *mesosv1.CommandInfo_URI,
) bool {
	if prevUri.GetValue() != newUri.GetValue() {
		return true
	}

	if prevUri.GetExecutable() != newUri.GetExecutable() {
		return true
	}

	if prevUri.GetExtract() != newUri.GetExtract() {
		return true
	}

	if prevUri.GetCache() != newUri.GetCache() {
		return true
	}

	if prevUri.GetOutputFile() != newUri.GetOutputFile() {
		return true
	}

	return false
}

// hasMesosCommandChanged checks whether two mesos CommandInfo have
// difference, with special handling for some of the fields.
func hasMesosCommandChanged(
	prevCommand, newCommand *mesosv1.CommandInfo,
) bool {
	if prevCommand == nil && newCommand == nil {
		return false
	}

	if prevCommand == nil || newCommand == nil {
		return true
	}

	if len(prevCommand.GetUris()) != len(newCommand.GetUris()) {
		return true
	}

	prevC := proto.Clone(prevCommand).(*mesosv1.CommandInfo)
	newC := proto.Clone(newCommand).(*mesosv1.CommandInfo)

	for i := range prevCommand.GetUris() {
		if hasMesosCommandUriChanged(
			prevC.GetUris()[i],
			newC.GetUris()[i],
		) {
			return true
		}
	}

	prevC.Uris = nil
	newC.Uris = nil

	return !proto.Equal(prevC, newC)
}

// hasMesosContainerChanged checks whether two mesos ContainerInfo have
// difference, with special handling for some of the fields.
func hasMesosContainerChanged(
	prevContainer, newContainer *mesosv1.ContainerInfo,
) bool {
	if prevContainer == nil && newContainer == nil {
		return false
	}

	if prevContainer == nil || newContainer == nil {
		return true
	}

	prevC := proto.Clone(prevContainer).(*mesosv1.ContainerInfo)
	newC := proto.Clone(newContainer).(*mesosv1.ContainerInfo)

	if prevC.Docker != nil && newC.Docker != nil {
		// network field has default value if not set, use getter to retrieve
		// value and compare.
		if prevC.GetDocker().GetNetwork() !=
			newC.GetDocker().GetNetwork() {
			return true
		}

		prevC.Docker.Network = nil
		newC.Docker.Network = nil
	}

	return !proto.Equal(prevC, newC)
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
		HasPortConfigsChanged(prevTaskConfig.GetPorts(), newTaskConfig.GetPorts()) ||
		hasMesosContainerChanged(prevTaskConfig.GetContainer(), newTaskConfig.GetContainer()) ||
		hasMesosCommandChanged(prevTaskConfig.GetCommand(), newTaskConfig.GetCommand()) {
		return true
	}

	prevTask := proto.Clone(prevTaskConfig).(*task.TaskConfig)
	newTask := proto.Clone(newTaskConfig).(*task.TaskConfig)

	oldName := prevTask.GetName()
	newName := newTask.GetName()
	oldLabels := prevTask.GetLabels()
	newLabels := newTask.GetLabels()
	oldPorts := prevTask.GetPorts()
	newPorts := newTask.GetPorts()
	oldContainer := prevTask.GetContainer()
	newContainer := newTask.GetContainer()
	oldCommand := prevTask.GetCommand()
	newCommand := newTask.GetCommand()

	defer func() {
		prevTask.Name = oldName
		newTask.Name = newName
		prevTask.Labels = oldLabels
		newTask.Labels = newLabels
		prevTask.Ports = oldPorts
		newTask.Ports = newPorts
		prevTask.Container = oldContainer
		newTask.Container = newContainer
		prevTask.Command = oldCommand
		newTask.Command = newCommand
	}()

	prevTask.Name = ""
	newTask.Name = ""
	prevTask.Labels = nil
	newTask.Labels = nil
	prevTask.Ports = nil
	newTask.Ports = nil
	prevTask.Container = nil
	newTask.Container = nil
	prevTask.Command = nil
	newTask.Command = nil

	return !proto.Equal(prevTask, newTask)
}

// HasContainerSpecChanged returns true if the container spec has changed.
func HasContainerSpecChanged(
	prevContainerSpec *pod.ContainerSpec,
	newContainerSpec *pod.ContainerSpec) bool {
	if prevContainerSpec == nil && newContainerSpec == nil {
		return false
	}

	if prevContainerSpec == nil ||
		newContainerSpec == nil ||
		HasPortSpecsChanged(prevContainerSpec.GetPorts(), newContainerSpec.GetPorts()) {
		return true
	}

	// TODO(kevinxu): we can avoid clone here if we don't expose it as
	//  public method, and make its internal caller clones ContainerSpec
	//  before calling this function.
	prevContainer := proto.Clone(prevContainerSpec).(*pod.ContainerSpec)
	newContainer := proto.Clone(newContainerSpec).(*pod.ContainerSpec)

	oldPorts := prevContainer.GetPorts()
	newPorts := newContainer.GetPorts()
	oldContainerInfo := prevContainer.GetContainer()
	newContainerInfo := newContainer.GetContainer()
	oldCommand := prevContainer.GetCommand()
	newCommand := newContainer.GetCommand()
	oldExecutor := prevContainer.GetExecutor()
	newExecutor := newContainer.GetExecutor()

	defer func() {
		prevContainer.Ports = oldPorts
		newContainer.Ports = newPorts
		prevContainer.Container = oldContainerInfo
		newContainer.Container = newContainerInfo
		prevContainer.Command = oldCommand
		newContainer.Command = newCommand
		prevContainer.Executor = oldExecutor
		newContainer.Executor = newExecutor
	}()

	prevContainer.Ports = nil
	newContainer.Ports = nil
	prevContainer.Container = nil
	newContainer.Container = nil
	prevContainer.Command = nil
	newContainer.Command = nil
	prevContainer.Executor = nil
	newContainer.Executor = nil

	return !proto.Equal(prevContainer, newContainer)
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

	prevPod := proto.Clone(prevPodSpec).(*pod.PodSpec)
	newPod := proto.Clone(newPodSpec).(*pod.PodSpec)

	oldPodName := prevPod.GetPodName()
	newPodName := newPod.GetPodName()
	oldLabels := prevPod.GetLabels()
	newLabels := newPod.GetLabels()
	oldInitContainers := prevPod.GetInitContainers()
	newInitContainers := newPod.GetInitContainers()
	oldContainers := prevPod.GetContainers()
	newContainers := newPod.GetContainers()

	defer func() {
		prevPod.PodName = oldPodName
		newPod.PodName = newPodName
		prevPod.Labels = oldLabels
		newPod.Labels = newLabels
		prevPod.InitContainers = oldInitContainers
		newPod.InitContainers = newInitContainers
		prevPod.Containers = oldContainers
		newPod.Containers = newContainers
	}()

	prevPod.PodName = nil
	newPod.PodName = nil
	prevPod.Labels = nil
	newPod.Labels = nil
	prevPod.InitContainers = nil
	newPod.InitContainers = nil
	prevPod.Containers = nil
	newPod.Containers = nil

	return !proto.Equal(prevPod, newPod)
}
