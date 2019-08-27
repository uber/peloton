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
	"go.uber.org/thriftrw/ptr"
	"testing"

	"github.com/stretchr/testify/assert"

	mesosv1 "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	v1peloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
)

// TestHasPelotonLabelsChanged checks peloton Label comparision util function
func TestHasPelotonLabelsChanged(t *testing.T) {
	l1 := []*peloton.Label{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
	}
	l2 := []*peloton.Label{
		{Key: "key2", Value: "value2"},
		{Key: "key1", Value: "value1"},
	}
	l3 := []*peloton.Label{
		{Key: "key3", Value: "value3"},
		{Key: "key1", Value: "value1"},
	}
	l4 := []*peloton.Label{
		{Key: "key2", Value: "value3"},
		{Key: "key1", Value: "value1"},
	}
	l5 := []*peloton.Label{
		{Key: "key2", Value: "value2"},
		{Key: "key1", Value: "value1"},
		{Key: "key11", Value: "value11"},
	}

	assert.False(t, HasPelotonLabelsChanged(l1, l2))
	assert.True(t, HasPelotonLabelsChanged(l1, l3))
	assert.True(t, HasPelotonLabelsChanged(l1, l4))
	assert.True(t, HasPelotonLabelsChanged(l1, l5))
}

// TestHasPelotonV1LabelsChanged checks v1 peloton Label comparision util function
func TestHasPelotonV1LabelsChanged(t *testing.T) {
	l1 := []*v1peloton.Label{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
	}
	l2 := []*v1peloton.Label{
		{Key: "key2", Value: "value2"},
		{Key: "key1", Value: "value1"},
	}
	l3 := []*v1peloton.Label{
		{Key: "key3", Value: "value3"},
		{Key: "key1", Value: "value1"},
	}
	l4 := []*v1peloton.Label{
		{Key: "key2", Value: "value3"},
		{Key: "key1", Value: "value1"},
	}
	l5 := []*v1peloton.Label{
		{Key: "key2", Value: "value2"},
		{Key: "key1", Value: "value1"},
		{Key: "key11", Value: "value11"},
	}

	assert.False(t, HasPelotonV1LabelsChanged(l1, l2))
	assert.True(t, HasPelotonV1LabelsChanged(l1, l3))
	assert.True(t, HasPelotonV1LabelsChanged(l1, l4))
	assert.True(t, HasPelotonV1LabelsChanged(l1, l5))
}

// TestHasPortConfigsChanged checks PortConfig comparision util function
func TestHasPortConfigsChanged(t *testing.T) {
	p1 := []*task.PortConfig{
		{Name: "name1", Value: 1111, EnvName: "env1"},
		{Name: "name2", Value: 2222, EnvName: "env2"},
	}
	p2 := []*task.PortConfig{
		{Name: "name2", Value: 2222, EnvName: "env2"},
		{Name: "name1", Value: 1111, EnvName: "env1"},
	}
	p3 := []*task.PortConfig{
		{Name: "name2", Value: 2222, EnvName: "env2"},
		{Name: "name1", Value: 1111, EnvName: "env1"},
		{Name: "name1", Value: 3333, EnvName: "env1"},
	}
	p4 := []*task.PortConfig{
		{Name: "name3", Value: 2222, EnvName: "env3"},
		{Name: "name1", Value: 1111, EnvName: "env1"},
	}
	p5 := []*task.PortConfig{
		{Name: "name2", Value: 2222, EnvName: "env3"},
		{Name: "name1", Value: 1111, EnvName: "env1"},
	}

	assert.False(t, HasPortConfigsChanged(p1, p2))
	assert.True(t, HasPortConfigsChanged(p1, p3))
	assert.True(t, HasPortConfigsChanged(p1, p4))
	assert.True(t, HasPortConfigsChanged(p1, p5))
}

// TestHasPortSpecsChanged checks PortSpec comparision util function
func TestHasPortSpecsChanged(t *testing.T) {
	p1 := []*pod.PortSpec{
		{Name: "name1", Value: 1111, EnvName: "env1"},
		{Name: "name2", Value: 2222, EnvName: "env2"},
	}
	p2 := []*pod.PortSpec{
		{Name: "name2", Value: 2222, EnvName: "env2"},
		{Name: "name1", Value: 1111, EnvName: "env1"},
	}
	p3 := []*pod.PortSpec{
		{Name: "name2", Value: 2222, EnvName: "env2"},
		{Name: "name1", Value: 1111, EnvName: "env1"},
		{Name: "name1", Value: 3333, EnvName: "env1"},
	}
	p4 := []*pod.PortSpec{
		{Name: "name3", Value: 2222, EnvName: "env3"},
		{Name: "name1", Value: 1111, EnvName: "env1"},
	}
	p5 := []*pod.PortSpec{
		{Name: "name2", Value: 2222, EnvName: "env3"},
		{Name: "name1", Value: 1111, EnvName: "env1"},
	}

	assert.False(t, HasPortSpecsChanged(p1, p2))
	assert.True(t, HasPortSpecsChanged(p1, p3))
	assert.True(t, HasPortSpecsChanged(p1, p4))
	assert.True(t, HasPortSpecsChanged(p1, p5))
}

// TestHasMesosCommandUriChanged checks Mesos CommandUri comparision util function
func TestHasMesosCommandUriChanged(t *testing.T) {
	tt := []struct {
		name          string
		prevUri       *mesosv1.CommandInfo_URI
		newUri        *mesosv1.CommandInfo_URI
		expectChanged bool
	}{
		{
			name:          "empty compare",
			prevUri:       &mesosv1.CommandInfo_URI{},
			newUri:        &mesosv1.CommandInfo_URI{},
			expectChanged: false,
		},
		{
			name:          "default extract compare 1",
			prevUri:       &mesosv1.CommandInfo_URI{},
			newUri:        &mesosv1.CommandInfo_URI{Extract: ptr.Bool(false)},
			expectChanged: true,
		},
		{
			name:    "default extract compare 2",
			prevUri: &mesosv1.CommandInfo_URI{},
			newUri: &mesosv1.CommandInfo_URI{
				Extract: ptr.Bool(true),
			},
			expectChanged: false,
		},
		{
			name:    "defaults compare",
			prevUri: &mesosv1.CommandInfo_URI{},
			newUri: &mesosv1.CommandInfo_URI{
				Value:      ptr.String(""),
				Extract:    ptr.Bool(true),
				Executable: ptr.Bool(false),
				Cache:      ptr.Bool(false),
				OutputFile: ptr.String(""),
			},
			expectChanged: false,
		},
		{
			name:    "defaults diff compare",
			prevUri: &mesosv1.CommandInfo_URI{},
			newUri: &mesosv1.CommandInfo_URI{
				Value:      ptr.String(""),
				Extract:    ptr.Bool(true),
				Executable: ptr.Bool(true),
				Cache:      ptr.Bool(false),
				OutputFile: ptr.String(""),
			},
			expectChanged: true,
		},
		{
			name: "values diff compare",
			prevUri: &mesosv1.CommandInfo_URI{
				Value:      ptr.String("blah"),
				Extract:    ptr.Bool(true),
				Executable: ptr.Bool(true),
				Cache:      ptr.Bool(false),
				OutputFile: ptr.String(""),
			},
			newUri: &mesosv1.CommandInfo_URI{
				Value:      ptr.String(""),
				Extract:    ptr.Bool(true),
				Executable: ptr.Bool(true),
				Cache:      ptr.Bool(false),
				OutputFile: ptr.String(""),
			},
			expectChanged: true,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			result := hasMesosCommandUriChanged(test.prevUri, test.newUri)
			assert.Equal(t, test.expectChanged, result)
		})
	}
}

// TestHasMesosCommandChanged checks Mesos Command comparision util function
func TestHasMesosCommandChanged(t *testing.T) {
	tt := []struct {
		name          string
		prevCommand   *mesosv1.CommandInfo
		newCommand    *mesosv1.CommandInfo
		expectChanged bool
	}{
		{
			name:          "nil compare",
			prevCommand:   nil,
			newCommand:    nil,
			expectChanged: false,
		},
		{
			name:          "one nil compare",
			prevCommand:   nil,
			newCommand:    &mesosv1.CommandInfo{},
			expectChanged: true,
		},
		{
			name:          "empty compare",
			prevCommand:   &mesosv1.CommandInfo{},
			newCommand:    &mesosv1.CommandInfo{},
			expectChanged: false,
		},
		{
			name: "uri length diff compare",
			prevCommand: &mesosv1.CommandInfo{
				Uris: []*mesosv1.CommandInfo_URI{{}, {}},
			},
			newCommand: &mesosv1.CommandInfo{
				Uris: []*mesosv1.CommandInfo_URI{{}},
			},
			expectChanged: true,
		},
		{
			name: "uri one nil compare",
			prevCommand: &mesosv1.CommandInfo{
				Uris: []*mesosv1.CommandInfo_URI{{}, {}},
			},
			newCommand:    &mesosv1.CommandInfo{},
			expectChanged: true,
		},
		{
			name: "uri diff compare",
			prevCommand: &mesosv1.CommandInfo{
				Uris: []*mesosv1.CommandInfo_URI{{
					Cache: ptr.Bool(true),
				}},
			},
			newCommand: &mesosv1.CommandInfo{
				Uris: []*mesosv1.CommandInfo_URI{{
					Cache: ptr.Bool(false),
				}},
			},
			expectChanged: true,
		},
		{
			name: "uri equal compare",
			prevCommand: &mesosv1.CommandInfo{
				Uris: []*mesosv1.CommandInfo_URI{{
					Cache: ptr.Bool(true),
				}},
			},
			newCommand: &mesosv1.CommandInfo{
				Uris: []*mesosv1.CommandInfo_URI{{
					Cache: ptr.Bool(true),
				}},
			},
			expectChanged: false,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			prevCommandCopy := proto.Clone(test.prevCommand).(*mesosv1.CommandInfo)
			newCommandCopy := proto.Clone(test.newCommand).(*mesosv1.CommandInfo)

			result := hasMesosCommandChanged(test.prevCommand, test.newCommand)
			assert.Equal(t, test.expectChanged, result)

			// Verify the commands are not modified after compare
			assert.True(t, proto.Equal(prevCommandCopy, test.prevCommand))
			assert.True(t, proto.Equal(newCommandCopy, test.newCommand))
		})
	}
}

// TestHasMesosContainerChanged checks Mesos Container comparision util function
func TestHasMesosContainerChanged(t *testing.T) {
	tt := []struct {
		name          string
		prevContainer *mesosv1.ContainerInfo
		newContainer  *mesosv1.ContainerInfo
		expectChanged bool
	}{
		{
			name:          "nil compare",
			prevContainer: nil,
			newContainer:  nil,
			expectChanged: false,
		},
		{
			name:          "one nil compare",
			prevContainer: nil,
			newContainer:  &mesosv1.ContainerInfo{},
			expectChanged: true,
		},
		{
			name:          "empty compare",
			prevContainer: &mesosv1.ContainerInfo{},
			newContainer:  &mesosv1.ContainerInfo{},
			expectChanged: false,
		},
		{
			name:          "one docker compare",
			prevContainer: &mesosv1.ContainerInfo{},
			newContainer: &mesosv1.ContainerInfo{
				Docker: &mesosv1.ContainerInfo_DockerInfo{},
			},
			expectChanged: true,
		},
		{
			name: "docker network default compare",
			prevContainer: &mesosv1.ContainerInfo{
				Docker: &mesosv1.ContainerInfo_DockerInfo{},
			},
			newContainer: &mesosv1.ContainerInfo{
				Docker: &mesosv1.ContainerInfo_DockerInfo{
					Network: mesosv1.ContainerInfo_DockerInfo_HOST.Enum(),
				},
			},
			expectChanged: false,
		},
		{
			name: "docker network value diff compare",
			prevContainer: &mesosv1.ContainerInfo{
				Docker: &mesosv1.ContainerInfo_DockerInfo{
					Network: mesosv1.ContainerInfo_DockerInfo_BRIDGE.Enum(),
				},
			},
			newContainer: &mesosv1.ContainerInfo{
				Docker: &mesosv1.ContainerInfo_DockerInfo{
					Network: mesosv1.ContainerInfo_DockerInfo_HOST.Enum(),
				},
			},
			expectChanged: true,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			prevContainerCopy := proto.Clone(test.prevContainer).(*mesosv1.ContainerInfo)
			newContainerCopy := proto.Clone(test.newContainer).(*mesosv1.ContainerInfo)

			result := hasMesosContainerChanged(test.prevContainer, test.newContainer)
			assert.Equal(t, test.expectChanged, result)

			// Verify the commands are not modified after compare
			assert.True(t, proto.Equal(prevContainerCopy, test.prevContainer))
			assert.True(t, proto.Equal(newContainerCopy, test.newContainer))
		})
	}
}

// TestHasTaskConfigChanged checks TaskConfig comparision util function
func TestHasTaskConfigChanged(t *testing.T) {
	t1 := &task.TaskConfig{
		Name: "task-1",
		Labels: []*peloton.Label{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
		},
		Ports: []*task.PortConfig{
			{Name: "port-1", Value: 10000},
			{Name: "port-2", Value: 10001},
		},
	}
	t2 := &task.TaskConfig{
		Name: "task-2",
		Labels: []*peloton.Label{
			{Key: "k2", Value: "v2"},
			{Key: "k1", Value: "v1"},
		},
		Ports: []*task.PortConfig{
			{Name: "port-2", Value: 10001},
			{Name: "port-1", Value: 10000},
		},
	}
	t3 := &task.TaskConfig{
		Name: "task-3",
		Labels: []*peloton.Label{
			{Key: "k2", Value: "v2"},
			{Key: "k1", Value: "v1"},
		},
		Ports: []*task.PortConfig{
			{Name: "port-2", Value: 10002},
			{Name: "port-1", Value: 10000},
		},
	}
	t4 := &task.TaskConfig{
		Name: "task-1",
		Labels: []*peloton.Label{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
		},
		Ports: []*task.PortConfig{
			{Name: "port-1", Value: 10000},
			{Name: "port-2", Value: 10001},
		},
		Container: &mesosv1.ContainerInfo{
			Docker: &mesosv1.ContainerInfo_DockerInfo{
				Network: mesosv1.ContainerInfo_DockerInfo_HOST.Enum(),
			},
		},
		Command: &mesosv1.CommandInfo{
			Uris: []*mesosv1.CommandInfo_URI{
				{
					Cache:      ptr.Bool(false),
					Executable: ptr.Bool(true),
					Extract:    ptr.Bool(true),
					Value:      ptr.String("value"),
				},
			},
		},
	}
	t5 := &task.TaskConfig{
		Name: "task-1",
		Labels: []*peloton.Label{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
		},
		Ports: []*task.PortConfig{
			{Name: "port-1", Value: 10000},
			{Name: "port-2", Value: 10001},
		},
		Container: &mesosv1.ContainerInfo{
			Docker: &mesosv1.ContainerInfo_DockerInfo{},
		},
		Command: &mesosv1.CommandInfo{
			Uris: []*mesosv1.CommandInfo_URI{
				{
					Executable: ptr.Bool(true),
					Value:      ptr.String("value"),
				},
			},
		},
	}

	testCases := []struct {
		name    string
		taskA   *task.TaskConfig
		taskB   *task.TaskConfig
		changed bool
	}{
		{
			"labels with different order should be the same",
			t1,
			t2,
			false,
		},
		{
			"ports with different value should be different",
			t1,
			t3,
			true,
		},
		{
			"command and container info with default values should be the same",
			t4,
			t5,
			false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.changed, HasTaskConfigChanged(tc.taskA, tc.taskB))
		})
	}
}

// TestHasContainerSpecChanged checks ContainerSpec comparision util function
func TestHasContainerSpecChanged(t *testing.T) {
	oldContainer := &pod.ContainerSpec{
		Name: "container",
		Ports: []*pod.PortSpec{
			{
				Name:    "name1",
				Value:   1111,
				EnvName: "env1",
			},
			{
				Name:    "name2",
				Value:   2222,
				EnvName: "env2",
			},
		},
	}
	newContainer := &pod.ContainerSpec{
		Name: "container",
		Ports: []*pod.PortSpec{
			{
				Name:    "name2",
				Value:   2222,
				EnvName: "env2",
			},
			{
				Name:    "name1",
				Value:   1111,
				EnvName: "env1",
			},
		},
	}

	assert.False(t, HasContainerSpecChanged(nil, nil))
	assert.True(t, HasContainerSpecChanged(oldContainer, nil))
	assert.True(t, HasContainerSpecChanged(nil, newContainer))
	assert.False(t, HasContainerSpecChanged(oldContainer, newContainer))
}

// TestHasPodSpecChanged checks PodSpec comparision util function
func TestHasPodSpecChanged(t *testing.T) {
	p1 := &pod.PodSpec{
		PodName: &v1peloton.PodName{Value: "pod-1"},
		Labels: []*v1peloton.Label{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
		},
		Containers: []*pod.ContainerSpec{
			{Name: "container-1"},
			{Name: "container-2"},
		},
	}
	p2 := &pod.PodSpec{
		PodName: &v1peloton.PodName{Value: "pod-2"},
		Labels: []*v1peloton.Label{
			{Key: "k2", Value: "v2"},
			{Key: "k1", Value: "v1"},
		},
		Containers: []*pod.ContainerSpec{
			{Name: "container-1"},
			{Name: "container-2"},
		},
	}
	p3 := &pod.PodSpec{
		PodName: &v1peloton.PodName{Value: "pod-3"},
		Labels: []*v1peloton.Label{
			{Key: "k2", Value: "v2"},
			{Key: "k1", Value: "v1"},
		},
		Containers: []*pod.ContainerSpec{
			{Name: "container-2"},
			{Name: "container-1"},
		},
	}
	p4 := &pod.PodSpec{
		PodName: &v1peloton.PodName{Value: "pod-3"},
		Labels: []*v1peloton.Label{
			{Key: "k2", Value: "v3"},
			{Key: "k1", Value: "v1"},
		},
		Containers: []*pod.ContainerSpec{
			{Name: "container-2"},
			{Name: "container-1"},
		},
	}
	p5 := &pod.PodSpec{
		PodName: &v1peloton.PodName{Value: "pod-3"},
		Labels: []*v1peloton.Label{
			{Key: "k2", Value: "v2"},
			{Key: "k1", Value: "v1"},
		},
		Containers: []*pod.ContainerSpec{
			{Name: "container-1"},
		},
	}
	p6 := &pod.PodSpec{
		PodName: &v1peloton.PodName{Value: "pod-1"},
		Labels: []*v1peloton.Label{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
		},
		InitContainers: []*pod.ContainerSpec{
			{Name: "container-1"},
			{Name: "container-2"},
		},
	}
	p7 := &pod.PodSpec{
		PodName: &v1peloton.PodName{Value: "pod-3"},
		Labels: []*v1peloton.Label{
			{Key: "k2", Value: "v2"},
			{Key: "k1", Value: "v1"},
		},
		InitContainers: []*pod.ContainerSpec{
			{Name: "container-2"},
			{Name: "container-1"},
		},
	}
	p8 := &pod.PodSpec{
		PodName: &v1peloton.PodName{Value: "pod-3"},
		Labels: []*v1peloton.Label{
			{Key: "k2", Value: "v2"},
			{Key: "k1", Value: "v1"},
		},
		InitContainers: []*pod.ContainerSpec{
			{Name: "container-1"},
		},
	}

	testCases := []struct {
		name    string
		podA    *pod.PodSpec
		podB    *pod.PodSpec
		changed bool
	}{
		{
			"labels with different order should be the same",
			p1,
			p2,
			false,
		},
		{
			"containers with different order should be different",
			p1,
			p3,
			true,
		},
		{
			"labels with different values should be different",
			p1,
			p4,
			true,
		},
		{
			"containers with different number of containers should be different",
			p1,
			p5,
			true,
		},
		{
			"init containers with different order should be different",
			p6,
			p7,
			true,
		},
		{
			"init containers with different number of containers should be different",
			p6,
			p8,
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.changed, HasPodSpecChanged(tc.podA, tc.podB))
		})
	}
}
