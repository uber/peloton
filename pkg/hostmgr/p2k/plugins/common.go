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

package plugins

import (
	"context"

	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/pkg/hostmgr/p2k/plugins/k8s"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
)

// NewK8sPlugin returns a new instance of k8s plugin.
func NewK8sPlugin(
	configPath string,
	podEventsCh chan<- *scalar.PodEvent,
	hostEventCh chan<- *scalar.HostEvent,
) (Plugin, error) {
	return k8s.NewK8sManager(configPath, podEventsCh, hostEventCh)
}

func NewNoopPlugin() Plugin {
	return &NoopPlugin{}
}

type NoopPlugin struct{}

// Start the plugin.
func (p *NoopPlugin) Start() error {
	return nil
}

// Stop the plugin.
func (p *NoopPlugin) Stop() {}

// LaunchPod launches a pod on a host.
func (p *NoopPlugin) LaunchPod(podSpec *pbpod.PodSpec, podID, hostname string) error {
	return nil
}

// KillPod kills a pod on a host.
func (p *NoopPlugin) KillPod(podID string) error {
	return nil
}

// AckPodEvent is only implemented by mesos plugin. For K8s this is a noop.
func (p *NoopPlugin) AckPodEvent(ctx context.Context, event *scalar.PodEvent) {}

// ReconcileHosts will return the current state of hosts in the cluster.
func (p *NoopPlugin) ReconcileHosts() ([]*scalar.HostInfo, error) {
	return nil, nil
}
