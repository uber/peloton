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

	"github.com/uber/peloton/pkg/hostmgr/models"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
)

// Plugin interface defines the Northbound API which would be implemented for
// different underlying cluster management systems.
type Plugin interface {
	// Start the plugin.
	Start() error

	// Stop the plugin.
	Stop()

	// LaunchPods launch a list of pods on a host.
	LaunchPods(ctx context.Context, pods []*models.LaunchablePod, hostname string) (launched []*models.LaunchablePod, _ error)

	// KillPod kills a pod on a host.
	KillPod(ctx context.Context, podID string) error

	// AckPodEvent is only implemented by mesos plugin. For K8s this is a noop.
	AckPodEvent(event *scalar.PodEvent)

	// ReconcileHosts will return the current state of hosts in the cluster.
	ReconcileHosts() ([]*scalar.HostInfo, error)
}
