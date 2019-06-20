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

package models

import "github.com/uber/peloton/pkg/placement/plugins"

// Offer is the interface that represents a Host Offer in v0, or
// a Host Lease in v1alpha API.
type Offer interface {
	plugins.Host

	ID() string
	Hostname() string
}

// Task is the interface that represents a resource manager task. This
// is meant to wrap around the resource manager protos so we can keep the same logic
// for v0 and v1alpha API.
type Task interface {
	plugins.Task

	IsReadyForHostReservation() bool
	IsRevocable() bool
	SetPlacement(Offer)
	GetPlacementFailure() string
}
