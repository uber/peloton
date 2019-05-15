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

package leader

// Nomination represents the set of callbacks to handle leadership
// election
type Nomination interface {
	// GainedLeadershipCallback is the callback when the current node
	// becomes the leader
	GainedLeadershipCallback() error

	// HasGainedLeadership returns true iff once GainedLeadershipCallback
	// completes
	HasGainedLeadership() bool

	// ShutDownCallback is the callback to shut down gracefully if
	// possible
	ShutDownCallback() error

	// LostLeadershipCallback is the callback when the leader lost
	// leadership
	LostLeadershipCallback() error

	// GetID returns the host:master_port of the node running for
	// leadership (i.e. the ID)
	GetID() string
}

// Candidate is an interface representing both a candidate campaigning
// to become a leader, and the observer that watches state changes in
// leadership to be notified about changes
type Candidate interface {
	IsLeader() bool
	Start() error
	Stop() error
	Resign()
}
