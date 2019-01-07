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

package mhttp

import "github.com/uber-go/tally"

// Metrics hold all metrics related to mhttp.
type Metrics struct {
	Running tally.Gauge
	Stopped tally.Gauge

	LeaderChanges tally.Counter
	StartCount    tally.Counter
	StopCount     tally.Counter

	Frames tally.Counter

	ReadLineError    tally.Counter
	FrameLengthError tally.Counter
	LineLengthError  tally.Counter
	RecordIOError    tally.Counter
}

func newMetrics(parent tally.Scope) *Metrics {
	scope := parent.SubScope("mhttp")
	errScope := scope.SubScope("errors")
	return &Metrics{
		Running: scope.Gauge("running"),
		Stopped: scope.Gauge("stopped"),

		LeaderChanges: scope.Counter("leader_changes"),
		StartCount:    scope.Counter("start"),
		StopCount:     scope.Counter("stop"),

		Frames: scope.Counter("frames"),

		ReadLineError:    errScope.Counter("read_line"),
		FrameLengthError: errScope.Counter("frame_length"),
		LineLengthError:  errScope.Counter("line_length"),
		RecordIOError:    errScope.Counter("recordio"),
	}
}
