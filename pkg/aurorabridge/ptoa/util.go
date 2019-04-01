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

package ptoa

import (
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/common/util"
)

// WorkflowsByMaxTS sorts stateless.WorkflowInfo by timestamp of latest event
type WorkflowsByMaxTS []*stateless.WorkflowInfo

func (w WorkflowsByMaxTS) Len() int      { return len(w) }
func (w WorkflowsByMaxTS) Swap(i, j int) { w[i], w[j] = w[j], w[i] }
func (w WorkflowsByMaxTS) Less(i, j int) bool {
	wi := w[i]
	wj := w[j]

	// Ideally events should exist for both workflows,
	// but in case any of the workflow does not contain
	// any events, return false to keep the original
	// ordering when used with stable sort.
	if len(wi.GetEvents()) == 0 {
		return false
	}
	if len(wj.GetEvents()) == 0 {
		return false
	}

	// According to pkg/storage/cassandra/migrations/0022_add_job_update_events.up.cql
	// events returned should be sorted by descending create timestamp
	wiLatestEvent := wi.GetEvents()[0]
	wjLatestEvent := wj.GetEvents()[0]

	wiLatestUnix, err := util.ConvertTimestampToUnixSeconds(wiLatestEvent.GetTimestamp())
	if err != nil {
		return false
	}

	wjLatestUnix, err := util.ConvertTimestampToUnixSeconds(wjLatestEvent.GetTimestamp())
	if err != nil {
		return false
	}

	return wiLatestUnix < wjLatestUnix
}

// JobInstanceUpdateEventsByTimestamp sorts instance update events by timestamp.
type JobInstanceUpdateEventsByTimestamp []*api.JobInstanceUpdateEvent

func (s JobInstanceUpdateEventsByTimestamp) Len() int      { return len(s) }
func (s JobInstanceUpdateEventsByTimestamp) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s JobInstanceUpdateEventsByTimestamp) Less(i, j int) bool {
	return s[i].GetTimestampMs() < s[j].GetTimestampMs()
}
