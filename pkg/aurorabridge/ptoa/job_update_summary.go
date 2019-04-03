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
	"fmt"

	"go.uber.org/thriftrw/ptr"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/opaquedata"
)

// NewJobUpdateSummary creates a new aurora job update summary using update info.
func NewJobUpdateSummary(
	k *api.JobKey,
	w *stateless.WorkflowInfo,
) (*api.JobUpdateSummary, error) {

	d, err := opaquedata.Deserialize(w.GetOpaqueData())
	if err != nil {
		return nil, fmt.Errorf("deserialize opaque data: %s", err)
	}

	status, err := NewJobUpdateStatus(w.GetStatus().GetState(), d)
	if err != nil {
		return nil, fmt.Errorf("new job update status: %s", err)
	}

	var createTime *int64
	var lastModifiedTime *int64
	numEvents := len(w.GetEvents())
	if numEvents > 0 {
		createTime, err = common.ConvertTimestampToUnixMS(
			w.GetEvents()[numEvents-1].GetTimestamp())
		if err != nil {
			return nil, err
		}
		lastModifiedTime = createTime
	}
	if numEvents > 1 {
		lastModifiedTime, err = common.ConvertTimestampToUnixMS(
			w.GetEvents()[0].GetTimestamp())
		if err != nil {
			return nil, err
		}
	}

	return &api.JobUpdateSummary{
		Key: &api.JobUpdateKey{
			Job: k,
			ID:  ptr.String(d.UpdateID),
		},
		State: &api.JobUpdateState{
			Status:                  &status,
			CreatedTimestampMs:      createTime,
			LastModifiedTimestampMs: lastModifiedTime,
		},
		Metadata: d.UpdateMetadata,
	}, nil
}
