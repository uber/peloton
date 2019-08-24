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

package fixture

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/label"
	"github.com/uber/peloton/pkg/aurorabridge/opaquedata"
	"github.com/uber/peloton/pkg/common/util/randutil"

	"github.com/pborman/uuid"
)

// PelotonEntityVersion returns a random EntityVersion.
func PelotonEntityVersion(vs ...int) *peloton.EntityVersion {
	entityVersion := make([]int, 3)
	for i := range entityVersion {
		if i < len(vs) {
			entityVersion[i] = vs[i]
			continue
		}
		entityVersion[i] = rand.Intn(10)
	}

	return &peloton.EntityVersion{
		Value: fmt.Sprintf(
			"%d-%d-%d",
			entityVersion[0],
			entityVersion[1],
			entityVersion[2],
		),
	}
}

// PelotonJobID returns a random JobID.
func PelotonJobID() *peloton.JobID {
	return &peloton.JobID{Value: uuid.New()}
}

// PelotonResourcePoolID returns a random ResourcePoolID.
func PelotonResourcePoolID() *peloton.ResourcePoolID {
	return &peloton.ResourcePoolID{
		Value: fmt.Sprintf("respool-%s", randutil.Text(8)),
	}
}

// PelotonUpdateSpec returns a random UpdateSpec.
func PelotonUpdateSpec() *stateless.UpdateSpec {
	return &stateless.UpdateSpec{
		BatchSize:                    uint32(randutil.Range(5, 10)),
		MaxInstanceRetries:           uint32(randutil.Range(1, 3)),
		MaxTolerableInstanceFailures: uint32(randutil.Range(1, 3)),
	}
}

// PelotonOpaqueData returns opaque data with a random update id.
func PelotonOpaqueData() *peloton.OpaqueData {
	d := &opaquedata.Data{UpdateID: uuid.New()}
	od, err := d.Serialize()
	if err != nil {
		panic(err)
	}
	return od
}

// PelotonWorkflowInfo returns a random WorkflowInfo.
func PelotonWorkflowInfo(eventTimestamp string) *stateless.WorkflowInfo {
	// Pick a random state. These were chosen fairly arbitrarily.
	var s stateless.WorkflowState
	for s = range map[stateless.WorkflowState]struct{}{
		stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD: {},
		stateless.WorkflowState_WORKFLOW_STATE_ROLLED_BACK:     {},
		stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED:       {},
		stateless.WorkflowState_WORKFLOW_STATE_ABORTED:         {},
	} {
	}

	if len(eventTimestamp) == 0 {
		eventTimestamp = randomTime()
	}

	return &stateless.WorkflowInfo{
		Status: &stateless.WorkflowStatus{
			State: s,
			Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
		},
		OpaqueData: PelotonOpaqueData(),
		Events: []*stateless.WorkflowEvent{
			{
				State:     stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED,
				Timestamp: eventTimestamp,
			},
		},
	}
}

// DefaultPelotonJobLabels returns a list of default labels for peloton jobs
func DefaultPelotonJobLabels(jobKey *api.JobKey) []*peloton.Label {
	mdl := label.NewAuroraMetadataLabels(AuroraMetadata())
	return append([]*peloton.Label{label.NewAuroraJobKey(jobKey)}, mdl...)
}

func randomTime() string {
	min := time.Date(2017, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2020, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0).Format(time.RFC3339)
}
