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

	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/pkg/common/util/randutil"
	"go.uber.org/thriftrw/ptr"
)

// AuroraJobKey returns a random JobKey.
func AuroraJobKey() *api.JobKey {
	return &api.JobKey{
		Role:        ptr.String(fmt.Sprintf("svc-%s", randutil.Text(6))),
		Environment: ptr.String(fmt.Sprintf("dep-%s", randutil.Text(6))),
		Name:        ptr.String(fmt.Sprintf("app-%s", randutil.Text(6))),
	}
}

// AuroraTaskConfig returns a random TaskConfig.
func AuroraTaskConfig() *api.TaskConfig {
	return &api.TaskConfig{
		Job: AuroraJobKey(),
	}
}

// AuroraJobUpdateRequest returns a random JobUpdateRequest.
func AuroraJobUpdateRequest() *api.JobUpdateRequest {
	return &api.JobUpdateRequest{
		TaskConfig: AuroraTaskConfig(),
	}
}

// AuroraJobUpdateKey returns a random JobUpdateKey.
func AuroraJobUpdateKey() *api.JobUpdateKey {
	return &api.JobUpdateKey{
		Job: AuroraJobKey(),
		ID:  ptr.String(fmt.Sprintf("update-id-%s", randutil.Text(6))),
	}
}

// AuroraTaskQuery returns a random TaskQuery containing a random job_key.
func AuroraTaskQuery() *api.TaskQuery {
	return &api.TaskQuery{
		JobKeys: []*api.JobKey{
			AuroraJobKey(),
		},
	}
}

// AuroraMetadata returns a list of random Metadata.
func AuroraMetadata() []*api.Metadata {
	var m []*api.Metadata
	for i := 0; i < 6; i++ {
		m = append(m, &api.Metadata{
			Key:   ptr.String(fmt.Sprintf("key-%s", randutil.Text(6))),
			Value: ptr.String(fmt.Sprintf("val-%s", randutil.Text(6))),
		})
	}
	return m
}

// AuroraInstanceSet returns n random instances whose values are less than
// max with no duplicates. Panics if n > max.
func AuroraInstanceSet(n, max int) map[int32]struct{} {
	if n > max {
		panic("n must be less than max")
	}

	// Map of all possible choices.
	choices := make(map[int32]struct{})
	for i := 0; i < max; i++ {
		choices[int32(i)] = struct{}{}
	}

	// Add first n instances of choices in random order.
	set := make(map[int32]struct{})
	for i := range choices {
		if len(set) == n {
			break
		}
		set[i] = struct{}{}
	}
	return set
}
