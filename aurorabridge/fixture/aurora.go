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
	"github.com/uber/peloton/util/randutil"
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
