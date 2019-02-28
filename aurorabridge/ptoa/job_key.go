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
	"strings"

	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"go.uber.org/thriftrw/ptr"
)

// NewJobKey creates Aurora JobKey from Peloton job name
// ("<role>/<environment>/<job_name>")
func NewJobKey(jobName string) (*api.JobKey, error) {
	ks := strings.Split(jobName, "/")
	if len(ks) != 3 {
		return nil, fmt.Errorf("invalid job name: %q", jobName)
	}
	for _, k := range ks {
		if len(k) == 0 {
			return nil, fmt.Errorf("invalid job name: %q", jobName)
		}
	}
	return &api.JobKey{
		Role:        ptr.String(ks[0]),
		Environment: ptr.String(ks[1]),
		Name:        ptr.String(ks[2]),
	}, nil
}
