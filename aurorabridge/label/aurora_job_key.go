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

package label

import (
	"fmt"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"go.uber.org/thriftrw/ptr"
)

const _auroraJobKeyKey = "aurora_job_key"
const _auroraJobKeyRoleKey = "aurora_job_key_role"
const _auroraJobKeyEnvironmentKey = "aurora_job_key_environment"
const _auroraJobKeyNameKey = "aurora_job_key_jobname"

// AuroraJobKey is a label for the original Aurora JobKey which was mapped into
// a job.
type AuroraJobKey struct {
	role, environment, name string
}

// NewAuroraJobKey creates a new AuroraJobKey label.
func NewAuroraJobKey(k *api.JobKey) *AuroraJobKey {
	return &AuroraJobKey{
		role:        k.GetRole(),
		environment: k.GetEnvironment(),
		name:        k.GetName(),
	}
}

// Key returns the label key.
func (i *AuroraJobKey) Key() string {
	return _auroraJobKeyKey
}

// Value returns the label value.
func (i *AuroraJobKey) Value() string {
	return fmt.Sprintf("%s/%s/%s", i.role, i.environment, i.name)
}

// BuildAuroraJobKeyLabels returns a list of Label for each of the parameter
// provided, the paramter is skipped if it's empty. Those labels will be
// used to query by job key or partial job key (role + environment or role
// etc.).
func BuildAuroraJobKeyLabels(
	role string,
	environment string,
	jobName string) []Label {
	var labels []Label
	if len(role) > 0 {
		labels = append(labels, &rawLabel{
			key:   _auroraJobKeyRoleKey,
			value: role,
		})
	}
	if len(environment) > 0 {
		labels = append(labels, &rawLabel{
			key:   _auroraJobKeyEnvironmentKey,
			value: environment,
		})
	}
	if len(jobName) > 0 {
		labels = append(labels, &rawLabel{
			key:   _auroraJobKeyNameKey,
			value: jobName,
		})
	}
	return labels
}

// BuildAuroraJobKeyFromLabels converts a list of Peloton labels to
// Aurora JobKey.
func BuildAuroraJobKeyFromLabels(labels []*peloton.Label) *api.JobKey {
	jobKey := &api.JobKey{}
	for _, label := range labels {
		switch label.GetKey() {
		case _auroraJobKeyRoleKey:
			jobKey.Role = ptr.String(label.GetValue())
		case _auroraJobKeyEnvironmentKey:
			jobKey.Environment = ptr.String(label.GetValue())
		case _auroraJobKeyNameKey:
			jobKey.Name = ptr.String(label.GetValue())
		}
	}
	return jobKey
}
