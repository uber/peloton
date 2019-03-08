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
)

const (
	_auroraJobKeyKey            = "aurora_job_key"
	_auroraJobKeyRoleKey        = "aurora_job_key_role"
	_auroraJobKeyEnvironmentKey = "aurora_job_key_environment"
	_auroraJobKeyNameKey        = "aurora_job_key_name"
)

// AuroraJobKey is a label for the original Aurora JobKey which was mapped into
// a job.
type AuroraJobKey struct {
	role, environment, name string
}

// NewAuroraJobKey creates a label for the original Aurora JobKey which was mapped
// into a Peloton job. Useful for simulating task per host limits.
func NewAuroraJobKey(k *api.JobKey) *peloton.Label {
	return &peloton.Label{
		Key:   _auroraJobKeyKey,
		Value: fmt.Sprintf("%s/%s/%s", k.GetRole(), k.GetEnvironment(), k.GetName()),
	}
}

// NewAuroraJobKeyRole creates a label for the original Aurora JobKey role which
// was mapped into a Peloton job. Useful for querying jobs by role.
func NewAuroraJobKeyRole(role string) *peloton.Label {
	return &peloton.Label{
		Key:   _auroraJobKeyRoleKey,
		Value: role,
	}
}

// NewAuroraJobKeyEnvironment creates a label for the original Aurora JobKey
// environment was mapped into a Peloton job. Useful for querying jobs by
// environment.
func NewAuroraJobKeyEnvironment(env string) *peloton.Label {
	return &peloton.Label{
		Key:   _auroraJobKeyEnvironmentKey,
		Value: env,
	}
}

// NewAuroraJobKeyName creates a label for the original Aurora JobKey name which
// was mapped into a Peloton job. Useful for querying jobs by name.
func NewAuroraJobKeyName(name string) *peloton.Label {
	return &peloton.Label{
		Key:   _auroraJobKeyNameKey,
		Value: name,
	}
}

// BuildPartialAuroraJobKeyLabels is a utility for creating labels for each set
// parameter. Useful for job queries.
func BuildPartialAuroraJobKeyLabels(role, env, name string) []*peloton.Label {
	var ls []*peloton.Label
	if role != "" {
		ls = append(ls, NewAuroraJobKeyRole(role))
	}
	if env != "" {
		ls = append(ls, NewAuroraJobKeyEnvironment(env))
	}
	if name != "" {
		ls = append(ls, NewAuroraJobKeyName(name))
	}
	return ls
}
