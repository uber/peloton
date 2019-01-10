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
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"go.uber.org/thriftrw/ptr"

	"github.com/stretchr/testify/assert"
)

func TestBuildPartialAuroraJobKeyLabels(t *testing.T) {
	const (
		role = "test-role"
		env  = "test-env"
		name = "test-name"
	)

	testCases := []struct {
		name       string
		key        *api.JobKey
		wantLabels []*peloton.Label
	}{
		{
			"all fields set",
			&api.JobKey{
				Role:        ptr.String(role),
				Environment: ptr.String(env),
				Name:        ptr.String(name),
			},
			[]*peloton.Label{
				NewAuroraJobKeyRole(role),
				NewAuroraJobKeyEnvironment(env),
				NewAuroraJobKeyName(name),
			},
		}, {
			"only role set",
			&api.JobKey{
				Role: ptr.String(role),
			},
			[]*peloton.Label{
				NewAuroraJobKeyRole(role),
			},
		}, {
			"role and environment set",
			&api.JobKey{
				Role:        ptr.String(role),
				Environment: ptr.String(env),
			},
			[]*peloton.Label{
				NewAuroraJobKeyRole(role),
				NewAuroraJobKeyEnvironment(env),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantLabels, BuildPartialAuroraJobKeyLabels(tc.key))
		})
	}
}
