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

	"github.com/stretchr/testify/assert"
)

func TestBuildPartialAuroraJobKeyLabels(t *testing.T) {
	const (
		role = "test-role"
		env  = "test-env"
		name = "test-name"
	)

	testCases := []struct {
		desc            string
		role, env, name string
		wantLabels      []*peloton.Label
	}{
		{
			"all fields set",
			role, env, name,
			[]*peloton.Label{
				NewAuroraJobKeyRole(role),
				NewAuroraJobKeyEnvironment(env),
				NewAuroraJobKeyName(name),
			},
		}, {
			"only role set",
			role, "", "",
			[]*peloton.Label{
				NewAuroraJobKeyRole(role),
			},
		}, {
			"role and environment set",
			role, env, "",
			[]*peloton.Label{
				NewAuroraJobKeyRole(role),
				NewAuroraJobKeyEnvironment(env),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.Equal(t,
				tc.wantLabels,
				BuildPartialAuroraJobKeyLabels(tc.role, tc.env, tc.name))
		})
	}
}
