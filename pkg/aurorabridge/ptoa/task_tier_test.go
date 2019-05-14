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
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"

	"github.com/stretchr/testify/assert"
)

// TestNewTaskTier checks NewTaskTier returns TaskTier string correctly
// based on input SlaSpec.
func TestNewTaskTier(t *testing.T) {
	testCases := []struct {
		name        string
		preemptible bool
		revocable   bool
		wantTier    string
	}{
		{
			"revocable tier",
			true,
			true,
			"revocable",
		},
		{
			"preemptible tier",
			true,
			false,
			"preemptible",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantTier, *NewTaskTier(&stateless.SlaSpec{
				Preemptible: tc.preemptible,
				Revocable:   tc.revocable,
			}))
		})
	}
}
