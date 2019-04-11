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

package noop

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNoopSecurityManager(t *testing.T) {
	m := NewNoopSecurityManager()
	u, err := m.Authenticate(nil)
	assert.NoError(t, err)

	assert.True(t, u.IsPermitted("peloton.api.v1alpha.job.stateless.svc.JobService::GetJob"))
	assert.True(t, u.IsPermitted("peloton.api.v1alpha.job.stateless.svc.JobService::CreateJob"))
	// even if the procedure name is not valid, still should pass permit check
	assert.True(t, u.IsPermitted(""))
}

func TestNoopSecurityClient(t *testing.T) {
	c := NewNoopSecurityClient()
	token := c.GetToken()
	assert.Len(t, token.Items(), 0)
	v, ok := token.Get("k1")
	assert.Len(t, v, 0)
	assert.False(t, ok)
}
