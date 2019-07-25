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

package models_v0

import (
	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"testing"
)

// setupHosts sets up the host info and task which all tests will use
func setupHosts() (*Host, *resmgr.Task, *hostsvc.HostInfo) {
	resmgrTask := &resmgr.Task{
		Name: "task",
	}
	hostInfo := &hostsvc.HostInfo{
		Hostname: "hostname",
	}
	host := NewHosts(hostInfo, []*resmgr.Task{resmgrTask})

	return host, resmgrTask, hostInfo
}

func TestHosts(t *testing.T) {
	host, _, _ := setupHosts()
	assert.Equal(t, "hostname", host.GetHost().Hostname)
}

func TestHosts_Tasks(t *testing.T) {
	host, task, _ := setupHosts()
	assert.Equal(t, task.Name, host.GetTasks()[0].Name)
}
