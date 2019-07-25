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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
)

func setupHostVariables() (time.Time, *hostsvc.HostOffer, *resmgrsvc.Gang, *resmgr.Task, *HostOffers, *TaskV0, *Assignment) {
	resmgrTask := &resmgr.Task{
		Name: "task",
	}
	hostOffer := &hostsvc.HostOffer{
		Hostname: "hostname",
	}
	now := time.Now()
	offer := NewHostOffers(hostOffer, []*resmgr.Task{resmgrTask}, now)
	resmgrGang := &resmgrsvc.Gang{
		Tasks: []*resmgr.Task{
			resmgrTask,
		},
	}
	task := NewTask(resmgrGang, resmgrTask, now.Add(5*time.Second), now, 3)
	assignment := NewAssignment(task)
	return now, hostOffer, resmgrGang, resmgrTask, offer, task, assignment
}

func TestHost_Offer(t *testing.T) {
	_, hostOffer, _, _, host, _, _ := setupHostVariables()
	assert.Equal(t, hostOffer, host.GetOffer())
}

func TestHost_Tasks(t *testing.T) {
	_, _, resmgrGang, _, host, _, _ := setupHostVariables()
	assert.Equal(t, resmgrGang.GetTasks(), host.GetTasks())
}

func TestHost_DataAndSetData(t *testing.T) {
	_, _, _, _, host, _, _ := setupHostVariables()
	assert.Nil(t, host.Data())
	host.SetData(42)
	assert.NotNil(t, host.Data())
}

func TestHost_Age(t *testing.T) {
	now, _, _, _, host, _, _ := setupHostVariables()
	assert.Equal(t, time.Duration(0), host.Age(now))
	assert.Equal(t, 2*time.Second, host.Age(now.Add(2*time.Second)))
}
