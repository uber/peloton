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

package models

import (
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
)

func setupAssignmentVariables() (
	*hostsvc.HostOffer,
	*resmgrsvc.Gang,
	*resmgr.Task,
	*HostOffers,
	*Task,
	*Assignment) {
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
	return hostOffer, resmgrGang, resmgrTask, offer, task, assignment
}

func TestAssignment_Task(t *testing.T) {
	_, _, _, _, task, assignment := setupAssignmentVariables()
	assert.Equal(t, task, assignment.GetTask())

	task.SetMaxRounds(5)
	assignment.SetTask(task)
	assert.Equal(t, 5, assignment.GetTask().GetMaxRounds())
}

func TestAssignment_Offer(t *testing.T) {
	_, _, _, _, _, assignment := setupAssignmentVariables()
	assert.Nil(t, assignment.GetHost())
}

func TestAssignment_SetOffer(t *testing.T) {
	_, _, _, host, _, assignment := setupAssignmentVariables()
	assignment.SetHost(host)
	assert.Equal(t, host, assignment.GetHost())
}

func TestTest(t *testing.T) {
	log.SetFormatter(&log.JSONFormatter{})
	initialLevel := log.DebugLevel
	log.SetLevel(initialLevel)

	_, _, _, host, _, assignment := setupAssignmentVariables()
	assignment.SetHost(host)
	entry, err := log.WithField("foo", assignment).String()
	assert.NoError(t, err)
	assert.Contains(t, entry, "foo")
	assert.Contains(t, entry, "host")
	assert.Contains(t, entry, "offer")
	assert.Contains(t, entry, "tasks")
	assert.Contains(t, entry, "claimed")
	assert.Contains(t, entry, "deadline")
	assert.Contains(t, entry, "max_rounds")
	assert.Contains(t, entry, "rounds")
}
