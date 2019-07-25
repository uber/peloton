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
	"sync"
	"time"

	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
)

// NewTask will create a new placement task from a resource manager task,
// and the gang it belongs to.
func NewTask(
	gang *resmgrsvc.Gang,
	task *resmgr.Task,
	deadline time.Time,
	placementDeadline time.Time,
	maxRounds int) *TaskV0 {
	return &TaskV0{
		Gang:              gang,
		Task:              task,
		Deadline:          deadline,
		MaxRounds:         maxRounds,
		PlacementDeadline: placementDeadline,
	}
}

// TaskV0 represents a Placement task, a Mimir placement entity can also be obtained from it.
type TaskV0 struct {
	Gang *resmgrsvc.Gang `json:"gang"`
	Task *resmgr.Task    `json:"task"`
	// Deadline when the task should successfully placed or have failed to do so.
	Deadline time.Time `json:"deadline"`
	// MaxRounds is the maximal number of successful placement Rounds.
	MaxRounds int `json:"max_rounds"`
	// Rounds is the current number of successful placement Rounds.
	Rounds int `json:"rounds"`
	// PlacementDeadline when the task should successfully placed
	// on the desired host or have failed to do so.
	PlacementDeadline time.Time `json:"placement_deadline"`
	// data is used by placement strategies to transfer state between calls to the
	// place once method.
	data interface{}
	lock sync.Mutex
}

// GetGang returns the resource manager gang of the task.
func (task *TaskV0) GetGang() *resmgrsvc.Gang {
	return task.Gang
}

// SetGang sets the resource manager gang of the task.
func (task *TaskV0) SetGang(gang *resmgrsvc.Gang) {
	task.Gang = gang
}

// GetTask returns the resource manager task of the task.
func (task *TaskV0) GetTask() *resmgr.Task {
	return task.Task
}

// SetTask sets the resource manager task of the task.
func (task *TaskV0) SetTask(resmgrTask *resmgr.Task) {
	task.Task = resmgrTask
}

// GetDeadline returns the deadline of the task.
func (task *TaskV0) GetDeadline() time.Time {
	return task.Deadline
}

// SetDeadline sets the deadline of the task.
func (task *TaskV0) SetDeadline(deadline time.Time) {
	task.Deadline = deadline
}

// GetMaxRounds returns the max rounds of the task.
func (task *TaskV0) GetMaxRounds() int {
	return task.MaxRounds
}

// SetMaxRounds sets the max rounds of the task.
func (task *TaskV0) SetMaxRounds(maxRounds int) {
	task.MaxRounds = maxRounds
}

// GetRounds returns the rounds of the task.
func (task *TaskV0) GetRounds() int {
	return task.Rounds
}

// SetRounds sets the rounds of the task.
func (task *TaskV0) SetRounds(rounds int) {
	task.Rounds = rounds
}

// SetData will set the data transfer object on the task.
func (task *TaskV0) SetData(data interface{}) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.data = data
}

// Data will return the data transfer object of the task.
func (task *TaskV0) Data() interface{} {
	task.lock.Lock()
	defer task.lock.Unlock()
	return task.data
}

// IncRounds will increment the number of placement rounds that the task have been through.
func (task *TaskV0) IncRounds() {
	task.Rounds++
}

// PastMaxRounds returns true iff the task has gone through its maximal number of placement rounds.
func (task *TaskV0) PastMaxRounds() bool {
	return task.Rounds >= task.MaxRounds
}

// PastDeadline will return true iff the deadline for the gang has passed.
func (task *TaskV0) PastDeadline(now time.Time) bool {
	return now.After(task.Deadline)
}

// PastDesiredHostPlacementDeadline returns true iff the deadline for
// placing the task onto its desired host has passed.
func (task *TaskV0) PastDesiredHostPlacementDeadline(now time.Time) bool {
	return now.After(task.PlacementDeadline)
}
