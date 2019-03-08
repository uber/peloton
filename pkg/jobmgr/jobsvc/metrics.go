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

package jobsvc

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track
// internal state of the job service
type Metrics struct {
	JobAPICreate   tally.Counter
	JobCreate      tally.Counter
	JobCreateFail  tally.Counter
	JobAPIGet      tally.Counter
	JobGet         tally.Counter
	JobGetFail     tally.Counter
	JobAPIDelete   tally.Counter
	JobDelete      tally.Counter
	JobDeleteFail  tally.Counter
	JobAPIQuery    tally.Counter
	JobQuery       tally.Counter
	JobQueryFail   tally.Counter
	JobAPIUpdate   tally.Counter
	JobUpdate      tally.Counter
	JobUpdateFail  tally.Counter
	JobAPIRefresh  tally.Counter
	JobRefresh     tally.Counter
	JobRefreshFail tally.Counter
	JobAPIRestart  tally.Counter
	JobRestart     tally.Counter
	JobRestartFail tally.Counter
	JobAPIStart    tally.Counter
	JobStart       tally.Counter
	JobStartFail   tally.Counter
	JobAPIStop     tally.Counter
	JobStop        tally.Counter
	JobStopFail    tally.Counter

	JobAPIGetByRespoolID  tally.Counter
	JobGetByRespoolID     tally.Counter
	JobGetByRespoolIDFail tally.Counter

	// Timers
	JobQueryHandlerDuration tally.Timer

	// TODO: find a better way of organizing metrics per package
	TaskCreate     tally.Counter
	TaskCreateFail tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	jobSuccessScope := scope.Tagged(map[string]string{"result": "success"})
	jobFailScope := scope.Tagged(map[string]string{"result": "fail"})
	jobAPIScope := scope.SubScope("api")

	// TODO: find a better way of organizing metrics per package so we
	// don't have to nest task scope under job scope here.
	taskScope := scope.SubScope("task")
	taskSuccessScope := taskScope.Tagged(map[string]string{"result": "success"})
	taskFailScope := taskScope.Tagged(map[string]string{"result": "fail"})

	return &Metrics{
		JobAPICreate:   jobAPIScope.Counter("create"),
		JobCreate:      jobSuccessScope.Counter("create"),
		JobCreateFail:  jobFailScope.Counter("create"),
		JobAPIGet:      jobAPIScope.Counter("get"),
		JobGet:         jobSuccessScope.Counter("get"),
		JobGetFail:     jobFailScope.Counter("get"),
		JobAPIDelete:   jobAPIScope.Counter("delete"),
		JobDelete:      jobSuccessScope.Counter("delete"),
		JobDeleteFail:  jobFailScope.Counter("delete"),
		JobAPIQuery:    jobAPIScope.Counter("query"),
		JobQuery:       jobSuccessScope.Counter("query"),
		JobQueryFail:   jobFailScope.Counter("query"),
		JobAPIUpdate:   jobAPIScope.Counter("update"),
		JobUpdate:      jobSuccessScope.Counter("update"),
		JobUpdateFail:  jobFailScope.Counter("update"),
		TaskCreate:     taskSuccessScope.Counter("create"),
		TaskCreateFail: taskFailScope.Counter("create"),
		JobAPIRefresh:  jobAPIScope.Counter("refresh"),
		JobRefresh:     jobSuccessScope.Counter("refresh"),
		JobRefreshFail: jobFailScope.Counter("refresh"),
		JobAPIRestart:  jobAPIScope.Counter("restart"),
		JobRestart:     jobSuccessScope.Counter("restart"),
		JobRestartFail: jobFailScope.Counter("restart"),
		JobAPIStart:    jobAPIScope.Counter("start"),
		JobStart:       jobSuccessScope.Counter("start"),
		JobStartFail:   jobFailScope.Counter("start"),
		JobAPIStop:     jobAPIScope.Counter("stop"),
		JobStop:        jobSuccessScope.Counter("stop"),
		JobStopFail:    jobFailScope.Counter("stop"),

		JobQueryHandlerDuration: jobAPIScope.Timer("job_query_duration"),

		JobAPIGetByRespoolID:  jobAPIScope.Counter("get_by_respool_id"),
		JobGetByRespoolID:     jobSuccessScope.Counter("get_by_respool_id"),
		JobGetByRespoolIDFail: jobFailScope.Counter("get_by_respool_id"),
	}
}
