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

package progress

import (
	"context"
	"time"

	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/pkg/common/background"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/jobmgr/cached"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
)

const _workflowProgressCheckName = "workflowProgressCheck"

// WorkflowProgressCheck checks if workflows are making progress periodically.
// And emit metrics if any workflow has not been updated within StaleWorkflowThreshold.
// It is used to catch potential slow workflow and bugs in the system.
type WorkflowProgressCheck struct {
	JobFactory cached.JobFactory
	Metrics    *Metrics
	Config     *Config
}

func (u *WorkflowProgressCheck) Register(manager background.Manager) error {
	if u.Config == nil {
		u.Config = &Config{}
	}

	u.Config.normalize()
	return manager.RegisterWorks(
		background.Work{
			Name: _workflowProgressCheckName,
			Func: func(_ *atomic.Bool) {
				u.Check()
			},
			Period: u.Config.WorkflowProgressCheckPeriod,
		},
	)
}

func (u *WorkflowProgressCheck) Check() {
	var totalWorkflow uint
	var totalActiveWorkflow uint
	var totalStaleWorkflow uint

	var getJobRuntimeFailure uint

	stopWatch := u.Metrics.ProcessDuration.Start()
	defer stopWatch.Stop()

	jobs := u.JobFactory.GetAllJobs()
	for _, cachedJob := range jobs {
		// batch cachedJob does not have workflow attached, just skip
		// the check
		if cachedJob.GetJobType() == pbjob.JobType_BATCH {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		runtime, err := cachedJob.GetRuntime(ctx)
		if err != nil {
			getJobRuntimeFailure++
			log.WithField("job_id", cachedJob.ID().GetValue()).
				WithError(err).
				Info("failed to get job runtime to check workflow progress")
			cancel()
			continue
		}

		if len(runtime.GetUpdateID().GetValue()) == 0 {
			cancel()
			continue
		}

		totalWorkflow++
		cachedWorkflow := cachedJob.GetWorkflow(runtime.GetUpdateID())
		if cachedWorkflow == nil {
			cancel()
			continue
		}

		if cached.IsUpdateStateActive(cachedWorkflow.GetState().State) {
			totalActiveWorkflow++
		} else {
			cancel()
			continue
		}

		lastUpdateTime := cachedWorkflow.GetLastUpdateTime()
		if time.Now().Sub(lastUpdateTime) > u.Config.StaleWorkflowThreshold &&
			!isWorkflowStaleDueToTaskThrottling(ctx, cachedJob, cachedWorkflow) {
			log.WithFields(log.Fields{
				"job_id":           cachedJob.ID().GetValue(),
				"update_id":        runtime.GetUpdateID().GetValue(),
				"last_update_time": lastUpdateTime,
				"update_type":      cachedWorkflow.GetWorkflowType().String(),
			}).Warn("workflow has not made progress and passed staleness threshold")
			totalStaleWorkflow++
		}
		cancel()
	}

	u.Metrics.TotalWorkflow.Update(float64(totalWorkflow))
	u.Metrics.TotalActiveWorkflow.Update(float64(totalActiveWorkflow))
	u.Metrics.TotalStaleWorkflow.Update(float64(totalStaleWorkflow))

	u.Metrics.GetJobRuntimeFailure.Update(float64(getJobRuntimeFailure))
}

// when a task fails, it would be restarted but is subject
// to throttling. As a result, the workflow may not
// see progress updated within the StaleWorkflowThreshold.
// This function checks if the staleness is solely due
// to task throttling. It is true if every task that is being
// processed by the workflow is subject to throttling.
func isWorkflowStaleDueToTaskThrottling(
	ctx context.Context,
	cachedJob cached.Job,
	cachedWorkflow cached.Update,
) bool {
	for _, instID := range cachedWorkflow.GetInstancesCurrent() {
		cachedTask := cachedJob.GetTask(instID)
		if cachedTask == nil {
			continue
		}

		taskRuntime, err := cachedTask.GetRuntime(ctx)
		if err != nil {
			log.WithField("job_id", cachedJob.ID().GetValue()).
				WithField("instance_id", instID).
				WithError(err).
				Info("failed to get task runtime to check workflow progress")
			continue
		}

		if !util.IsTaskThrottled(taskRuntime.GetState(), taskRuntime.GetMessage()) {
			return false
		}
	}

	return true
}
