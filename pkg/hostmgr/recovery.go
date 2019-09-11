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

package hostmgr

import (
	"context"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/recovery"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/hostmgr/metrics"
	"github.com/uber/peloton/pkg/hostmgr/offer"
	"github.com/uber/peloton/pkg/hostmgr/p2k/hostcache"
	"github.com/uber/peloton/pkg/storage"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

// RecoveryHandler defines the interface to
// be called by leader election callbacks.
type RecoveryHandler interface {
	Start() error
	Stop() error
}

// recoveryHandler restores the contents of MaintenanceQueue
// from Mesos Maintenance Status
type recoveryHandler struct {
	metrics       *metrics.Metrics
	recoveryScope tally.Scope

	taskStore     storage.TaskStore
	hostCache     hostcache.HostCache
	activeJobsOps ormobjects.ActiveJobsOps
	jobConfigOps  ormobjects.JobConfigOps
	jobRuntimeOps ormobjects.JobRuntimeOps
}

// NewRecoveryHandler creates a recoveryHandler
func NewRecoveryHandler(
	parent tally.Scope,
	taskStore storage.TaskStore,
	ormStore *ormobjects.Store,
	hostCache hostcache.HostCache,
) RecoveryHandler {
	recovery := &recoveryHandler{
		metrics:       metrics.NewMetrics(parent),
		recoveryScope: parent.SubScope("recovery"),

		hostCache:     hostCache,
		taskStore:     taskStore,
		activeJobsOps: ormobjects.NewActiveJobsOps(ormStore),
		jobConfigOps:  ormobjects.NewJobConfigOps(ormStore),
		jobRuntimeOps: ormobjects.NewJobRuntimeOps(ormStore),
	}
	return recovery
}

// Stop is a no-op for recovery handler
func (r *recoveryHandler) Stop() error {
	log.Info("Stopping recovery")
	return nil
}

// Start requeues all 'DRAINING' hosts into maintenance queue
func (r *recoveryHandler) Start() error {
	log.Info("start recovery from DB")
	if err := recovery.RecoverActiveJobs(
		context.Background(),
		r.recoveryScope,
		r.activeJobsOps,
		r.jobConfigOps,
		r.jobRuntimeOps,
		r.recoverTasks,
	); err != nil {
		return err
	}

	r.metrics.RecoverySuccess.Inc(1)
	return nil
}

// recoverTasks recovers tasks from DB on bootstrap after leadership change.
// recovery updates host to task map in offerpool and host summary.
func (r *recoveryHandler) recoverTasks(
	ctx context.Context,
	id string,
	jobConfig *job.JobConfig,
	configAddOn *models.ConfigAddOn,
	jobRuntime *job.RuntimeInfo,
	batch recovery.TasksBatch,
	errChan chan<- error,
) {

	jobID := &peloton.JobID{Value: id}
	offerPool := offer.GetEventHandler().GetOfferPool()

	taskInfos, err := r.taskStore.GetTasksForJobByRange(
		ctx,
		jobID,
		&task.InstanceRange{
			From: batch.From,
			To:   batch.To,
		})
	if err != nil {
		log.WithError(err).
			WithField("job_id", id).
			WithField("from", batch.From).
			WithField("to", batch.To).
			Error("failed to fetch task infos")
		if yarpcerrors.IsNotFound(err) {
			// Due to task_config table deprecation, we might see old jobs
			// fail to recover due to their task config was created in
			// task_config table instead of task_config_v2. Only log the
			// error here instead of crashing jobmgr.
			return
		}
		errChan <- err
		return
	}

	for _, taskInfo := range taskInfos {
		runtime := taskInfo.GetRuntime()
		offerPool.UpdateTasksOnHost(
			runtime.GetMesosTaskId().GetValue(),
			runtime.GetState(),
			taskInfo)
		r.hostCache.RecoverPodInfoOnHost(
			util.CreatePodIDFromMesosTaskID(runtime.GetMesosTaskId()),
			runtime.GetHost(),
			api.ConvertTaskStateToPodState(runtime.GetState()),
			api.ConvertTaskConfigToPodSpec(taskInfo.Config, id, taskInfo.InstanceId),
		)
	}

	return
}
