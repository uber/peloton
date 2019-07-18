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

package reconcile

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"

	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/common/util"
	hostmgr_mesos "github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/storage"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"
)

// TaskReconciler is the interface to initiate task reconciliation to mesos master.
type TaskReconciler interface {
	Reconcile(running *atomic.Bool)
	SetExplicitReconcileTurn(flag bool)
}

// taskReconciler implements TaskReconciler.
type taskReconciler struct {
	metrics *Metrics

	schedulerClient       mpb.SchedulerClient
	taskStore             storage.TaskStore
	activeJobsOps         ormobjects.ActiveJobsOps
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider

	explicitReconcileBatchInterval time.Duration
	explicitReconcileBatchSize     int

	isExplicitReconcileRunning atomic.Bool
	// Run explicit reconcile if True, otherwise run implicit reconcile.
	isExplicitReconcileTurn atomic.Bool
}

// NewTaskReconciler initialize the task reconciler.
func NewTaskReconciler(
	client mpb.SchedulerClient,
	parent tally.Scope,
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider,
	activeJobsOps ormobjects.ActiveJobsOps,
	taskStore storage.TaskStore,
	cfg *TaskReconcilerConfig) TaskReconciler {

	reconciler := &taskReconciler{
		schedulerClient:       client,
		activeJobsOps:         activeJobsOps,
		taskStore:             taskStore,
		metrics:               NewMetrics(parent.SubScope("reconcile")),
		frameworkInfoProvider: frameworkInfoProvider,
		explicitReconcileBatchInterval: time.Duration(
			cfg.ExplicitReconcileBatchIntervalSec) * time.Second,
		explicitReconcileBatchSize: cfg.ExplicitReconcileBatchSize,
	}
	reconciler.isExplicitReconcileTurn.Store(true)
	return reconciler
}

// Reconcile kicks off actual explicit or implicit reconcile run.
func (r *taskReconciler) Reconcile(running *atomic.Bool) {
	// Explicit and implicit reconcile might overlap if we have more than 360K
	// tasks to reconcile.
	ctx := context.Background()
	if r.isExplicitReconcileTurn.Toggle() {
		go r.reconcileExplicitly(ctx, running)
	} else {
		go r.reconcileImplicitly(ctx)
	}
}

// SetExplicitReconcileTurn set value for explicit reconciliation turn.
func (r *taskReconciler) SetExplicitReconcileTurn(flag bool) {
	r.isExplicitReconcileTurn.Store(flag)
}

func (r *taskReconciler) reconcileImplicitly(ctx context.Context) {
	log.Info("Reconcile tasks implicitly called.")

	frameworkID := r.frameworkInfoProvider.GetFrameworkID(ctx)
	streamID := r.frameworkInfoProvider.GetMesosStreamID(ctx)

	callType := sched.Call_RECONCILE

	msg := &sched.Call{
		FrameworkId: frameworkID,
		Type:        &callType,
		Reconcile:   &sched.Call_Reconcile{},
	}

	err := r.schedulerClient.Call(streamID, msg)
	if err != nil {
		r.metrics.ReconcileImplicitlyFail.Inc(1)
		log.WithField("error", err).
			Error("Reconcile tasks implicitly mesos call failed")
		return
	}
	r.metrics.ReconcileImplicitly.Inc(1)
	log.Info("Reconcile tasks implicitly returned.")
}

func (r *taskReconciler) reconcileExplicitly(
	ctx context.Context,
	running *atomic.Bool) {
	log.Info("Reconcile tasks explicitly called.")
	if r.isExplicitReconcileRunning.Swap(true) {
		log.Warn("Reconcile tasks explicit is already running, no-op.")
		return
	}
	defer r.isExplicitReconcileRunning.Store(false)

	reconcileTasks, err := r.getReconcileTasks(ctx)
	if err != nil {
		log.Error("Explicit reconcile failed due to tasks query error.")
		r.metrics.ReconcileExplicitlyFail.Inc(1)
		return
	}

	reconcileTasksLen := len(reconcileTasks)
	log.WithField("reconcile_tasks_total", reconcileTasksLen).
		Info("Total number of tasks to reconcile explicitly.")

	frameworkID := r.frameworkInfoProvider.GetFrameworkID(ctx)
	streamID := r.frameworkInfoProvider.GetMesosStreamID(ctx)
	callType := sched.Call_RECONCILE
	explicitTasksPerRun := 0
	for i := 0; i < reconcileTasksLen; i += r.explicitReconcileBatchSize {
		if !running.Load() {
			r.metrics.ExplicitTasksPerRun.Update(float64(explicitTasksPerRun))
			r.metrics.ReconcileExplicitlyAbort.Inc(1)
			log.WithField("offset", i).
				Info("Abort explicit reconcile due to task reconciler stopped.")
			return
		}

		var currBatch []*sched.Call_Reconcile_Task
		if i+r.explicitReconcileBatchSize >= reconcileTasksLen {
			currBatch = reconcileTasks[i:reconcileTasksLen]
		} else {
			currBatch = reconcileTasks[i : i+r.explicitReconcileBatchSize]
		}
		explicitTasksPerRun += len(currBatch)
		msg := &sched.Call{
			FrameworkId: frameworkID,
			Type:        &callType,
			Reconcile: &sched.Call_Reconcile{
				Tasks: currBatch,
			},
		}
		err = r.schedulerClient.Call(streamID, msg)
		if err != nil {
			r.metrics.ExplicitTasksPerRun.Update(float64(explicitTasksPerRun))
			r.metrics.ReconcileExplicitlyFail.Inc(1)
			log.WithField("error", err).
				Error("Abort explicit reconcile due to mesos CALL failed.")
			return
		}
		time.Sleep(r.explicitReconcileBatchInterval)
	}

	r.metrics.ExplicitTasksPerRun.Update(float64(explicitTasksPerRun))
	r.metrics.ReconcileExplicitly.Inc(1)
	log.Info("Reconcile tasks explicitly returned.")
}

// getReconcileTasks queries datastore and get
// all the non-terminal tasks in Mesos.
func (r *taskReconciler) getReconcileTasks(ctx context.Context) (
	[]*sched.Call_Reconcile_Task, error) {

	var reconcileTasks []*sched.Call_Reconcile_Task
	activeJobIDs, err := r.activeJobsOps.GetAll(ctx)
	if err != nil {
		log.WithError(err).Error("Failed to get active jobs.")
		return reconcileTasks, err
	}
	jobIDs := util.GetDereferencedJobIDsList(activeJobIDs)

	log.WithField("job_ids", jobIDs).Info("explicit reconcile job ids.")

	for _, jobID := range jobIDs {
		// Mesos TaskState: TASK_STAGING -> Peloton: TaskState_LAUNCHED
		nonTerminalTasks, getTasksErr := r.taskStore.GetTasksForJobAndStates(
			ctx,
			&jobID,
			[]task.TaskState{
				task.TaskState_LAUNCHED,
				task.TaskState_STARTING,
				task.TaskState_RUNNING,
				task.TaskState_KILLING,
			},
		)
		if getTasksErr != nil {
			log.WithError(getTasksErr).WithFields(log.Fields{
				"job": jobID,
			}).Error("Failed to get running tasks for job")
			r.metrics.ReconcileGetTasksFail.Inc(1)
			continue
		}
		if len(nonTerminalTasks) != 0 {
			for _, taskInfo := range nonTerminalTasks {
				reconcileTasks = append(
					reconcileTasks,
					&sched.Call_Reconcile_Task{
						TaskId:  taskInfo.GetRuntime().GetMesosTaskId(),
						AgentId: taskInfo.GetRuntime().GetAgentID(),
					},
				)
			}
		}
	}
	return reconcileTasks, nil
}
