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

package task

import (
	"context"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/resmgr/scalar"
	"github.com/uber/peloton/pkg/storage"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

var (
	// represents the terminal task states
	terminalTaskStates = map[task.TaskState]bool{
		task.TaskState_FAILED:    true,
		task.TaskState_KILLED:    true,
		task.TaskState_SUCCEEDED: true,
	}
)

// activeTasksTracker gets the active tasks in the scheduler
type activeTasksTracker interface {
	// GetActiveTasks returns task states map
	GetActiveTasks(
		jobID string,
		respoolID string,
		states []string) map[string][]*RMTask
}

// applier applies the operation to the tasks which need reconciliation
type applier interface {
	apply(map[string]string) error
}

// right now the resource manager only logs the tasks which are found to be
// in an inconsistent state with the store.
// TODO do something useful with this
type logApplier struct{}

func (l *logApplier) apply(tasks map[string]string) error {
	log.WithField("tasks_to_reconcile", tasks).
		WithField("num_tasks", len(tasks)).
		Infof("Completed task reconciliation")
	return nil
}

// Reconciler reconciles the tasks between the tracker and the store
type Reconciler struct {
	lifeCycle            lifecycle.LifeCycle
	taskStore            storage.TaskStore
	tracker              activeTasksTracker
	applier              applier
	reconciliationPeriod time.Duration
	metrics              *Metrics
}

// NewReconciler returns a new reconciler
func NewReconciler(
	tracker activeTasksTracker,
	taskStore storage.TaskStore,
	parent tally.Scope,
	reconciliationPeriod time.Duration,
) *Reconciler {
	return &Reconciler{
		tracker:              tracker,
		taskStore:            taskStore,
		reconciliationPeriod: reconciliationPeriod,
		metrics:              NewMetrics(parent.SubScope("instance")),
		lifeCycle:            lifecycle.NewLifeCycle(),
		applier:              new(logApplier),
	}
}

// Start starts the reconciler
func (t *Reconciler) Start() error {
	if !t.lifeCycle.Start() {
		log.Warn(
			"Task Reconciler is already running, no action will be performed")
		return nil
	}

	go func() {
		defer t.lifeCycle.StopComplete()

		ticker := time.NewTicker(t.reconciliationPeriod)
		defer ticker.Stop()

		log.Info("Starting Task Reconciler")
		for {
			select {
			case <-t.lifeCycle.StopCh():
				log.Info("Exiting Task Reconciler")
				return
			case <-ticker.C:
			}

			if err := t.run(); err != nil {
				t.metrics.ReconciliationFail.Inc(1)
				log.WithError(err).Error("failed task reconciliation")
				continue
			}
			t.metrics.ReconciliationSuccess.Inc(1)
		}
	}()
	return nil
}

func (t *Reconciler) run() error {
	tasks, err := t.getTasksToReconcile(
		// get all active tasks in the scheduler
		t.tracker.GetActiveTasks(
			"",
			"",
			nil,
		),
	)
	if err != nil {
		return err
	}

	return t.applier.apply(tasks)
}

// Stop stops the reconciler
func (t *Reconciler) Stop() error {
	if !t.lifeCycle.Stop() {
		log.Warn("Task Reconciler is already stopped, no" +
			" action will be performed")
		return nil
	}

	// Wait for task instance to be stopped
	t.lifeCycle.Wait()
	log.Info("Task Reconciler Stopped")
	return nil
}

// goes through the active tasks to return a map of peloton_task_id
// ->mesos_task_id of tasks which have different states between the scheduler
// and the store.
func (t *Reconciler) getTasksToReconcile(activeTasks map[string][]*RMTask) (
	map[string]string, error) {
	log.Info("performing task reconciliation")
	errs := new(multierror.Error)

	leakedResource := scalar.ZeroResource

	// map of pelotonTaskID->MesosTaskID
	tasksToReconcile := make(map[string]string)
	for trackerState, pelotonTasks := range activeTasks {
		for _, pelotonTask := range pelotonTasks {
			pelotonTaskID := pelotonTask.Task().GetId().GetValue()

			taskInfo, err := t.taskStore.GetTaskByID(
				context.Background(),
				pelotonTaskID,
			)
			if err != nil {
				errs = multierror.Append(
					errs,
					errors.Errorf(
						"unable to get task:%s from the database",
						pelotonTaskID))
				continue
			}

			actualState := taskInfo.GetRuntime().GetState()

			if !terminalTaskStates[actualState] {
				continue
			}

			if pelotonTask.Task().GetTaskId().GetValue() ==
				taskInfo.Runtime.GetMesosTaskId().GetValue() &&
				actualState.String() != trackerState {

				// The task state in the database is terminal but in the
				// tracker it is not. We an inconsistent task!
				tasksToReconcile[pelotonTaskID] =
					pelotonTask.Task().GetTaskId().GetValue()

				// update leaked resources
				leakedResource.Add(scalar.ConvertToResmgrResource(
					pelotonTask.task.GetResource(),
				))
			}
		}
	}

	t.metrics.LeakedResources.Update(leakedResource)
	return tasksToReconcile, errs.ErrorOrNil()
}
