package reconcile

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"

	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
)

// TaskReconciler is the interface to initiate task reconciliation to mesos master.
type TaskReconciler interface {
	Reconcile(running *atomic.Bool)
}

// taskReconciler implements TaskReconciler.
type taskReconciler struct {
	metrics *Metrics

	schedulerClient       mpb.SchedulerClient
	jobStore              storage.JobStore
	taskStore             storage.TaskStore
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
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	cfg *TaskReconcilerConfig) TaskReconciler {

	reconciler := &taskReconciler{
		schedulerClient:       client,
		jobStore:              jobStore,
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
	log.Debug("Reconcile tasks implicitly returned.")
}

func (r *taskReconciler) reconcileExplicitly(ctx context.Context, running *atomic.Bool) {
	log.Info("Reconcile tasks explicitly called.")
	if r.isExplicitReconcileRunning.Swap(true) {
		log.Info("Reconcile tasks explicit is already running, no-op.")
		return
	}
	defer r.isExplicitReconcileRunning.Store(false)

	reconcileTasks, err := r.getReconcileTasks(ctx)
	reconcileTasksLen := len(reconcileTasks)
	if err != nil {
		log.Error("Explicit reconcile failed due to tasks query error.")
		r.metrics.ReconcileExplicitlyFail.Inc(1)
		return
	}
	log.WithField("total_reconcile_tasks", reconcileTasksLen).
		Debug("Total number of tasks to reconcile explicitly.")

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
	log.Debug("Reconcile tasks explicitly returned.")
}

// getReconcileTasks queries datastore and get all the RUNNING tasks.
// TODO(mu): Revisit all the non-terminal state in Peloton.
func (r *taskReconciler) getReconcileTasks(ctx context.Context) (
	[]*sched.Call_Reconcile_Task, error) {

	var reconcileTasks []*sched.Call_Reconcile_Task
	jobStates := []job.JobState{
		job.JobState_RUNNING,
	}
	jobIDs, err := r.jobStore.GetJobsByStates(ctx, jobStates)
	if err != nil {
		log.WithError(err).Error("Failed to get running jobs.")
		return reconcileTasks, err
	}

	for _, jobID := range jobIDs {
		nonTerminalTasks, getTasksErr := r.taskStore.GetTasksForJobAndState(
			ctx,
			&jobID,
			task.TaskState_RUNNING.String(),
		)
		if getTasksErr != nil {
			log.WithError(getTasksErr).WithFields(log.Fields{
				"job": jobID,
			}).Error("Failed to get running tasks for job")
			return reconcileTasks, getTasksErr
		}
		// TODO(mu): Consider storing agent_id in tasks table for faster
		// task reconciliation.
		if len(nonTerminalTasks) != 0 {
			for _, taskInfo := range nonTerminalTasks {
				reconcileTasks = append(
					reconcileTasks,
					&sched.Call_Reconcile_Task{
						TaskId: taskInfo.GetRuntime().GetMesosTaskId(),
					},
				)
			}
		}
	}
	return reconcileTasks, nil
}
