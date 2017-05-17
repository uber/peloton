package reconcile

import (
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"

	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
)

// TaskReconciler is the interface to initiate task reconciliation to mesos master.
type TaskReconciler interface {
	Start()
	Stop()
}

// taskReconciler implements TaskReconciler.
type taskReconciler struct {
	sync.Mutex

	Running  atomic.Bool
	stopChan chan struct{}
	metrics  *Metrics

	schedulerClient       mpb.SchedulerClient
	jobStore              storage.JobStore
	taskStore             storage.TaskStore
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider

	initialReconcileDelay          time.Duration
	reconcileInterval              time.Duration
	explicitReconcileBatchInterval time.Duration
	explicitReconcileBatchSize     int

	isExplicitReconcileRunning atomic.Bool
	// Run explicit reconcile if True, otherwise run implicit reconcile.
	isExplicitReconcileTurn atomic.Bool
}

/*
	reconcilerConfig := &reconcile.TaskReconcilerConfig{
		InitialReconcileDelay: ,
		ReconcileInterval: ,
		ExplicitReconcileBatchInterval: ,
		ExplicitReconcileBatchSize: ,
	}

*/

// Singleton task reconciler in hostmgr.
var reconciler *taskReconciler

// InitTaskReconciler initialize the task reconciler.
func InitTaskReconciler(
	client mpb.SchedulerClient,
	parent tally.Scope,
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	cfg *TaskReconcilerConfig) {

	if reconciler != nil {
		log.Warn("Task reconciler has already been initialized")
		return
	}

	reconciler = &taskReconciler{
		schedulerClient:       client,
		jobStore:              jobStore,
		taskStore:             taskStore,
		metrics:               NewMetrics(parent.SubScope("reconcile")),
		frameworkInfoProvider: frameworkInfoProvider,
		initialReconcileDelay: time.Duration(
			cfg.InitialReconcileDelaySec) * time.Second,
		reconcileInterval: time.Duration(
			cfg.ReconcileIntervalSec) * time.Second,
		explicitReconcileBatchInterval: time.Duration(
			cfg.ExplicitReconcileBatchIntervalSec) * time.Second,
		explicitReconcileBatchSize: cfg.ExplicitReconcileBatchSize,
		stopChan:                   make(chan struct{}, 1),
	}
	reconciler.isExplicitReconcileTurn.Store(true)
}

// GetTaskReconciler returns the singleton task reconciler.
func GetTaskReconciler() TaskReconciler {
	if reconciler == nil {
		log.Fatal("Task reconciler is not initialized")
	}
	return reconciler
}

// Start initiates explicit task reconciliation.
func (r *taskReconciler) Start() {
	log.Info("Task reconciler start called.")
	r.Lock()
	defer r.Unlock()
	if r.Running.Swap(true) {
		log.Info("Task reconciler is already running, no-op.")
		return
	}

	// TODO: add stats for # of reconciliation updates.
	go func() {
		defer r.Running.Store(false)

		initialTimer := time.NewTimer(r.initialReconcileDelay)
		select {
		case <-r.stopChan:
			log.Info("Periodic reconcile stopped before first run.")
			return
		case <-initialTimer.C:
			log.Debug("Initial delay passed")
		}

		r.reconcile()

		ticker := time.NewTicker(r.reconcileInterval)
		defer ticker.Stop()
		for {
			select {
			case <-r.stopChan:
				log.Info("periodical task reconciliation stopped.")
				return
			case t := <-ticker.C:
				log.WithField("tick", t).
					Info("periodical task reconciliation triggered.")
				r.reconcile()
			}
		}
	}()
}

// reconcile kicks off actual explicit or implicit reconcile run.
func (r *taskReconciler) reconcile() {
	// Explicit and implicit reconcile might overlap if we have more than 360K
	// tasks to reconcile.
	if r.isExplicitReconcileTurn.Toggle() {
		go r.reconcileExplicitly()
	} else {
		go r.reconcileImplicitly()
	}
}

// Stop stops explicit task reconciliation.
func (r *taskReconciler) Stop() {
	log.Info("Task reconciler stop called.")

	if !r.Running.Load() {
		log.Warn("Task reconciler is not running, no-op.")
		return
	}

	r.Lock()
	defer r.Unlock()

	log.Info("Stopping task reconciler.")
	r.stopChan <- struct{}{}

	for r.Running.Load() {
		time.Sleep(1 * time.Millisecond)
	}
	log.Info("Task reconciler stop returned.")
}

func (r *taskReconciler) reconcileImplicitly() {
	log.Info("Reconcile tasks implicitly called.")

	frameworkID := r.frameworkInfoProvider.GetFrameworkID()
	streamID := r.frameworkInfoProvider.GetMesosStreamID()

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

func (r *taskReconciler) reconcileExplicitly() {
	log.Info("Reconcile tasks explicitly called.")
	if r.isExplicitReconcileRunning.Swap(true) {
		log.Info("Reconcile tasks explicit is already running, no-op.")
		return
	}
	defer r.isExplicitReconcileRunning.Store(false)

	reconcileTasks, err := r.getReconcileTasks()
	reconcileTasksLen := len(reconcileTasks)
	if err != nil {
		log.Error("Explicit reconcile failed due to tasks query error.")
		r.metrics.ReconcileExplicitlyFail.Inc(1)
		return
	}
	log.WithField("total_reconcile_tasks", reconcileTasksLen).
		Debug("Total number of tasks to reconcile explicitly.")

	frameworkID := r.frameworkInfoProvider.GetFrameworkID()
	streamID := r.frameworkInfoProvider.GetMesosStreamID()
	callType := sched.Call_RECONCILE
	explicitTasksPerRun := 0
	for i := 0; i < reconcileTasksLen; i += r.explicitReconcileBatchSize {
		if !r.Running.Load() {
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
func (r *taskReconciler) getReconcileTasks() (
	[]*sched.Call_Reconcile_Task, error) {

	var reconcileTasks []*sched.Call_Reconcile_Task
	allJobs, err := r.jobStore.GetAllJobs()
	if err != nil {
		log.WithField("error", err).Error("Failed to get all jobs.")
		return reconcileTasks, err
	}

	// TODO(mu): we should query tasks by non-terminal state directly on tasks
	// table with pagination to avoid repetitive queries and storing all tasks
	// in memory.
	for jobID := range allJobs {
		nonTerminalTasks, getTasksErr := r.taskStore.GetTasksForJobAndState(
			&peloton.JobID{Value: jobID},
			strconv.Itoa(int(task.TaskState_value["RUNNING"])),
		)
		if getTasksErr != nil {
			log.WithFields(log.Fields{
				"job":   jobID,
				"error": err,
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
						TaskId: taskInfo.GetRuntime().TaskId,
					},
				)
			}
		}
	}
	return reconcileTasks, nil
}
