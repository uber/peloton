package task

import (
	"context"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/common/lifecycle"
	"code.uber.internal/infra/peloton/resmgr/scalar"
	"code.uber.internal/infra/peloton/storage"

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
	// singleton
	instance *reconciler
	once     sync.Once
)

// Reconciler is the interface for reconciling tasks between
// the database and the task tracker
type Reconciler interface {
	Start() error
	Stop() error
}

// reconciler implements Reconciler
// It goes through all the tasks in tracker and compares them with the state in the database
type reconciler struct {
	taskStore            storage.TaskStore
	tracker              Tracker
	reconciliationPeriod time.Duration
	metrics              *Metrics
	lifeCycle            lifecycle.LifeCycle // lifecycle manager
}

// InitReconciler initializes the instance
func InitReconciler(tracker Tracker, taskStore storage.TaskStore,
	parent tally.Scope, reconciliationPeriod time.Duration,
) {
	once.Do(func() {
		instance = &reconciler{
			tracker:              tracker,
			taskStore:            taskStore,
			reconciliationPeriod: reconciliationPeriod,
			metrics:              NewMetrics(parent.SubScope("instance")),
			lifeCycle:            lifecycle.NewLifeCycle(),
		}
	})
}

// GetReconciler returns the instance instance
func GetReconciler() Reconciler {
	if instance == nil {
		log.Error("Task Reconciler is not initialized")
	}
	return instance
}

func (t *reconciler) Start() error {
	if !t.lifeCycle.Start() {
		log.Warn("Task Reconciler is already running, no action will be " +
			"performed")
		return nil
	}

	started := make(chan int, 1)
	go func() {
		defer t.lifeCycle.StopComplete()

		ticker := time.NewTicker(t.reconciliationPeriod)
		defer ticker.Stop()

		log.Infof("Starting Task Reconciler")
		close(started)

		for {
			select {
			case <-t.lifeCycle.StopCh():
				log.Info("Exiting Task Reconciler")
				return
			case <-ticker.C:
				tasksToReconcile, err := t.getTasksToReconcile()
				if err != nil {
					t.metrics.ReconciliationFail.Inc(1)
					log.WithError(err).Error("failed task reconciliation")
				} else {
					t.metrics.ReconciliationSuccess.Inc(1)
					log.WithField("tasks_to_reconcile", tasksToReconcile).
						WithField("number_tasks", len(tasksToReconcile)).
						Infof("completed task reconciliation")
				}
			}
		}
	}()
	<-started
	return nil
}

func (t *reconciler) Stop() error {
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

// getTasksToReconcile goes through all the tasks in the tracker and compares
// the tracker's view of the task vs whats in the database. A task in terminal
// state in the database but active in the tracker is considered as a leak
func (t *reconciler) getTasksToReconcile() (map[string]string, error) {
	log.Infof("performing task reconciliation")
	errs := new(multierror.Error)

	// map of pelotonTaskID->MesosTaskID
	tasksToReconcile := make(map[string]string)

	activeTasks := t.tracker.GetActiveTasks("", "", nil)
	for rmtrackerState, pelotonTasks := range activeTasks {
		for _, pelotonTask := range pelotonTasks {
			pelotonTaskID := pelotonTask.task.Id.Value
			taskInfo, err := t.taskStore.GetTaskByID(context.Background(), pelotonTaskID)

			if err != nil {
				errs = multierror.Append(errs, errors.Errorf("unable to get task:%s from the "+
					"database", pelotonTaskID))
				continue
			}
			actualState := taskInfo.GetRuntime().GetState()
			if _, ok := terminalTaskStates[actualState]; ok {
				rmTask := t.tracker.GetTask(&peloton.TaskID{
					Value: pelotonTaskID,
				})
				if rmTask == nil {
					errs = multierror.Append(errs, errors.Errorf("unable to get rm task:%s "+
						"from tracker", pelotonTaskID))
				} else if rmTask.Task().GetTaskId().GetValue() ==
					taskInfo.Runtime.GetMesosTaskId().GetValue() &&
					actualState.String() != rmtrackerState {
					// The task state in the database is terminal but in the tracker it is not
					// we have a leak!
					tasksToReconcile[pelotonTaskID] = rmTask.Task().GetTaskId().GetValue()

					// update metrics
					leakedResources := scalar.ConvertToResmgrResource(
						rmTask.task.GetResource())
					t.metrics.LeakedResources.Update(leakedResources)
				}
			}
		}
	}
	return tasksToReconcile, errs.ErrorOrNil()
}
