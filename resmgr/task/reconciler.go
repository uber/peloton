package task

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

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
	sync.Mutex
	runningState         int32
	stopChan             chan struct{}
	taskStore            storage.TaskStore
	tracker              Tracker
	reconciliationPeriod time.Duration
	metrics              *Metrics
}

// InitReconciler initializes the instance
func InitReconciler(tracker Tracker, taskStore storage.TaskStore,
	parent tally.Scope, reconciliationPeriod time.Duration,
) {
	once.Do(func() {
		instance = &reconciler{
			runningState:         runningStateNotStarted,
			tracker:              tracker,
			taskStore:            taskStore,
			stopChan:             make(chan struct{}, 1),
			reconciliationPeriod: reconciliationPeriod,
			metrics:              NewMetrics(parent.SubScope("instance")),
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
	t.Lock()
	defer t.Unlock()

	if t.runningState == runningStateRunning {
		log.Warn("Task Reconciler is already running, no action will be " +
			"performed")
		return nil
	}

	started := make(chan int, 1)
	go func() {
		defer atomic.StoreInt32(&t.runningState, runningStateNotStarted)
		atomic.StoreInt32(&t.runningState, runningStateRunning)

		ticker := time.NewTicker(t.reconciliationPeriod)
		defer ticker.Stop()

		log.Infof("Starting Task Reconciler")
		started <- 0

		for {
			select {
			case <-t.stopChan:
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
	t.Lock()
	defer t.Unlock()

	if t.runningState == runningStateNotStarted {
		log.Warn("Task Reconciler is already stopped, no" +
			" action will be performed")
		return nil
	}

	log.Info("Stopping Task Reconciler")
	t.stopChan <- struct{}{}

	// Wait for task instance to be stopped
	for {
		runningState := atomic.LoadInt32(&t.runningState)
		if runningState == runningStateRunning {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
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

	activeTasks := t.tracker.GetActiveTasks("", "")
	for pelotonTaskID, rmtrackerState := range activeTasks {
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
	return tasksToReconcile, errs.ErrorOrNil()
}
