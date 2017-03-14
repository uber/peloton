package reconcile

import (
	"errors"
	"time"

	mesos "mesos/v1"
	sched "mesos/v1/scheduler"

	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
)

// TaskReconciler is the interface to initiate task reconciliation to mesos master.
type TaskReconciler interface {
	Start()
	Stop()
	Reconcile([]*mesos.TaskID) error
}

// taskReconciler implements TaskReconciler.
type taskReconciler struct {
	client                    mpb.Client
	metrics                   *Metrics
	frameworkInfoProvider     hostmgr_mesos.FrameworkInfoProvider
	initialReconcileDelay     time.Duration
	implicitReconcileInterval time.Duration
	stopChan                  chan struct{}
}

// Singleton task reconciler in hostmgr.
var reconciler *taskReconciler

// InitTaskReconciler initialize the task reconciler.
func InitTaskReconciler(
	client mpb.Client,
	parent tally.Scope,
	frameworkInfoProvider hostmgr_mesos.FrameworkInfoProvider,
	initialReconcileDelay time.Duration,
	implicitReconcileInterval time.Duration) {

	if reconciler != nil {
		log.Warning("Task reconciler has already been initialized")
		return
	}
	reconciler = &taskReconciler{
		client:                    client,
		metrics:                   NewMetrics(parent.SubScope("reconcile")),
		frameworkInfoProvider:     frameworkInfoProvider,
		initialReconcileDelay:     initialReconcileDelay,
		implicitReconcileInterval: implicitReconcileInterval,
		stopChan:                  make(chan struct{}, 1),
	}
}

// GetTaskReconciler returns the singleton task reconciler.
func GetTaskReconciler() TaskReconciler {
	if reconciler == nil {
		log.Fatal("Task reconciler is not initialized")
	}
	return reconciler
}

// Start initiates implicit task reconciliation.
func (r *taskReconciler) Start() {
	log.Info("Reconcile tasks start called.")
	// TODO: add stats for # of reconciliation updates.
	go func() {
		time.Sleep(r.initialReconcileDelay)
		err := r.reconcileImplicitly()
		if err != nil {
			log.WithField("error", err).
				Error("Initial implicit task reconciliation failed")
		}

		ticker := time.NewTicker(r.implicitReconcileInterval)
		defer ticker.Stop()
		for {
			select {
			case <-r.stopChan:
				log.Info("Stopping periodical implicit task reconciliation.")
				return
			case t := <-ticker.C:
				log.WithField("tick", t).
					Info("Start periodic implicit task reconciliation")
				err = r.reconcileImplicitly()
				if err != nil {
					log.WithField("error", err).
						Error("Periodically implicit task reconciliation failed")
				}
			}
		}
	}()
}

// Stop stops implicit task reconciliation.
func (r *taskReconciler) Stop() {
	log.Info("Stop implicitly reconcile tasks called.")
	r.stopChan <- struct{}{}
}

func (r *taskReconciler) reconcileImplicitly() error {
	frameworkID := r.frameworkInfoProvider.GetFrameworkID()
	streamID := r.frameworkInfoProvider.GetMesosStreamID()

	callType := sched.Call_RECONCILE

	msg := &sched.Call{
		FrameworkId: frameworkID,
		Type:        &callType,
		Reconcile:   &sched.Call_Reconcile{},
	}

	err := r.client.Call(streamID, msg)
	if err != nil {
		r.metrics.ReconcileImplicitlyFail.Inc(1)
		log.WithField("error", err).Error("Implicit task reconciliation CALL failed")
		return err
	}
	r.metrics.ReconcileImplicitly.Inc(1)
	log.Info("Reconcile tasks implicitly returned.")
	return nil
}

// Reconcile initiates explicit task reconciliation to Mesos master.
func (r *taskReconciler) Reconcile([]*mesos.TaskID) error {
	return errors.New("Not implemented")
}
