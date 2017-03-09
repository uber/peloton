package reconciliation

import (
	mesos "mesos/v1"
	sched "mesos/v1/scheduler"

	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
)

// TaskReconciler is the interface to initiate task reconciliation to mesos master.
type TaskReconciler interface {
	// Reconcile all tasks known to mesos master, aka implicitly.
	ReconcileTasksImplicitly() error
}

// taskReconciler implements TaskReconciler.
type taskReconciler struct {
	client  mpb.Client
	metrics *Metrics
}

// NewTaskReconciler returns a new instance of taskReconciler.
func NewTaskReconciler(
	client mpb.Client,
	parent tally.Scope) TaskReconciler {

	return &taskReconciler{
		client:  client,
		metrics: NewMetrics(parent.SubScope("reconciliation")),
	}
}

func (r *taskReconciler) ReconcileTasksImplicitly() error {
	log.Info("Reconcile tasks implicitly called.")
	frameworkID := hostmgr_mesos.GetSchedulerDriver().GetFrameworkID()
	streamID := hostmgr_mesos.GetSchedulerDriver().GetMesosStreamID()
	return r.reconcileTasksImplicitly(frameworkID, streamID)
}

func (r *taskReconciler) reconcileTasksImplicitly(
	frameworkID *mesos.FrameworkID,
	streamID string) error {

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
