package metrics

import (
	"github.com/uber-go/tally"
)

// Metrics contains all the metrics relevant to the scheduler
type Metrics struct {
	// Running indicates if the scheduler is currently running or not
	Running tally.Gauge

	// TaskLaunchDispatches counts the number of times we call into the task launcher to request tasks to be launched and it succeeds
	// NOTE: one increment can correspond to multiple mesos tasks being launched
	TaskLaunchDispatches tally.Counter
	// TaskLaunchDispatchesFail counts the number of times we call into the task launcher to request tasks to be launched and it fails
	// NOTE: one increment can correspond to multiple mesos tasks being launched
	TaskLaunchDispatchesFail tally.Counter
	TaskQueueDepth           tally.Gauge

	// OfferStarved indicates the number of times the scheduler attempted to get an Offer to request a task launch, but was returned an
	// empty set.
	OfferStarved tally.Counter
	// OfferGet indicates the number of times the scheduler requested an Offer and it was fulfilled successfully
	OfferGet tally.Counter
	// OfferGet indicates the number of times the scheduler requested an Offer and it failed
	OfferGetFail tally.Counter

	// Launcher metrics
	// LaunchTask is the number of mesos tasks launched. This is a many:1 relationship with offers launched into
	LaunchTask tally.Counter
	// LaunchTaskFail is the number of mesos tasks failed to launch. This is a many:1 relationship with offers
	LaunchTaskFail tally.Counter
	// LaunchOfferAccept is the number of mesos offers that were accepted
	LaunchOfferAccept tally.Counter
	// LaunchOfferAcceptFail is the number of mesos offers that failed to be accepted
	LaunchOfferAcceptFail tally.Counter
}

// New returns a new Metrics struct with all metrics initialized and rooted below the given tally scope
func New(scope tally.Scope) *Metrics {
	taskScope := scope.SubScope("task")
	offerScope := scope.SubScope("offer")

	taskSuccessScope := taskScope.Tagged(map[string]string{"type": "success"})
	taskFailScope := taskScope.Tagged(map[string]string{"type": "fail"})
	offerSuccessScope := offerScope.Tagged(map[string]string{"type": "success"})
	offerFailScope := offerScope.Tagged(map[string]string{"type": "fail"})

	m := &Metrics{
		Running:      scope.Gauge("running"),
		OfferStarved: scope.Counter("offer_starved"),

		TaskQueueDepth:           taskScope.Gauge("queue_depth"),
		TaskLaunchDispatches:     taskSuccessScope.Counter("launch_dispatch"),
		TaskLaunchDispatchesFail: taskFailScope.Counter("launch_dispatch"),

		OfferGet:     offerSuccessScope.Counter("get"),
		OfferGetFail: offerFailScope.Counter("get_fail"),

		LaunchTask:            taskSuccessScope.Counter("launch"),
		LaunchTaskFail:        taskFailScope.Counter("launch"),
		LaunchOfferAccept:     offerSuccessScope.Counter("accept"),
		LaunchOfferAcceptFail: offerFailScope.Counter("accept"),
	}
	return m
}
