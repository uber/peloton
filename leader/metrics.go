package leader

import (
	"github.com/uber-go/tally"
)

type electionMetrics struct {
	Start            tally.Counter
	Stop             tally.Counter
	Resigned         tally.Counter
	LostLeadership   tally.Counter
	GainedLeadership tally.Counter
	IsLeader         tally.Gauge
	Running          tally.Gauge
	Error            tally.Counter
}

type observerMetrics struct {
	Start         tally.Counter
	Stop          tally.Counter
	LeaderChanged tally.Counter
	Running       tally.Gauge
	Error         tally.Counter
}

//TODO(rcharles) replace tag hostname with instanceNumber
func newElectionMetrics(scope tally.Scope, hostname string) electionMetrics {
	s := scope.Tagged(map[string]string{"hostname": hostname})

	return electionMetrics{
		Start:            s.Counter("start"),
		Stop:             s.Counter("stop"),
		Resigned:         s.Counter("resigned"),
		LostLeadership:   s.Counter("lost_leadership"),
		GainedLeadership: s.Counter("gained_leadership"),
		IsLeader:         s.Gauge("is_leader"),
		Running:          s.Gauge("running"),
		Error:            s.Counter("error"),
	}
}

func newObserverMetrics(scope tally.Scope, role string) observerMetrics {
	s := scope.Tagged(map[string]string{"role": role})

	return observerMetrics{
		Start:         s.Counter("start"),
		Stop:          s.Counter("stop"),
		LeaderChanged: s.Counter("leader_changed"),
		Running:       s.Gauge("running"),
		Error:         s.Counter("error"),
	}
}
