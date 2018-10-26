package task

import (
	"github.com/uber-go/tally"
)

// Metrics tracks various metrics at task state level.
type Metrics struct {
	taskUpdateCounter  tally.Counter
	taskUpdateAck      tally.Counter
	taskAckChannelSize tally.Gauge

	scope tally.Scope
}

// NewMetrics returns a new Metrics struct, with all metrics initialized
// and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		taskUpdateCounter:  scope.Counter("task_updates"),
		taskUpdateAck:      scope.Counter("task_update_ack"),
		taskAckChannelSize: scope.Gauge("task_ack_channel_size"),

		scope: scope,
	}
}
