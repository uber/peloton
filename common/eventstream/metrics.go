package eventstream

import "github.com/uber-go/tally"

// HandlerMetrics is the metrics for event stream handler
type HandlerMetrics struct {
	Head                 tally.Gauge
	Tail                 tally.Gauge
	Size                 tally.Gauge
	Capacity             tally.Gauge
	UnexpectedClients    tally.Counter
	PurgeEventError      tally.Counter
	InvalidStreamIDError tally.Counter
	AddEvent             tally.Counter
	AddEventSuccess      tally.Counter
	AddEventFail         tally.Counter
	InitStream           tally.Counter
	InitStreamSuccess    tally.Counter
	InitStreamFail       tally.Counter
	WaitForEvents        tally.Counter
	WaitForEventsSuccess tally.Counter
	WaitForEventsFailed  tally.Counter
}

// NewHandlerMetrics creates a HandlerMetrics
func NewHandlerMetrics(scope tally.Scope) *HandlerMetrics {
	return &HandlerMetrics{
		Head:                 scope.Gauge("head"),
		Tail:                 scope.Gauge("tail"),
		Size:                 scope.Gauge("size"),
		Capacity:             scope.Gauge("capacity"),
		UnexpectedClients:    scope.Counter("unexpected_client_error"),
		PurgeEventError:      scope.Counter("purge_event_error"),
		InvalidStreamIDError: scope.Counter("invalid_stream_id_error"),
		AddEvent:             scope.Counter("add_event"),
		AddEventSuccess:      scope.Counter("add_event_success"),
		AddEventFail:         scope.Counter("add_event_fail"),
		InitStream:           scope.Counter("init_stream"),
		InitStreamSuccess:    scope.Counter("init_stream_success"),
		InitStreamFail:       scope.Counter("init_stream_fail"),
		WaitForEvents:        scope.Counter("wait_for_events"),
		WaitForEventsSuccess: scope.Counter("wait_for_events_success"),
		WaitForEventsFailed:  scope.Counter("wait_for_events_failed"),
	}
}

// ClientMetrics is the metrics for event stream client
type ClientMetrics struct {
	EventsConsumed       tally.Counter
	InitStream           tally.Counter
	InitStreamSuccess    tally.Counter
	InitStreamFail       tally.Counter
	WaitForEvents        tally.Counter
	WaitForEventsSuccess tally.Counter
	WaitForEventsFailed  tally.Counter
	StreamIDChange       tally.Counter
	PurgeOffset          tally.Gauge
}

// NewClientMetrics creates a new ClientMetrics
func NewClientMetrics(scope tally.Scope) *ClientMetrics {
	return &ClientMetrics{
		EventsConsumed:       scope.Counter("events_consumed"),
		InitStream:           scope.Counter("init_stream"),
		InitStreamSuccess:    scope.Counter("init_stream_success"),
		InitStreamFail:       scope.Counter("init_stream_fail"),
		WaitForEvents:        scope.Counter("wait_for_events"),
		WaitForEventsSuccess: scope.Counter("wait_for_events_success"),
		WaitForEventsFailed:  scope.Counter("wait_for_events_failed"),
		StreamIDChange:       scope.Counter("stream_id_change"),
		PurgeOffset:          scope.Gauge("purge_offset"),
	}
}
