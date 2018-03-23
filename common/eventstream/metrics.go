package eventstream

import "github.com/uber-go/tally"

// HandlerMetrics is the metrics for event stream handler
type HandlerMetrics struct {
	Head     tally.Gauge
	Tail     tally.Gauge
	Size     tally.Gauge
	Capacity tally.Gauge

	UnexpectedClientError tally.Counter
	PurgeEventError       tally.Counter
	InvalidStreamIDError  tally.Counter

	AddEventAPI          tally.Counter
	AddEventSuccess      tally.Counter
	AddEventFail         tally.Counter
	InitStreamAPI        tally.Counter
	InitStreamSuccess    tally.Counter
	InitStreamFail       tally.Counter
	WaitForEventsAPI     tally.Counter
	WaitForEventsSuccess tally.Counter
	WaitForEventsFailed  tally.Counter
}

// NewHandlerMetrics creates a HandlerMetrics
func NewHandlerMetrics(scope tally.Scope) *HandlerMetrics {
	handlerAPIScope := scope.SubScope("api")
	handlerSuccessScope := scope.Tagged(map[string]string{"result": "success"})
	handlerFailScope := scope.Tagged(map[string]string{"result": "fail"})
	return &HandlerMetrics{
		Head:                  scope.Gauge("head"),
		Tail:                  scope.Gauge("tail"),
		Size:                  scope.Gauge("size"),
		Capacity:              scope.Gauge("capacity"),
		UnexpectedClientError: scope.Counter("unexpectedClientError"),
		PurgeEventError:       scope.Counter("purgeEventError"),
		InvalidStreamIDError:  scope.Counter("invalidStreamIdError"),
		AddEventAPI:           handlerAPIScope.Counter("addEvent"),
		AddEventSuccess:       handlerSuccessScope.Counter("addEvent"),
		AddEventFail:          handlerFailScope.Counter("addEvent"),
		InitStreamAPI:         handlerAPIScope.Counter("initStream"),
		InitStreamSuccess:     handlerSuccessScope.Counter("initStream"),
		InitStreamFail:        handlerFailScope.Counter("initStream"),
		WaitForEventsAPI:      handlerAPIScope.Counter("waitForEvents"),
		WaitForEventsSuccess:  handlerSuccessScope.Counter("waitForEvents"),
		WaitForEventsFailed:   handlerFailScope.Counter("waitForEvents"),
	}
}

// ClientMetrics is the metrics for event stream client
type ClientMetrics struct {
	EventsConsumed tally.Counter
	StreamIDChange tally.Counter
	PurgeOffset    tally.Gauge

	InitStreamAPI        tally.Counter
	InitStreamSuccess    tally.Counter
	InitStreamFail       tally.Counter
	WaitForEventsAPI     tally.Counter
	WaitForEventsSuccess tally.Counter
	WaitForEventsFailed  tally.Counter
}

// NewClientMetrics creates a new ClientMetrics
func NewClientMetrics(scope tally.Scope) *ClientMetrics {
	clientAPIScope := scope.SubScope("api")
	clientSuccessScope := scope.Tagged(map[string]string{"result": "success"})
	clientFailScope := scope.Tagged(map[string]string{"result": "fail"})
	return &ClientMetrics{
		EventsConsumed:       scope.Counter("eventsConsumed"),
		StreamIDChange:       scope.Counter("streamIdChange"),
		PurgeOffset:          scope.Gauge("purgeOffset"),
		InitStreamAPI:        clientAPIScope.Counter("initStream"),
		InitStreamSuccess:    clientSuccessScope.Counter("initStream"),
		InitStreamFail:       clientFailScope.Counter("initStream"),
		WaitForEventsAPI:     clientAPIScope.Counter("waitForEvents"),
		WaitForEventsSuccess: clientSuccessScope.Counter("waitForEvents"),
		WaitForEventsFailed:  clientFailScope.Counter("waitForEvents"),
	}
}
