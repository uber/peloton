package offerpool

import (
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/common/scalar"
)

// Metrics tracks various metrics at offer pool level.
type Metrics struct {
	ready   scalar.GaugeMaps
	placing scalar.GaugeMaps

	refreshTimer tally.Timer

	// metrics for handler
	OfferEvents               tally.Counter
	RescindEvents             tally.Counter
	InverseOfferEvents        tally.Counter
	RescindInverseOfferEvents tally.Counter

	// metrics for pool
	Decline     tally.Counter
	DeclineFail tally.Counter

	// metrics for pruner
	Pruned      tally.Counter
	PrunerValid tally.Gauge

	// metrics for unreserving offers
	UnreserveOffer           tally.Counter
	UnreserveOfferFail       tally.Counter
	CleanReservationResource tally.Counter

	// metrics for total number of available hosts in the cluster.
	AvailableHosts tally.Gauge
}

// NewMetrics returns a new Metrics struct, with all metrics initialized
// and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	poolScope := scope.SubScope("pool")
	hostScope := scope.SubScope("hosts")
	readyScope := poolScope.SubScope("ready")
	placingScope := poolScope.SubScope("placing")
	poolFailScope := poolScope.SubScope("fail")
	poolCallScope := poolScope.SubScope("call")

	handlerScope := scope.SubScope("handler")
	offerEventScope := handlerScope.Tagged(map[string]string{"result": "offer"})
	inverseEventScope := handlerScope.Tagged(map[string]string{"result": "inverse"})

	prunerScope := handlerScope.SubScope("pruner")

	return &Metrics{
		ready:   scalar.NewGaugeMaps(readyScope),
		placing: scalar.NewGaugeMaps(placingScope),

		refreshTimer: poolScope.Timer("refresh"),

		OfferEvents:   offerEventScope.Counter("offer"),
		RescindEvents: offerEventScope.Counter("rescind"),

		InverseOfferEvents:        inverseEventScope.Counter("offer"),
		RescindInverseOfferEvents: inverseEventScope.Counter("rescind"),

		Decline:     poolCallScope.Counter("decline"),
		DeclineFail: poolFailScope.Counter("decline"),

		Pruned:      prunerScope.Counter("pruned"),
		PrunerValid: prunerScope.Gauge("valid"),

		UnreserveOffer:           poolCallScope.Counter("unreserve"),
		UnreserveOfferFail:       poolFailScope.Counter("unreserve"),
		CleanReservationResource: poolCallScope.Counter("clean"),

		AvailableHosts: hostScope.Gauge("available_total"),
	}
}
