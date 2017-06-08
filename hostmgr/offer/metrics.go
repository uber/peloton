package offer

import (
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/common/scalar"
)

// Metrics tracks various metrics at offer pool level.
type Metrics struct {
	ready   scalar.GaugeMaps
	placing scalar.GaugeMaps

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
}

// NewMetrics returns a new Metrics struct, with all metrics initialized
// and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	poolScope := scope.SubScope("pool")
	readyScope := poolScope.SubScope("ready")
	placingScope := poolScope.SubScope("placing")
	poolFailScope := poolScope.SubScope("fail")
	poolCallScope := poolScope.SubScope("call")

	handlerScope := scope.SubScope("handler")
	offerEventScope := handlerScope.Tagged(map[string]string{"type": "offer"})
	inverseEventScope := handlerScope.Tagged(map[string]string{"type": "inverse"})

	prunerScope := handlerScope.SubScope("pruner")

	return &Metrics{
		ready:   scalar.NewGaugeMaps(readyScope),
		placing: scalar.NewGaugeMaps(placingScope),

		OfferEvents:   offerEventScope.Counter("offer"),
		RescindEvents: offerEventScope.Counter("rescind"),

		InverseOfferEvents:        inverseEventScope.Counter("offer"),
		RescindInverseOfferEvents: inverseEventScope.Counter("rescind"),

		Decline:     poolCallScope.Counter("decline"),
		DeclineFail: poolFailScope.Counter("decline"),

		Pruned:      prunerScope.Counter("pruned"),
		PrunerValid: prunerScope.Gauge("valid"),
	}
}
