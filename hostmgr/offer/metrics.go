package offer

import (
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/hostmgr/scalar"
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
}

// NewMetrics returns a new Metrics struct, with all metrics initialized
// and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	poolScope := scope.SubScope("pool")
	readyScope := poolScope.SubScope("ready")
	placingScope := poolScope.SubScope("placing")

	handlerScope := scope.SubScope("handler")
	offerEventScope := handlerScope.Tagged(map[string]string{"type": "offer"})
	inverseEventScope := handlerScope.Tagged(map[string]string{"type": "inverse"})

	return &Metrics{
		ready:   scalar.NewGaugeMaps(readyScope),
		placing: scalar.NewGaugeMaps(placingScope),

		OfferEvents:   offerEventScope.Counter("offer"),
		RescindEvents: offerEventScope.Counter("rescind"),

		InverseOfferEvents:        inverseEventScope.Counter("offer"),
		RescindInverseOfferEvents: inverseEventScope.Counter("rescind"),
	}
}
