package models

import (
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
)

// OfferGroup represents a Mesos offer to placement group mapping.
type OfferGroup struct {
	Offer *Offer
	Group *placement.Group
}
