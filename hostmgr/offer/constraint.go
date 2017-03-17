package offer

import (
	"peloton/private/hostmgr/hostsvc"

	mesos "mesos/v1"

	"code.uber.internal/infra/peloton/hostmgr/scalar"

	log "github.com/Sirupsen/logrus"
)

// Constraint is a wrapper around hostsvc.Constraint type, with additional mention functions defined.
type Constraint struct {
	hostsvc.Constraint
}

// effectiveLimit is common helper function to determine effective limit on number of OfferOffer.
func (c *Constraint) effectiveLimit() uint32 {
	var limit uint32 = 1
	// Zero limit is treated as one.
	if c.Limit > 0 {
		limit = c.Limit
	}
	return limit
}

// match determines whether given constraint matches the given map of offers.
func (c *Constraint) match(offerMap map[string]*mesos.Offer) bool {
	min := c.GetResourceConstraint().GetMinimum()
	if min != nil {
		scalarRes := scalar.FromOfferMap(offerMap)
		scalarMin := scalar.FromResourceConfig(min)
		if !scalarRes.Contains(&scalarMin) {
			return false
		}

		// Special handling for GPU: GPU hosts are only for GPU tasks.
		if scalarRes.HasGPU() != scalarMin.HasGPU() {
			return false
		}
	}

	// TODO(zhitao): Add attribute based matching here.
	return true
}

// ConstraintMatcher keeps a list of constraints and try to match each incoming HostOffer.
type ConstraintMatcher struct {
	constraints []*Constraint

	// matchedHostCount is the number of `HostOffer` already matched for the constraint with the same index.
	matchedHostCount []uint32

	hostOffers map[string][]*mesos.Offer
}

// tryMatch determines whether given list of offers matches a particular constraint and should be kept.
func (m *ConstraintMatcher) tryMatch(hostname string, summary *hostOfferSummary) bool {
	if !summary.hasOffer() {
		return false
	}

	for index, c := range m.constraints {
		if m.matchedHostCount[index] >= c.effectiveLimit() {
			continue
		}
		if matched, offers := summary.tryMatch(c); matched {
			log.WithFields(log.Fields{
				"constraint": *c,
				"host":       hostname,
			}).Debug("Constraint matched host offers")
			m.matchedHostCount[index]++
			m.hostOffers[hostname] = offers
			return true
		}
	}

	log.WithFields(log.Fields{
		"constraints": m.constraints,
		"host":        hostname,
	}).Debug("Host Offers is not matched by any constraint")

	return false
}

// HasEnoughOffers returns whether this instance has matched enough host offers.
func (m *ConstraintMatcher) HasEnoughOffers() bool {
	for index, c := range m.constraints {
		if m.matchedHostCount[index] < c.effectiveLimit() {
			return false
		}
	}
	return true
}

// claimHostOffers returns all hostOffers from this matcher and clears cached result.
func (m *ConstraintMatcher) claimHostOffers() map[string][]*mesos.Offer {
	result := make(map[string][]*mesos.Offer)
	// swap
	result, m.hostOffers = m.hostOffers, result
	m.matchedHostCount = make([]uint32, len(m.constraints))
	return result
}

// NewConstraintMatcher returns a new instance of ConstraintMatcher.
func NewConstraintMatcher(constraints []*Constraint) *ConstraintMatcher {
	return &ConstraintMatcher{
		constraints:      constraints,
		matchedHostCount: make([]uint32, len(constraints)),
		hostOffers:       make(map[string][]*mesos.Offer),
	}
}
