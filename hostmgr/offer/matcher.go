package offer

import (
	log "github.com/Sirupsen/logrus"

	mesos "mesos/v1"
	"peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common/constraints"
)

// MatchResult is an enum type describing why a constraint is not matched.
type MatchResult int

const (
	// Matched indicates that offers on a host is matched
	// to given constraint and will be used.
	Matched MatchResult = iota + 1
	// InsufficientResources due to insufficient scalar resources.
	InsufficientResources
	// MismatchAttributes indicates attributes mismatches
	// to task-host scheduling constraint.
	MismatchAttributes
	// MismatchGPU indicates host is reserved for GPU tasks while task not
	// using GPU.
	MismatchGPU
	// MismatchStatus indicates that host is not in ready status.
	MismatchStatus
	// HostLimitExceeded indicates host offers matched so far already
	// exceeded host limit.
	HostLimitExceeded
)

// effectiveHostLimit is common helper function to determine effective limit on
// number of hosts.
func effectiveHostLimit(c *hostsvc.Constraint) uint32 {
	var limit uint32 = 1
	// Zero limit is treated as one.
	if c.HostLimit > 0 {
		limit = c.HostLimit
	}
	return limit
}

// Matcher keeps track of matched host offers for given constraints.
type Matcher struct {
	constraint *hostsvc.Constraint
	evaluator  constraints.Evaluator
	hostOffers map[string][]*mesos.Offer
}

// tryMatch tries to match ready unreserved offers in summary with particular
// constraint.
// If properly matched, the offers will be kept in Matcher for later return,
// otherwise they are untouched.
func (m *Matcher) tryMatch(
	hostname string, summary *hostOfferSummary) MatchResult {

	if m.HasEnoughHosts() {
		return HostLimitExceeded
	}

	if !summary.hasOffer() {
		return MismatchStatus
	}

	tryResult, offers := summary.tryMatch(m.constraint, m.evaluator)
	log.WithFields(log.Fields{
		"constraint": m.constraint,
		"host":       hostname,
		"match":      tryResult,
	}).Debug("Constraint matching result")

	if tryResult == Matched {
		m.hostOffers[hostname] = offers
	}
	return tryResult
}

// HasEnoughHosts returns whether this instance has matched enough hosts based
// on input HostLimit.
func (m *Matcher) HasEnoughHosts() bool {
	return uint32(len(m.hostOffers)) >= effectiveHostLimit(m.constraint)
}

// getHostOffers returns all hostOffers from matcher and clears cached result.
func (m *Matcher) getHostOffers() map[string][]*mesos.Offer {
	result := make(map[string][]*mesos.Offer)
	// swap
	result, m.hostOffers = m.hostOffers, result
	return result
}

// NewMatcher returns a new instance of Matcher.
func NewMatcher(
	constraint *hostsvc.Constraint,
	evaluator constraints.Evaluator,
) *Matcher {
	return &Matcher{
		constraint: constraint,
		evaluator:  evaluator,
		hostOffers: make(map[string][]*mesos.Offer),
	}
}
