package offerpool

import (
	"math"
	"strings"

	log "github.com/sirupsen/logrus"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common/constraints"
	"code.uber.internal/infra/peloton/hostmgr/summary"
)

// effectiveHostLimit is common helper function to determine effective limit on
// number of hosts.
func effectiveHostLimit(f *hostsvc.HostFilter) uint32 {
	limit := f.GetQuantity().GetMaxHosts()
	if limit == 0 {
		limit = math.MaxUint32
	}
	return limit
}

// Matcher keeps track of matched host offers for given constraints.
type Matcher struct {
	hostFilter *hostsvc.HostFilter
	evaluator  constraints.Evaluator
	hostOffers map[string][]*mesos.Offer

	filterResultCounts map[string]uint32
}

// tryMatch tries to match ready unreserved offers in summary with particular
// constraint.
// If properly matched, the offers will be kept in Matcher for later return,
// otherwise they are untouched.
func (m *Matcher) tryMatch(
	hostname string,
	s summary.HostSummary) {
	result := m.tryMatchImpl(hostname, s)
	if name, ok := hostsvc.HostFilterResult_name[int32(result)]; !ok {
		log.WithField("value", result).
			Error("Unknown enum value for HostFilterResult_name")
	} else {
		m.filterResultCounts[strings.ToLower(name)]++
	}
}

func (m *Matcher) tryMatchImpl(
	hostname string,
	s summary.HostSummary) hostsvc.HostFilterResult {
	if m.HasEnoughHosts() {
		return hostsvc.HostFilterResult_MISMATCH_MAX_HOST_LIMIT
	}

	tryResult, offers := s.TryMatch(m.hostFilter, m.evaluator)
	log.WithFields(log.Fields{
		"host_filter": m.hostFilter,
		"host":        hostname,
		"match":       tryResult,
	}).Debug("Constraint matching result")

	if tryResult == hostsvc.HostFilterResult_MATCH {
		m.hostOffers[hostname] = offers
	}
	return tryResult
}

// HasEnoughHosts returns whether this instance has matched enough hosts based
// on input HostLimit.
func (m *Matcher) HasEnoughHosts() bool {
	return uint32(len(m.hostOffers)) >= effectiveHostLimit(m.hostFilter)
}

// getHostOffers returns all hostOffers from matcher and clears cached result.
func (m *Matcher) getHostOffers() (map[string][]*mesos.Offer, map[string]uint32) {
	result := make(map[string][]*mesos.Offer)
	resultCount := make(map[string]uint32)
	// swap
	result, m.hostOffers = m.hostOffers, result
	resultCount, m.filterResultCounts = m.filterResultCounts, resultCount
	return result, resultCount
}

// NewMatcher returns a new instance of Matcher.
func NewMatcher(
	hostFilter *hostsvc.HostFilter,
	evaluator constraints.Evaluator,
) *Matcher {
	return &Matcher{
		hostFilter:         hostFilter,
		evaluator:          evaluator,
		hostOffers:         make(map[string][]*mesos.Offer),
		filterResultCounts: make(map[string]uint32),
	}
}
