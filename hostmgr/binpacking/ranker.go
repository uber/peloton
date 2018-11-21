package binpacking

import "code.uber.internal/infra/peloton/hostmgr/summary"

// Ranker is the interface for bin packing strategy for ranking the host
// it returns the list of ordered list of hosts summary. Caller of the
// interface would get the list of ordered host summary and then match the
// host from 0->n to match the constiants with offer.
type Ranker interface {
	// Returns the name of the ranker implementation
	Name() string
	// returns the list of ranked ordered list
	GetRankedHostList(offerIndex map[string]summary.HostSummary) []interface{}
	// Refreshes the ranker based on new host summary index
	// we need to call this asynchronously to mitigate the
	// performance panality of bin packing.
	RefreshRanking(offerIndex map[string]summary.HostSummary)
}
