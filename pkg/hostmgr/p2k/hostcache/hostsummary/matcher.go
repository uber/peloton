// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hostsummary

import (
	"math"
	"strings"

	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"

	log "github.com/sirupsen/logrus"
)

// effectiveHostLimit is common helper function to determine effective limit on
// number of hosts.
func effectiveHostLimit(f *hostmgr.HostFilter) uint32 {
	limit := f.GetMaxHosts()
	if limit == 0 {
		limit = math.MaxUint32
	}
	return limit
}

// Match represents the result of a match
type Match struct {
	// The result of the match
	Result hostmgr.HostFilterResult
	// host name of the matched host
	HostName string
}

// Matcher keeps track of matched hosts for given constraints.
// This struct is not thread-safe.
type Matcher struct {
	hostFilter *hostmgr.HostFilter

	// List of host names that match the filter.
	hostNames []string

	// This will be used for debugging purposes and passed along to Placement
	// as a API response for AcquireHosts. It will be used to construct a reason
	// string for the result of AcquireHosts API.
	// Keys are of type HostFilterResult, values are counts.
	filterCounts map[string]uint32
}

// NewMatcher returns a new instance of Matcher.
func NewMatcher(hostFilter *hostmgr.HostFilter) *Matcher {
	return &Matcher{
		hostFilter:   hostFilter,
		filterCounts: make(map[string]uint32),
	}
}

// TryMatch tries to match ready host with particular constraint.
// If properly matched, the host name will be kept in Matcher.
func (m *Matcher) TryMatch(
	hostname string,
	s HostSummary) {
	result := m.tryMatchImpl(hostname, s)
	if name, ok := hostmgr.HostFilterResult_name[int32(result)]; ok {
		m.filterCounts[strings.ToLower(name)]++
	}
}

func (m *Matcher) tryMatchImpl(
	hostname string,
	s HostSummary,
) hostmgr.HostFilterResult {
	if m.HostLimitReached() {
		return hostmgr.HostFilterResult_HOST_FILTER_MISMATCH_MAX_HOST_LIMIT
	}

	// try to match host filter with this particular host
	match := s.TryMatch(m.hostFilter)
	log.WithFields(log.Fields{
		"host_filter": m.hostFilter,
		"host":        hostname,
		"status":      s.GetHostStatus(),
		"match":       match,
	}).Debug("match result")

	if match.Result == hostmgr.HostFilterResult_HOST_FILTER_MATCH {
		m.hostNames = append(m.hostNames, match.HostName)
	}
	return match.Result
}

// GetHostNames returns list of host names that match the filter.
func (m *Matcher) GetHostNames() []string {
	return m.hostNames
}

func (m *Matcher) GetFilterCounts() map[string]uint32 {
	return m.filterCounts
}

// HostLimitReached returns true when the matcher has matched hosts equal to
// the max hosts specified in the filter
func (m *Matcher) HostLimitReached() bool {
	return uint32(len(m.hostNames)) >= effectiveHostLimit(m.hostFilter)
}
