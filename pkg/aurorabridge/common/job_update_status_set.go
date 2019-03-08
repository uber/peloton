package common

import (
	"sort"
	"strings"

	"github.com/uber/peloton/.gen/thrift/aurora/api"
)

// JobUpdateStatusSet is an immutable set of JobUpdateStatuses.
type JobUpdateStatusSet struct {
	m map[api.JobUpdateStatus]struct{}
}

// NewJobUpdateStatusSet creates a new JobUpdateStatusSet.
func NewJobUpdateStatusSet(statuses ...api.JobUpdateStatus) *JobUpdateStatusSet {
	m := make(map[api.JobUpdateStatus]struct{}, len(statuses))
	for _, s := range statuses {
		m[s] = struct{}{}
	}
	return &JobUpdateStatusSet{m}
}

// Has returns whether x is in s.
func (s *JobUpdateStatusSet) Has(x api.JobUpdateStatus) bool {
	_, ok := s.m[x]
	return ok
}

func (s *JobUpdateStatusSet) String() string {
	var l []string
	for x := range s.m {
		l = append(l, x.String())
	}
	sort.Strings(l)
	return "{" + strings.Join(l, ", ") + "}"
}
