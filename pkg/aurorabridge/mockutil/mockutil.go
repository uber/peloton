package mockutil

import (
	"fmt"

	"github.com/golang/mock/gomock"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	"github.com/uber/peloton/pkg/aurorabridge/opaquedata"
)

type replaceJobRequestUpdateActionsMatcher struct {
	expected []opaquedata.UpdateAction
}

// MatchReplaceJobRequestUpdateActions matches against the OpaqueData field of
// ReplaceJobRequests.
func MatchReplaceJobRequestUpdateActions(actions []opaquedata.UpdateAction) gomock.Matcher {
	return &replaceJobRequestUpdateActionsMatcher{actions}
}

func (m *replaceJobRequestUpdateActionsMatcher) String() string {
	return fmt.Sprintf("%+v", m.expected)
}

func (m *replaceJobRequestUpdateActionsMatcher) Matches(x interface{}) bool {
	r, ok := x.(*svc.ReplaceJobRequest)
	if !ok {
		return false
	}
	d, err := opaquedata.Deserialize(r.GetOpaqueData())
	if err != nil {
		return false
	}
	if len(d.UpdateActions) != len(m.expected) {
		return false
	}
	for i := range d.UpdateActions {
		if d.UpdateActions[i] != m.expected[i] {
			return false
		}
	}
	return true
}
