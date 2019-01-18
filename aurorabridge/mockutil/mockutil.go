package mockutil

import (
	"github.com/golang/mock/gomock"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/aurorabridge/opaquedata"
)

type replaceJobRequestOpaqueDataMatcher struct {
	expected *peloton.OpaqueData
}

// MatchReplaceJobRequestOpaqueData matches against the OpaqueData field of
// ReplaceJobRequests.
func MatchReplaceJobRequestOpaqueData(d *opaquedata.Data) gomock.Matcher {
	od, err := d.Serialize()
	if err != nil {
		panic(err)
	}
	return &replaceJobRequestOpaqueDataMatcher{od}
}

func (m *replaceJobRequestOpaqueDataMatcher) String() string {
	return m.expected.GetData()
}

func (m *replaceJobRequestOpaqueDataMatcher) Matches(x interface{}) bool {
	r, ok := x.(*svc.ReplaceJobRequest)
	if !ok {
		return false
	}
	return r.GetOpaqueData().GetData() == m.expected.GetData()
}
