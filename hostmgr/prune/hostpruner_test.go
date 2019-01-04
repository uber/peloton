package prune

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	hostmgr_offer_offerpool_mocks "github.com/uber/peloton/hostmgr/offer/offerpool/mocks"
)

func TestPrune(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	offerPool := hostmgr_offer_offerpool_mocks.NewMockPool(ctrl)

	testTable := []struct {
		mockResetExpiredHostSummaries []string
		counterPrunedExpected         int64
		msg                           string
	}{
		{
			mockResetExpiredHostSummaries: []string{},
			counterPrunedExpected:         0,
			msg:                           "No host pruned",
		},
		{
			mockResetExpiredHostSummaries: []string{"host0", "host1"},
			counterPrunedExpected:         2,
			msg:                           "2 hosts pruned",
		},
	}

	for _, tt := range testTable {
		testScope := tally.NewTestScope("", map[string]string{})
		offerPool.EXPECT().ResetExpiredHostSummaries(gomock.Any()).Return(tt.mockResetExpiredHostSummaries)
		hostPruner := NewHostPruner(offerPool, testScope)
		hostPruner.Prune(nil)

		assert.Equal(
			t,
			tt.counterPrunedExpected,
			testScope.Snapshot().Counters()["pruned+"].Value(),
			tt.msg,
		)
	}
}
