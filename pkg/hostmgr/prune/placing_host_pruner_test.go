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

package prune

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	hostmgr_offer_offerpool_mocks "github.com/uber/peloton/pkg/hostmgr/offer/offerpool/mocks"
)

func TestPlacingHostPrune(t *testing.T) {
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
		offerPool.EXPECT().ResetExpiredPlacingHostSummaries(gomock.Any()).Return(tt.mockResetExpiredHostSummaries)
		hostPruner := NewPlacingHostPruner(offerPool, testScope)
		hostPruner.Prune(nil)

		assert.Equal(
			t,
			tt.counterPrunedExpected,
			testScope.Snapshot().Counters()["pruned+"].Value(),
			tt.msg,
		)
	}
}
