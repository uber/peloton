package cleaner

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/uber-go/tally"

	offerpool_mocks "code.uber.internal/infra/peloton/hostmgr/offer/offerpool/mocks"
)

func TestCleaner(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	offerPool := offerpool_mocks.NewMockPool(ctrl)

	testScope := tally.NewTestScope("", map[string]string{})
	offerPool.EXPECT().CleanReservationResources()
	hostPruner := NewCleaner(offerPool, testScope)
	hostPruner.Run(nil)
}
