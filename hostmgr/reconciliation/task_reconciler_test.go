package reconciliation

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	mock_mpb "code.uber.internal/infra/peloton/yarpc/encoding/mpb/mocks"
)

const (
	testStreamID = "teststream"
)

func TestInitImplicitTaskReconcilation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMesosClient := mock_mpb.NewMockClient(ctrl)
	mtx := NewMetrics(tally.NoopScope)
	gomock.InOrder(
		mockMesosClient.EXPECT().
			Call(
				gomock.Eq(testStreamID),
				gomock.Any()).
			Return(nil),
	)

	reconciler := &taskReconciler{
		client:  mockMesosClient,
		metrics: mtx,
	}
	err := reconciler.reconcileTasksImplicitly(nil, testStreamID)
	assert.Equal(t, err, nil)
}

func TestInitImplicitTaskReconcilationFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMesosClient := mock_mpb.NewMockClient(ctrl)
	mtx := NewMetrics(tally.NoopScope)
	gomock.InOrder(
		mockMesosClient.EXPECT().
			Call(
				gomock.Eq(testStreamID),
				gomock.Any()).
			Return(fmt.Errorf("fake error")),
	)

	reconciler := &taskReconciler{
		client:  mockMesosClient,
		metrics: mtx,
	}
	err := reconciler.reconcileTasksImplicitly(nil, testStreamID)
	assert.NotNil(t, err)
}
