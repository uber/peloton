package reconcile

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"

	mesos "mesos/v1"

	mock_mpb "code.uber.internal/infra/peloton/yarpc/encoding/mpb/mocks"
)

const (
	streamID                                = "streamID"
	frameworkID                             = "frameworkID"
	initialReconcileDelay     time.Duration = 300 * time.Millisecond
	implicitReconcileInterval time.Duration = 100 * time.Millisecond
)

// A mock implementation of FrameworkInfoProvider
type mockFrameworkInfoProvider struct{}

func (m *mockFrameworkInfoProvider) GetMesosStreamID() string {
	return streamID
}

func (m *mockFrameworkInfoProvider) GetFrameworkID() *mesos.FrameworkID {
	tmp := frameworkID
	return &mesos.FrameworkID{Value: &tmp}
}

func TestInitImplicitTaskReconcilationOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMesosClient := mock_mpb.NewMockClient(ctrl)
	mtx := NewMetrics(tally.NoopScope)
	reconciler := &taskReconciler{
		Running:                   atomic.NewBool(false),
		client:                    mockMesosClient,
		metrics:                   mtx,
		frameworkInfoProvider:     &mockFrameworkInfoProvider{},
		initialReconcileDelay:     initialReconcileDelay,
		implicitReconcileInterval: implicitReconcileInterval,
		stopChan:                  make(chan struct{}, 1),
	}

	gomock.InOrder(
		mockMesosClient.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any()).
			Return(nil),
	)

	reconciler.Start()
	time.Sleep(350 * time.Millisecond)
	assert.Equal(t, len(reconciler.stopChan), 0)
	reconciler.Stop()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, len(reconciler.stopChan), 0)
}

func TestInitImplicitTaskReconcilationWithPeriodicalCalls(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMesosClient := mock_mpb.NewMockClient(ctrl)
	mtx := NewMetrics(tally.NoopScope)
	reconciler := &taskReconciler{
		Running:                   atomic.NewBool(false),
		client:                    mockMesosClient,
		metrics:                   mtx,
		frameworkInfoProvider:     &mockFrameworkInfoProvider{},
		initialReconcileDelay:     initialReconcileDelay,
		implicitReconcileInterval: implicitReconcileInterval,
		stopChan:                  make(chan struct{}, 1),
	}

	gomock.InOrder(
		mockMesosClient.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any()).
			Return(nil).
			Times(3),
	)

	reconciler.Start()
	time.Sleep(550 * time.Millisecond)
	assert.Equal(t, len(reconciler.stopChan), 0)
	reconciler.Stop()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, len(reconciler.stopChan), 0)
}

func TestInitImplicitTaskReconcilationPeriodicalFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMesosClient := mock_mpb.NewMockClient(ctrl)
	mtx := NewMetrics(tally.NoopScope)
	reconciler := &taskReconciler{
		Running:                   atomic.NewBool(false),
		client:                    mockMesosClient,
		metrics:                   mtx,
		frameworkInfoProvider:     &mockFrameworkInfoProvider{},
		initialReconcileDelay:     initialReconcileDelay,
		implicitReconcileInterval: implicitReconcileInterval,
		stopChan:                  make(chan struct{}, 1),
	}

	gomock.InOrder(
		mockMesosClient.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any()).
			Return(nil).
			Times(1),
		mockMesosClient.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any()).
			Return(fmt.Errorf("fake error")).
			Times(1),
	)

	reconciler.Start()
	time.Sleep(450 * time.Millisecond)
	assert.Equal(t, len(reconciler.stopChan), 0)
	reconciler.Stop()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, len(reconciler.stopChan), 0)
}

func TestInitImplicitTaskReconcilationNotStartIfAlreadyRunning(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMesosClient := mock_mpb.NewMockClient(ctrl)
	mtx := NewMetrics(tally.NoopScope)
	reconciler := &taskReconciler{
		Running:                   atomic.NewBool(false),
		client:                    mockMesosClient,
		metrics:                   mtx,
		frameworkInfoProvider:     &mockFrameworkInfoProvider{},
		initialReconcileDelay:     initialReconcileDelay,
		implicitReconcileInterval: implicitReconcileInterval,
		stopChan:                  make(chan struct{}, 1),
	}

	gomock.InOrder(
		mockMesosClient.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any()).
			Return(nil),
	)

	reconciler.Stop()
	reconciler.Start()
	reconciler.Start()
	time.Sleep(350 * time.Millisecond)
	assert.Equal(t, len(reconciler.stopChan), 0)
	reconciler.Stop()
	reconciler.Stop()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, len(reconciler.stopChan), 0)
}
