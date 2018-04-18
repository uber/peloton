package goalstate

import (
	"sync"
	"testing"

	"github.com/golang/mock/gomock"

	"code.uber.internal/infra/peloton/jobmgr/tracked/mocks"
)

func TestEngineStartStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tmMock := mocks.NewMockManager(ctrl)

	e := &engine{
		trackedManager: tmMock,
	}

	var wg sync.WaitGroup
	wg.Add(2)

	tmMock.EXPECT().WaitForScheduledTask(gomock.Any()).Do(func(stopChan <-chan struct{}) {
		<-stopChan
		wg.Done()
	}).Return(nil)

	tmMock.EXPECT().WaitForScheduledJob(gomock.Any()).Do(func(stopChan <-chan struct{}) {
		<-stopChan
		wg.Done()
	}).Return(nil)

	e.Start()

	e.Stop()

	wg.Wait()
}
