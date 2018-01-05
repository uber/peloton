package tracked

import (
	"context"
	"testing"
	"time"

	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskRunAction(t *testing.T) {
	tt := &task{
		lastAction: StopAction,
		job: &job{
			m: &manager{
				mtx: NewMetrics(tally.NoopScope),
			},
		},
	}

	assert.False(t, tt.IsScheduled())

	before := time.Now()

	reschedule, err := tt.RunAction(context.Background(), NoAction)
	assert.False(t, reschedule)
	assert.NoError(t, err)

	la, lt := tt.LastAction()
	assert.Equal(t, NoAction, la)
	assert.True(t, lt.After(before))
}

func TestTaskReloadRuntime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstoreMock := store_mocks.NewMockTaskStore(ctrl)

	tt := &task{
		lastAction: StopAction,
		job: &job{
			m: &manager{
				mtx:       NewMetrics(tally.NoopScope),
				taskStore: taskstoreMock,
			},
		},
	}

	taskstoreMock.EXPECT().
		GetTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	err := tt.reloadRuntime(context.Background())
	assert.Error(t, err)
}
