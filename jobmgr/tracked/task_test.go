package tracked

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

func TestTaskRunAction(t *testing.T) {
	tt := &task{
		lastAction: StopAction,
		job: &job{
			m: &manager{
				mtx: newMetrics(tally.NoopScope),
			},
		},
	}

	before := time.Now()

	assert.NoError(t, tt.RunAction(context.Background(), NoAction))

	la, lt := tt.LastAction()
	assert.Equal(t, NoAction, la)
	assert.True(t, lt.After(before))
}

func TestTaskRunActionReloadRuntime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstoreMock := store_mocks.NewMockTaskStore(ctrl)

	tt := &task{
		lastAction: StopAction,
		job: &job{
			m: &manager{
				taskStore: taskstoreMock,
				mtx:       newMetrics(tally.NoopScope),
			},
		},
	}

	taskstoreMock.EXPECT().GetTaskRuntime(context.Background(), gomock.Any(), gomock.Any()).Return(nil, nil)

	assert.NoError(t, tt.RunAction(context.Background(), ReloadRuntime))
}

func TestTaskGetRuntime(t *testing.T) {
	tt := &task{}

	r, err := tt.getRuntime()
	assert.Nil(t, r)
	assert.EqualError(t, err, "missing task runtime")

	tt.runtime = &pb_task.RuntimeInfo{
		Revision: &peloton.Revision{
			Version: 5,
		},
	}

	r, err = tt.getRuntime()
	assert.Equal(t, uint64(5), r.Revision.Version)
	assert.NoError(t, err)

	r.Revision.Version = 6

	assert.Equal(t, uint64(5), tt.runtime.Revision.Version)
}
