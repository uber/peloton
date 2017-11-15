package tracked

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
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

	reschedule, err := tt.RunAction(context.Background(), NoAction)
	assert.False(t, reschedule)
	assert.NoError(t, err)

	la, lt := tt.LastAction()
	assert.Equal(t, NoAction, la)
	assert.True(t, lt.After(before))
}
