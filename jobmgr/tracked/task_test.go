package tracked

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTaskRunAction(t *testing.T) {
	tt := &task{
		job:        &job{},
		lastAction: StopAction,
	}

	before := time.Now()

	assert.NoError(t, tt.RunAction(context.Background(), NoAction))

	la, lt := tt.LastAction()
	assert.Equal(t, NoAction, la)
	assert.True(t, lt.After(before))
}
