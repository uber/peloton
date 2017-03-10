package task

import (
	"github.com/stretchr/testify/assert"
	p_task "peloton/api/task"
	"testing"
)

func TestIsErrorState(t *testing.T) {
	assert.Equal(t, true, isUnexpected(p_task.RuntimeInfo_FAILED))
	assert.Equal(t, true, isUnexpected(p_task.RuntimeInfo_LOST))

	assert.Equal(t, false, isUnexpected(p_task.RuntimeInfo_KILLED))
	assert.Equal(t, false, isUnexpected(p_task.RuntimeInfo_LAUNCHED))
	assert.Equal(t, false, isUnexpected(p_task.RuntimeInfo_RUNNING))
	assert.Equal(t, false, isUnexpected(p_task.RuntimeInfo_SUCCEEDED))
	assert.Equal(t, false, isUnexpected(p_task.RuntimeInfo_INITIALIZED))
}
