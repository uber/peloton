package tracked

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"github.com/stretchr/testify/assert"
)

func TestTaskUpdateRuntimeToNilAlwaysOverrides(t *testing.T) {
	tt := &task{
		runtime: &pb_task.RuntimeInfo{
			Revision: &peloton.Revision{Version: 42},
		},
	}

	tt.updateRuntime(nil)

	assert.Nil(t, tt.runtime)
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
