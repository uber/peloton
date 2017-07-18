package scalar

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/common"

	"github.com/stretchr/testify/assert"
)

const zeroEpsilon = 0.000001

func TestAdd(t *testing.T) {
	empty := Resources{}
	r1 := Resources{
		CPU: 1.0,
	}

	result := empty.Add(&empty)
	assert.InEpsilon(t, 0.0, result.CPU, zeroEpsilon)
	assert.InEpsilon(t, 0.0, result.MEMORY, zeroEpsilon)
	assert.InEpsilon(t, 0.0, result.DISK, zeroEpsilon)
	assert.InEpsilon(t, 0.0, result.GPU, zeroEpsilon)

	result = r1.Add(&Resources{})
	assert.InEpsilon(t, 1.0, result.CPU, zeroEpsilon)
	assert.InEpsilon(t, 0.0, result.MEMORY, zeroEpsilon)
	assert.InEpsilon(t, 0.0, result.DISK, zeroEpsilon)
	assert.InEpsilon(t, 0.0, result.GPU, zeroEpsilon)

	r2 := Resources{
		CPU:    4.0,
		MEMORY: 3.0,
		DISK:   2.0,
		GPU:    1.0,
	}
	result = r1.Add(&r2)
	assert.InEpsilon(t, 5.0, result.CPU, zeroEpsilon)
	assert.InEpsilon(t, 3.0, result.MEMORY, zeroEpsilon)
	assert.InEpsilon(t, 2.0, result.DISK, zeroEpsilon)
	assert.InEpsilon(t, 1.0, result.GPU, zeroEpsilon)
}

func TestSubtract(t *testing.T) {
	empty := Resources{}
	r1 := Resources{
		CPU:    1.0,
		MEMORY: 2.0,
		DISK:   3.0,
		GPU:    4.0,
	}

	res := r1.Subtract(&empty)
	assert.NotNil(t, res)
	assert.InEpsilon(t, 1.0, res.CPU, zeroEpsilon)
	assert.InEpsilon(t, 2.0, res.MEMORY, zeroEpsilon)
	assert.InEpsilon(t, 3.0, res.DISK, zeroEpsilon)
	assert.InEpsilon(t, 4.0, res.GPU, zeroEpsilon)

	r2 := Resources{
		CPU:    2.0,
		MEMORY: 5.0,
		DISK:   4.0,
		GPU:    7.0,
	}
	res = r2.Subtract(&r1)

	assert.NotNil(t, res)
	assert.InEpsilon(t, 1.0, res.CPU, zeroEpsilon)
	assert.InEpsilon(t, 3.0, res.MEMORY, zeroEpsilon)
	assert.InEpsilon(t, 1.0, res.DISK, zeroEpsilon)
	assert.InEpsilon(t, 3.0, res.GPU, zeroEpsilon)

	res = r1.Subtract(&r2)
	assert.InEpsilon(t, 0.0, res.CPU, zeroEpsilon)
	assert.InEpsilon(t, 0.0, res.MEMORY, zeroEpsilon)
	assert.InEpsilon(t, 0.0, res.DISK, zeroEpsilon)
	assert.InEpsilon(t, 0.0, res.GPU, zeroEpsilon)
}

func TestLessThanOrEqual(t *testing.T) {
	r1 := Resources{
		CPU:    1.0,
		MEMORY: 2.0,
		DISK:   3.0,
		GPU:    4.0,
	}
	r2 := Resources{
		CPU:    2.0,
		MEMORY: 5.0,
		DISK:   4.0,
		GPU:    7.0,
	}
	assert.EqualValues(t, true, r1.LessThanOrEqual(&r2))
	assert.EqualValues(t, false, r2.LessThanOrEqual(&r1))
}

func TestConvertToResmgrResource(t *testing.T) {
	taskConfig := &task.ResourceConfig{
		CpuLimit:    4.0,
		DiskLimitMb: 5.0,
		GpuLimit:    1.0,
		MemLimitMb:  10.0,
	}
	res := ConvertToResmgrResource(taskConfig)
	assert.EqualValues(t, 4.0, res.CPU)
	assert.EqualValues(t, 5.0, res.DISK)
	assert.EqualValues(t, 1.0, res.GPU)
	assert.EqualValues(t, 10.0, res.MEMORY)
}

func TestGet(t *testing.T) {
	r1 := Resources{
		CPU:    1.0,
		MEMORY: 2.0,
		DISK:   3.0,
		GPU:    4.0,
	}
	assert.EqualValues(t, float64(1.0), r1.Get(common.CPU))
	assert.EqualValues(t, float64(4.0), r1.Get(common.GPU))
	assert.EqualValues(t, float64(2.0), r1.Get(common.MEMORY))
	assert.EqualValues(t, float64(3.0), r1.Get(common.DISK))
}

func TestSet(t *testing.T) {
	r1 := Resources{
		CPU:    1.0,
		MEMORY: 2.0,
		DISK:   3.0,
		GPU:    4.0,
	}
	assert.EqualValues(t, float64(1.0), r1.Get(common.CPU))
	assert.EqualValues(t, float64(4.0), r1.Get(common.GPU))
	assert.EqualValues(t, float64(2.0), r1.Get(common.MEMORY))
	assert.EqualValues(t, float64(3.0), r1.Get(common.DISK))
	r1.Set(common.CPU, float64(2.0))
	r1.Set(common.MEMORY, float64(3.0))
	r1.Set(common.DISK, float64(4.0))
	r1.Set(common.GPU, float64(5.0))
	assert.EqualValues(t, float64(2.0), r1.Get(common.CPU))
	assert.EqualValues(t, float64(5.0), r1.Get(common.GPU))
	assert.EqualValues(t, float64(3.0), r1.Get(common.MEMORY))
	assert.EqualValues(t, float64(4.0), r1.Get(common.DISK))
}
