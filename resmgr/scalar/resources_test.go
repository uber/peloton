package scalar

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

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

func TestSubtractLessThanEpsilon(t *testing.T) {
	r1 := Resources{
		CPU:    1.0,
		MEMORY: 2.0,
		DISK:   3.0,
		GPU:    4.0,
	}
	r2 := Resources{
		CPU:    1.00009,
		MEMORY: 2.00009,
		DISK:   3.00009,
		GPU:    4.00009,
	}
	res := r2.Subtract(&r1)
	assert.NotNil(t, res)
	assert.Equal(t, float64(0), res.CPU)
	assert.Equal(t, float64(0), res.MEMORY)
	assert.Equal(t, float64(0), res.DISK)
	assert.Equal(t, float64(0), res.GPU)
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

func TestEqual(t *testing.T) {
	r1 := Resources{
		CPU:    1.0,
		MEMORY: 2.0,
		DISK:   3.0,
		GPU:    4.0,
	}
	r2 := Resources{
		CPU:    1.0,
		MEMORY: 2.0,
		DISK:   3.0,
		GPU:    4.0,
	}
	r3 := Resources{
		CPU:    0.0,
		MEMORY: 2.2,
		DISK:   3.0,
		GPU:    4.0,
	}
	assert.EqualValues(t, true, r1.Equal(&r2))
	assert.EqualValues(t, false, r2.Equal(&r3))
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

func TestClone(t *testing.T) {
	r1 := Resources{
		CPU:    1.0,
		MEMORY: 2.0,
		DISK:   3.0,
		GPU:    4.0,
	}
	r2 := r1.Clone()
	assert.Equal(t, r1.Equal(r2), true)
}

func TestCopy(t *testing.T) {
	r1 := Resources{
		CPU:    1.0,
		MEMORY: 2.0,
		DISK:   3.0,
		GPU:    4.0,
	}
	r2 := Resources{
		CPU:    0.0,
		MEMORY: 0.0,
		DISK:   0.0,
		GPU:    0.0,
	}
	r2.Copy(&r1)
	assert.Equal(t, r1.Equal(&r2), true)
}

func TestInitializeAllocation(t *testing.T) {
	alloc := NewAllocation()
	for _, v := range alloc.Value {
		assert.Equal(t, v, ZeroResource)
	}
}

func withTotalAlloc() *Allocation {
	return withAllocByType(TotalAllocation)
}

func withNonPreemptibleAlloc() *Allocation {
	return withAllocByType(NonPreemptibleAllocation)
}

func withControllerAlloc() *Allocation {
	return withAllocByType(ControllerAllocation)
}

func withAllocByType(allocationType AllocationType) *Allocation {
	alloc := initializeZeroAlloc()

	alloc.Value[allocationType] = &Resources{
		CPU:    1.0,
		MEMORY: 2.0,
		DISK:   3.0,
		GPU:    4.0,
	}
	return alloc
}

func TestAddAllocation(t *testing.T) {
	totalAlloc := withTotalAlloc()
	npAlloc := withNonPreemptibleAlloc()

	expectedAlloc := initializeZeroAlloc()
	expectedAlloc.Value[TotalAllocation] = &Resources{
		CPU:    1.0,
		MEMORY: 2.0,
		DISK:   3.0,
		GPU:    4.0,
	}
	expectedAlloc.Value[NonPreemptibleAllocation] = &Resources{
		CPU:    1.0,
		MEMORY: 2.0,
		DISK:   3.0,
		GPU:    4.0,
	}
	expectedAlloc.Value[ControllerAllocation] = ZeroResource
	totalAlloc = totalAlloc.Add(npAlloc)
	assert.Equal(t, expectedAlloc, totalAlloc)

	controllerAlloc := withControllerAlloc()
	expectedAlloc.Value[ControllerAllocation] = &Resources{
		CPU:    1.0,
		MEMORY: 2.0,
		DISK:   3.0,
		GPU:    4.0,
	}
	assert.Equal(t, expectedAlloc, totalAlloc.Add(controllerAlloc))
}

func TestSubtractAllocation(t *testing.T) {
	npAlloc := withNonPreemptibleAlloc() // only has non-preemptible alloc
	totalAlloc := withTotalAlloc()       // only has total alloc

	totalAlloc = totalAlloc.Subtract(npAlloc)

	//since they are in different dimensions they should not be affected
	assert.NotEqual(t, ZeroResource, totalAlloc)

	totalAlloc = totalAlloc.Subtract(totalAlloc)
	for _, v := range totalAlloc.Value {
		assert.Equal(t, ZeroResource, v)
	}

	npAlloc = npAlloc.Subtract(npAlloc)
	for _, v := range npAlloc.Value {
		assert.Equal(t, ZeroResource, v)
	}
}

func TestGetTaskAllocation(t *testing.T) {
	taskConfig := &task.ResourceConfig{
		CpuLimit:    4.0,
		DiskLimitMb: 5.0,
		GpuLimit:    1.0,
		MemLimitMb:  10.0,
	}

	tt := []struct {
		preemptible bool
		controller  bool
		noAlloc     []AllocationType
		hasAlloc    []AllocationType
	}{
		{
			// represents a preemptible batch task
			preemptible: true,
			controller:  false,
			noAlloc:     []AllocationType{NonPreemptibleAllocation, ControllerAllocation},
			hasAlloc:    []AllocationType{PreemptibleAllocation},
		},
		{
			// represents a non-preemptible batch task
			preemptible: false,
			controller:  false,
			noAlloc:     []AllocationType{PreemptibleAllocation, ControllerAllocation},
			hasAlloc:    []AllocationType{NonPreemptibleAllocation},
		},
		{
			// represents a non-preemptible controller task
			preemptible: false,
			controller:  true,
			noAlloc:     []AllocationType{PreemptibleAllocation},
			hasAlloc:    []AllocationType{ControllerAllocation, NonPreemptibleAllocation},
		},
		{
			// represents a preemptible controller task
			preemptible: true,
			controller:  true,
			noAlloc:     []AllocationType{NonPreemptibleAllocation},
			hasAlloc:    []AllocationType{ControllerAllocation, PreemptibleAllocation},
		},
	}

	for _, test := range tt {
		rmTask := &resmgr.Task{
			Preemptible: test.preemptible,
			Resource:    taskConfig,
			Controller:  test.controller,
		}

		alloc := GetTaskAllocation(rmTask)

		// total should always be equal to the taskConfig
		res := alloc.GetByType(TotalAllocation)
		assert.Equal(t, float64(4.0), res.CPU)
		assert.Equal(t, float64(5.0), res.DISK)
		assert.Equal(t, float64(1.0), res.GPU)
		assert.Equal(t, float64(10.0), res.MEMORY)

		// these should be equal to the taskConfig
		for _, allocType := range test.hasAlloc {
			res := alloc.GetByType(allocType)
			assert.Equal(t, float64(4.0), res.CPU)
			assert.Equal(t, float64(5.0), res.DISK)
			assert.Equal(t, float64(1.0), res.GPU)
			assert.Equal(t, float64(10.0), res.MEMORY)
		}

		// these should be equal to zero
		for _, allocType := range test.noAlloc {
			res := alloc.GetByType(allocType)
			assert.Equal(t, ZeroResource, res)
		}
	}
}
