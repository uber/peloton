// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scalar

import (
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common"

	"github.com/stretchr/testify/assert"
)

const _zeroDelta = 0.000001

func TestAdd(t *testing.T) {
	empty := Resources{}
	r1 := Resources{
		CPU: 1.0,
	}

	result := empty.Add(&empty)
	assertEqual(t, &Resources{0.0, 0.0, 0.0, 0.0}, result)

	result = r1.Add(&Resources{})
	assertEqual(t, &Resources{1.0, 0.0, 0.0, 0.0}, result)

	r2 := Resources{
		CPU:    4.0,
		MEMORY: 3.0,
		DISK:   2.0,
		GPU:    1.0,
	}
	result = r1.Add(&r2)
	assertEqual(t, &Resources{5.0, 3.0, 2.0, 1.0}, result)
}

func assertEqual(t *testing.T, expected *Resources, result *Resources) {
	for _, typeRes := range []string{
		common.CPU,
		common.MEMORY,
		common.DISK,
		common.GPU} {
		assert.InDelta(t, expected.Get(typeRes), result.Get(typeRes), _zeroDelta)
	}
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
	assertEqual(t, &Resources{1.0, 2.0, 3.0, 4.0}, res)

	r2 := Resources{
		CPU:    2.0,
		MEMORY: 5.0,
		DISK:   4.0,
		GPU:    7.0,
	}
	res = r2.Subtract(&r1)

	assert.NotNil(t, res)
	assertEqual(t, &Resources{1.0, 3.0, 1.0, 3.0}, res)

	res = r1.Subtract(&r2)
	assertEqual(t, &Resources{0.0, 0.0, 0.0, 0.0}, res)
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
	assertEqual(t, &Resources{0.0, 0.0, 0.0, 0.0}, res)
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
	assertEqual(t, &Resources{4.0, 10.0, 5.0, 1.0}, res)
}

func TestSet(t *testing.T) {
	r1 := Resources{
		CPU:    1.0,
		MEMORY: 2.0,
		DISK:   3.0,
		GPU:    4.0,
	}
	assertEqual(t, &Resources{1.0, 2.0, 3.0, 4.0}, &r1)
	r1.Set(common.CPU, float64(2.0))
	r1.Set(common.MEMORY, float64(3.0))
	r1.Set(common.DISK, float64(4.0))
	r1.Set(common.GPU, float64(5.0))
	assertEqual(t, &Resources{2.0, 3.0, 4.0, 5.0}, &r1)
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

func TestMinResources(t *testing.T) {
	r1 := &Resources{
		CPU:    0,
		MEMORY: 100,
		DISK:   1000,
		GPU:    10,
	}

	r2 := &Resources{
		CPU:    10,
		MEMORY: 80,
		DISK:   1000,
		GPU:    4,
	}

	result := Min(r1, r2)
	assert.Equal(t, result.CPU, float64(0))
	assert.Equal(t, result.MEMORY, float64(80))
	assert.Equal(t, result.DISK, float64(1000))
	assert.Equal(t, result.GPU, float64(4))
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
		assertEqual(t, &Resources{4.0, 10.0, 5.0, 1.0}, res)

		// these should be equal to the taskConfig
		for _, allocType := range test.hasAlloc {
			res := alloc.GetByType(allocType)
			assertEqual(t, &Resources{4.0, 10.0, 5.0, 1.0}, res)
		}

		// these should be equal to zero
		for _, allocType := range test.noAlloc {
			res := alloc.GetByType(allocType)
			assert.Equal(t, ZeroResource, res)
		}
	}
}

func TestGetGangResources(t *testing.T) {
	res := GetGangResources(nil)
	assert.Nil(t, res)
	res = GetGangResources(&resmgrsvc.Gang{
		Tasks: []*resmgr.Task{
			{
				Resource: &task.ResourceConfig{
					CpuLimit:    1,
					DiskLimitMb: 1,
					GpuLimit:    1,
					MemLimitMb:  1,
				},
			},
		},
	})
	assertEqual(t, &Resources{1.0, 1.0, 1.0, 1.0}, res)
	assert.Equal(t, "CPU:1.00 MEM:1.00 DISK:1.00 GPU:1.00", res.String())
}

func TestGetGangAllocation(t *testing.T) {
	res := GetGangAllocation(&resmgrsvc.Gang{
		Tasks: []*resmgr.Task{
			{
				Resource: &task.ResourceConfig{
					CpuLimit:    1,
					DiskLimitMb: 1,
					GpuLimit:    1,
					MemLimitMb:  1,
				},
			},
		},
	})
	assertEqual(t, &Resources{1.0, 1.0, 1.0, 1.0}, res.GetByType(TotalAllocation))
}
