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
	"fmt"
	"math"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"

	log "github.com/sirupsen/logrus"
)

// AllocationType represents the different allocation dimensions the resource
// pool can track for admission control
type AllocationType int

const (
	// NonPreemptibleAllocation tracks allocation for non-preemptible tasks
	NonPreemptibleAllocation AllocationType = iota + 1
	// PreemptibleAllocation tracks allocation for preemptible tasks
	PreemptibleAllocation
	// ControllerAllocation tracks allocation for controller tasks
	ControllerAllocation
	// TotalAllocation tracks the allocation of all tasks(
	// including NonPreemptibleAllocation,PreemptibleAllocation and ControllerAllocation)
	TotalAllocation
	// SlackAllocation track allocation for tasks launched using slack resources.
	SlackAllocation
	// NonSlackAllocation track allocation for non-revocable tasks.
	NonSlackAllocation
)

// Allocation is the container to track allocation across different dimensions
type Allocation struct {
	Value map[AllocationType]*Resources
}

// NewAllocation returns a new Allocation
func NewAllocation() *Allocation {
	return initializeZeroAlloc()
}

// GetByType returns the allocation by type
func (a *Allocation) GetByType(allocationType AllocationType) *Resources {
	return a.Value[allocationType]
}

// Add adds one allocation to another
func (a *Allocation) Add(other *Allocation) *Allocation {
	result := initializeZeroAlloc()
	for t, v := range a.Value {
		result.Value[t] = v.Add(other.Value[t])
	}
	return result
}

// Subtract subtracts one allocation to another
func (a *Allocation) Subtract(other *Allocation) *Allocation {
	result := initializeZeroAlloc()
	for t, v := range a.Value {
		result.Value[t] = v.Subtract(other.Value[t])
	}
	return result
}

// initializeZeroAlloc initializes a zero alloc
func initializeZeroAlloc() *Allocation {
	alloc := &Allocation{
		Value: make(map[AllocationType]*Resources),
	}

	alloc.Value[TotalAllocation] = ZeroResource
	alloc.Value[NonPreemptibleAllocation] = ZeroResource
	alloc.Value[ControllerAllocation] = ZeroResource
	alloc.Value[PreemptibleAllocation] = ZeroResource
	alloc.Value[SlackAllocation] = ZeroResource
	alloc.Value[NonSlackAllocation] = ZeroResource

	return alloc
}

// GetGangAllocation returns the allocation across different dimensions of
// all tasks in a gang
func GetGangAllocation(gang *resmgrsvc.Gang) *Allocation {
	gangAllocation := initializeZeroAlloc()

	for _, t := range gang.GetTasks() {
		gangAllocation = gangAllocation.Add(GetTaskAllocation(t))
	}
	return gangAllocation
}

// GetTaskAllocation returns the allocation across different dimensions of a
// task
func GetTaskAllocation(rmTask *resmgr.Task) *Allocation {
	alloc := initializeZeroAlloc()

	taskResource := ConvertToResmgrResource(rmTask.Resource)

	// check if the task is non-preemptible
	if rmTask.GetPreemptible() {
		alloc.Value[PreemptibleAllocation] = taskResource
	} else {
		alloc.Value[NonPreemptibleAllocation] = taskResource
	}

	// check if its a controller task
	if rmTask.GetController() {
		alloc.Value[ControllerAllocation] = taskResource
	}

	if rmTask.GetRevocable() {
		alloc.Value[SlackAllocation] = taskResource
	} else {
		alloc.Value[NonSlackAllocation] = taskResource
	}

	// every task account for total allocation
	alloc.Value[TotalAllocation] = taskResource

	return alloc
}

// ZeroResource represents the minimum Value of a resource
var ZeroResource = &Resources{
	CPU:    float64(0),
	GPU:    float64(0),
	DISK:   float64(0),
	MEMORY: float64(0),
}

// Resources is a non-thread safe helper struct holding recognized resources.
type Resources struct {
	CPU    float64
	MEMORY float64
	DISK   float64
	GPU    float64
}

// GetCPU returns the CPU resource
func (r *Resources) GetCPU() float64 {
	return r.CPU
}

// GetDisk returns the disk resource
func (r *Resources) GetDisk() float64 {
	return r.DISK
}

// GetMem returns the memory resource
func (r *Resources) GetMem() float64 {
	return r.MEMORY
}

// GetGPU returns the GPU resource
func (r *Resources) GetGPU() float64 {
	return r.GPU
}

// Get returns the kind of resource
func (r *Resources) Get(kind string) float64 {
	switch kind {
	case common.CPU:
		return r.GetCPU()
	case common.GPU:
		return r.GetGPU()
	case common.MEMORY:
		return r.GetMem()
	case common.DISK:
		return r.GetDisk()
	}
	return float64(0)
}

// Set sets the kind of resource with the Value
func (r *Resources) Set(kind string, value float64) {
	switch kind {
	case common.CPU:
		r.CPU = value
	case common.GPU:
		r.GPU = value
	case common.MEMORY:
		r.MEMORY = value
	case common.DISK:
		r.DISK = value
	}
}

// Add atomically add another scalar resources onto current one.
func (r *Resources) Add(other *Resources) *Resources {
	return &Resources{
		CPU:    r.CPU + other.CPU,
		MEMORY: r.MEMORY + other.MEMORY,
		DISK:   r.DISK + other.DISK,
		GPU:    r.GPU + other.GPU,
	}
}

func lessThanOrEqual(f1, f2 float64) bool {
	v := f1 - f2
	if math.Abs(v) < util.ResourceEpsilon {
		return true
	}
	return v < 0
}

// LessThanOrEqual determines current Resources is less than or equal
// the other one.
func (r *Resources) LessThanOrEqual(other *Resources) bool {
	return lessThanOrEqual(r.CPU, other.CPU) &&
		lessThanOrEqual(r.MEMORY, other.MEMORY) &&
		lessThanOrEqual(r.DISK, other.DISK) &&
		lessThanOrEqual(r.GPU, other.GPU)
}

func equal(f1, f2 float64) bool {
	return f1 == f2
}

// Equal determines current Resources is equal to
// the other one.
func (r *Resources) Equal(other *Resources) bool {
	return equal(r.CPU, other.CPU) &&
		equal(r.MEMORY, other.MEMORY) &&
		equal(r.DISK, other.DISK) &&
		equal(r.GPU, other.GPU)
}

// ConvertToResmgrResource converts task resource config to scalar.Resources
func ConvertToResmgrResource(resource *task.ResourceConfig) *Resources {
	return &Resources{
		CPU:    resource.GetCpuLimit(),
		DISK:   resource.GetDiskLimitMb(),
		GPU:    resource.GetGpuLimit(),
		MEMORY: resource.GetMemLimitMb(),
	}
}

// GetGangResources aggregates gang resources to resmgr resources
func GetGangResources(gang *resmgrsvc.Gang) *Resources {
	if gang == nil {
		return nil
	}
	totalRes := &Resources{}
	for _, task := range gang.GetTasks() {
		totalRes = totalRes.Add(
			ConvertToResmgrResource(task.GetResource()))
	}
	return totalRes
}

func (r *Resources) String() string {
	return fmt.Sprintf("CPU:%.2f MEM:%.2f DISK:%.2f GPU:%.2f",
		r.GetCPU(), r.GetMem(), r.GetDisk(), r.GetGPU())
}

// Min Gets the minimum value for each resource type
func Min(r1, r2 *Resources) *Resources {
	return &Resources{
		CPU:    math.Min(r1.GetCPU(), r2.GetCPU()),
		MEMORY: math.Min(r1.GetMem(), r2.GetMem()),
		DISK:   math.Min(r1.GetDisk(), r2.GetDisk()),
		GPU:    math.Min(r1.GetGPU(), r2.GetGPU()),
	}
}

// Subtract another scalar resources from current one and return a new copy of result.
func (r *Resources) Subtract(other *Resources) *Resources {
	var result Resources
	if r.CPU < other.CPU {
		log.WithFields(log.Fields{
			"from_cpu ": r.CPU,
			"value_cpu": other.CPU,
		}).Debug("Subtracted Value is Greater")
		result.CPU = float64(0)
	} else {
		result.CPU = r.CPU - other.CPU
		if result.CPU < util.ResourceEpsilon {
			result.CPU = float64(0)
		}
	}

	if r.GPU < other.GPU {
		log.WithFields(log.Fields{
			"from_gpu ": r.GPU,
			"value_gpu": other.GPU,
		}).Debug("Subtracted Value is Greater")
		result.GPU = float64(0)
	} else {
		result.GPU = r.GPU - other.GPU
		if result.GPU < util.ResourceEpsilon {
			result.GPU = float64(0)
		}
	}

	if r.MEMORY < other.MEMORY {
		log.WithFields(log.Fields{
			"from_memory ": r.MEMORY,
			"value_memory": other.MEMORY,
		}).Debug("Subtracted Value is Greater")
		result.MEMORY = float64(0)
	} else {
		result.MEMORY = r.MEMORY - other.MEMORY
		if result.MEMORY < util.ResourceEpsilon {
			result.MEMORY = float64(0)
		}
	}

	if r.DISK < other.DISK {
		log.WithFields(log.Fields{
			"from_disk":  r.DISK,
			"value_disk": other.DISK,
		}).Debug("Subtracted Value is Greater")
		result.DISK = float64(0)
	} else {
		result.DISK = r.DISK - other.DISK
		if result.DISK < util.ResourceEpsilon {
			result.DISK = float64(0)
		}
	}
	return &result
}

// Clone creates the new new object of the resources
// and copies the values to the new object and return
// the new object
func (r *Resources) Clone() *Resources {
	return &Resources{
		CPU:    r.CPU,
		DISK:   r.DISK,
		MEMORY: r.MEMORY,
		GPU:    r.GPU,
	}
}

// Copy copies the values from the passed resource object
// to the calling object
func (r *Resources) Copy(other *Resources) {
	r.CPU = other.CPU
	r.DISK = other.DISK
	r.MEMORY = other.MEMORY
	r.GPU = other.GPU
}
