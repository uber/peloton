// @generated AUTO GENERATED - DO NOT EDIT! 117d51fa2854b0184adc875246a35929bbbf0a91

// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricType_SetDerivation(t *testing.T) {
	diskWaste := Type{
		Name: "disk_waste",
		Unit: "bytes",
	}

	assert.Nil(t, diskWaste.Derivation())

	err := diskWaste.SetDerivation(&derivation{
		dependencies: []Type{CPUTotal, CPUUsed, CPUFree},
	})

	assert.NoError(t, err)
	assert.NotNil(t, diskWaste.Derivation())
}

func TestMetricType_SetDerivationGivesErrorOnCyclicDependencies(t *testing.T) {
	err := CPUUsed.SetDerivation(&derivation{
		dependencies: []Type{CPUTotal, CPUFree},
	})

	assert.Error(t, err)
}

func TestMetricType_Derivation(t *testing.T) {
	set := NewSet()
	set.Set(CPUTotal, 1000.0)
	set.Set(CPUUsed, 500.0)
	set.Set(CPUFree, 0.0)

	set.Set(MemoryTotal, 128.0*GiB)
	set.Set(MemoryUsed, 64.0*GiB)
	set.Set(MemoryFree, 0.0)

	set.Set(DiskTotal, 4.0*TiB)
	set.Set(DiskUsed, 1.0*TiB)
	set.Set(DiskFree, 0.0)

	set.Set(NetworkTotal, 1.0*GiBit)
	set.Set(NetworkUsed, 200*MiBit)
	set.Set(NetworkFree, 0.0)

	set.Set(GPUTotal, 2000.0)
	set.Set(GPUUsed, 1000.0)
	set.Set(GPUFree, 0.0)

	set.Set(FileDescriptorsTotal, 10000.0)
	set.Set(FileDescriptorsUsed, 100.0)
	set.Set(FileDescriptorsFree, 0.0)

	set.Set(PortsTotal, 1000.0)
	set.Set(PortsUsed, 2.0)
	set.Set(PortsFree, 0.0)

	set.Update()

	assert.Equal(t, 500.0, set.Get(CPUFree))
	assert.Equal(t, 64*GiB, set.Get(MemoryFree))
	assert.Equal(t, 3*TiB, set.Get(DiskFree))
	assert.Equal(t, 824*MiBit, set.Get(NetworkFree))
	assert.Equal(t, 1000.0, set.Get(GPUFree))
	assert.Equal(t, 9900.0, set.Get(FileDescriptorsFree))
	assert.Equal(t, 998.0, set.Get(PortsFree))
}
