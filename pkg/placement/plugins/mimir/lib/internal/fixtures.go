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

package internal

import (
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// SetupTwoGroupsAndEntity creates two placement groups and an entity for use in tests of all Mimir-Lib packages.
func SetupTwoGroupsAndEntity() (*placement.Group, *placement.Group, []*placement.Group, *placement.Entity) {
	metrics1 := metrics.NewSet()
	metrics1.Set(metrics.MemoryTotal, 128*metrics.GiB)
	metrics1.Set(metrics.MemoryUsed, 32*metrics.GiB)
	metrics1.Set(metrics.MemoryFree, 96*metrics.GiB)
	metrics1.Set(metrics.DiskTotal, 2*metrics.TiB)
	metrics1.Set(metrics.DiskUsed, 1*metrics.TiB)
	metrics1.Set(metrics.DiskFree, 1*metrics.TiB)
	labels1 := labels.NewBag()
	labels1.Add(labels.NewLabel("datacenter", "sjc1"))
	labels1.Add(labels.NewLabel("rack", "sjc1-a0042"))
	relations1 := labels.NewBag()
	relations1.Add(labels.NewLabel("schemaless", "instance", "mezzanine"))
	group1 := &placement.Group{
		Name:      "group1",
		Metrics:   metrics1,
		Labels:    labels1,
		Relations: relations1,
	}

	metrics2 := metrics.NewSet()
	metrics2.Set(metrics.MemoryTotal, 128*metrics.GiB)
	metrics2.Set(metrics.MemoryUsed, 64*metrics.GiB)
	metrics2.Set(metrics.MemoryFree, 64*metrics.GiB)
	metrics2.Set(metrics.DiskTotal, 2*metrics.TiB)
	metrics2.Set(metrics.DiskUsed, 0.5*metrics.TiB)
	metrics2.Set(metrics.DiskFree, 1.5*metrics.TiB)
	labels2 := labels.NewBag()
	labels2.Add(labels.NewLabel("datacenter", "sjc1"))
	labels2.Add(labels.NewLabel("rack", "sjc1-a0084"))
	relations2 := labels.NewBag()
	relations2.Add(labels.NewLabel("schemaless", "instance", "trifle"))
	group2 := &placement.Group{
		Name:      "group2",
		Metrics:   metrics2,
		Labels:    labels2,
		Relations: relations2,
	}

	usage := metrics.NewSet()
	usage.Set(metrics.DiskUsed, 0.5*metrics.TiB)
	entity := &placement.Entity{
		Name:    "entity",
		Metrics: usage,
	}

	return group1, group2, []*placement.Group{group1, group2}, entity
}
