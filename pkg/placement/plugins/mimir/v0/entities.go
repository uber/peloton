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

package mimir_v0

import (
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	common "github.com/uber/peloton/pkg/placement/plugins/mimir/common"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/orderings"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/requirements"
)

// TaskToEntity will convert a task to an entity.
func TaskToEntity(task *resmgr.Task, isLaunched bool) *placement.Entity {
	entity := placement.NewEntity(task.GetId().GetValue())
	if !isLaunched {
		addMetrics(task, entity.Metrics)
	}
	addRelations(task.GetLabels(), entity.Relations)

	var order []placement.Ordering
	// if the task has a desired host, add the host as the highest priority when picking group
	if len(task.GetDesiredHost()) != 0 {
		label := labels.NewLabel(common.HostNameLabel, task.DesiredHost)
		order = append(order, orderings.Negate(orderings.Label(nil, label)))
	}

	order = append(order,
		orderings.Negate(orderings.Metric(orderings.GroupSource, common.DiskFree)),
		orderings.Negate(orderings.Metric(orderings.GroupSource, common.MemoryFree)),
		orderings.Negate(orderings.Metric(orderings.GroupSource, common.CPUFree)),
		orderings.Negate(orderings.Metric(orderings.GroupSource, common.GPUFree)),
	)

	entity.Ordering = orderings.Concatenate(order...)

	var req []placement.Requirement
	req = append(req, makeAffinityRequirements(task.GetConstraint()))
	req = append(req, makeMetricRequirements(task)...)
	entity.Requirement = requirements.NewAndRequirement(req...)
	return entity
}

func makeComparison(comparison task.LabelConstraint_Condition) requirements.Comparison {
	switch comparison {
	case task.LabelConstraint_CONDITION_LESS_THAN:
		return requirements.LessThan
	case task.LabelConstraint_CONDITION_EQUAL:
		return requirements.Equal
	case task.LabelConstraint_CONDITION_GREATER_THAN:
		return requirements.GreaterThan
	default:
		log.WithField("condition", comparison).
			Warn("unknown constraint condition")
		return requirements.Equal
	}
}

func makeLabel(key, value string) *labels.Label {
	return labels.NewLabel(append(strings.Split(key, "."), value)...)
}

func makeAffinityRequirements(constraint *task.Constraint) placement.Requirement {
	switch constraint.GetType() {
	case task.Constraint_LABEL_CONSTRAINT:
		kind := constraint.GetLabelConstraint().GetKind()
		labelRelation := makeLabel(
			constraint.GetLabelConstraint().GetLabel().GetKey(),
			constraint.GetLabelConstraint().GetLabel().GetValue())
		comparison := makeComparison(constraint.GetLabelConstraint().GetCondition())
		switch kind {
		case task.LabelConstraint_TASK:
			return requirements.NewRelationRequirement(
				nil, labelRelation, comparison, int(constraint.GetLabelConstraint().GetRequirement()))
		case task.LabelConstraint_HOST:
			return requirements.NewLabelRequirement(
				nil, labelRelation, comparison, int(constraint.GetLabelConstraint().GetRequirement()))
		default:
			log.WithField("kind", kind).
				Warn("unknown relation constraint kind")
			return requirements.NewAndRequirement()
		}
	case task.Constraint_AND_CONSTRAINT:
		var subRequirements []placement.Requirement
		for _, subConstraint := range constraint.GetAndConstraint().GetConstraints() {
			subRequirement := makeAffinityRequirements(subConstraint)
			subRequirements = append(subRequirements, subRequirement)
		}
		return requirements.NewAndRequirement(subRequirements...)
	case task.Constraint_OR_CONSTRAINT:
		var subRequirements []placement.Requirement
		for _, subConstraint := range constraint.GetOrConstraint().GetConstraints() {
			subRequirement := makeAffinityRequirements(subConstraint)
			subRequirements = append(subRequirements, subRequirement)
		}
		return requirements.NewOrRequirement(subRequirements...)
	default:
		if constraint != nil {
			log.WithField("type", constraint.GetType()).
				Warn("unknown constraint type")
		}
		return requirements.NewAndRequirement()
	}
}

func makeMetricRequirements(task *resmgr.Task) []placement.Requirement {
	resource := task.GetResource()
	cpuRequirement := requirements.NewMetricRequirement(
		common.CPUFree, requirements.GreaterThanEqual, resource.GetCpuLimit()*100.0)
	memoryRequirement := requirements.NewMetricRequirement(
		common.MemoryFree, requirements.GreaterThanEqual, resource.GetMemLimitMb()*metrics.MiB)
	diskRequirement := requirements.NewMetricRequirement(
		common.DiskFree, requirements.GreaterThanEqual, resource.GetDiskLimitMb()*metrics.MiB)
	gpuRequirement := requirements.NewMetricRequirement(
		common.GPUFree, requirements.GreaterThanEqual, resource.GetGpuLimit()*100.0)
	portRequirement := requirements.NewMetricRequirement(
		common.PortsFree, requirements.GreaterThanEqual, float64(task.GetNumPorts()))
	return []placement.Requirement{
		cpuRequirement, memoryRequirement, diskRequirement, gpuRequirement, portRequirement,
	}
}

func addRelations(labels *mesos_v1.Labels, relations *labels.Bag) {
	for _, label := range labels.GetLabels() {
		log.WithField("label", label.String()).Debug("Adding relation label")
		relations.Add(makeLabel(label.GetKey(), label.GetValue()))
	}
}

func addMetrics(task *resmgr.Task, metricSet *metrics.Set) {
	resource := task.GetResource()
	metricSet.Set(common.CPUReserved, resource.GetCpuLimit()*100.0)
	metricSet.Set(common.GPUReserved, resource.GetGpuLimit()*100)
	metricSet.Set(common.MemoryReserved, resource.GetMemLimitMb()*metrics.MiB)
	metricSet.Set(common.DiskReserved, resource.GetDiskLimitMb()*metrics.MiB)
	metricSet.Set(common.PortsReserved, float64(task.GetNumPorts()))
}
