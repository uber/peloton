package mimir

import (
	"strings"

	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/orderings"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"code.uber.internal/infra/peloton/mimir-lib/model/requirements"
)

// TaskToEntity will convert a task to an entity.
func TaskToEntity(task *resmgr.Task, isLaunched bool) *placement.Entity {
	entity := placement.NewEntity(task.GetId().GetValue())
	if !isLaunched {
		addMetrics(task, entity.Metrics)
	}
	addRelations(task.GetLabels(), entity.Relations)
	entity.Ordering = orderings.NewCustomOrdering(
		orderings.Concatenate(
			orderings.Negate(orderings.Metric(orderings.GroupSource, DiskFree)),
			orderings.Negate(orderings.Metric(orderings.GroupSource, MemoryFree)),
			orderings.Negate(orderings.Metric(orderings.GroupSource, CPUFree)),
			orderings.Negate(orderings.Metric(orderings.GroupSource, GPUFree)),
		),
	)
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
		CPUFree, requirements.GreaterThanEqual, resource.GetCpuLimit()*100.0)
	memoryRequirement := requirements.NewMetricRequirement(
		MemoryFree, requirements.GreaterThanEqual, resource.GetMemLimitMb()*metrics.MiB)
	diskRequirement := requirements.NewMetricRequirement(
		DiskFree, requirements.GreaterThanEqual, resource.GetDiskLimitMb()*metrics.MiB)
	gpuRequirement := requirements.NewMetricRequirement(
		GPUFree, requirements.GreaterThanEqual, resource.GetGpuLimit()*100.0)
	portRequirement := requirements.NewMetricRequirement(
		PortsFree, requirements.GreaterThanEqual, float64(task.GetNumPorts()))
	return []placement.Requirement{
		cpuRequirement, memoryRequirement, diskRequirement, gpuRequirement, portRequirement,
	}
}

func addRelations(labels *mesos_v1.Labels, relations *labels.LabelBag) {
	for _, label := range labels.GetLabels() {
		log.WithField("label", label.String()).Debug("Adding relation label")
		relations.Add(makeLabel(label.GetKey(), label.GetValue()))
	}
}

func addMetrics(task *resmgr.Task, metricSet *metrics.MetricSet) {
	resource := task.GetResource()
	metricSet.Set(CPUReserved, resource.GetCpuLimit()*100.0)
	metricSet.Set(GPUReserved, resource.GetGpuLimit()*100)
	metricSet.Set(MemoryReserved, resource.GetMemLimitMb()*metrics.MiB)
	metricSet.Set(DiskReserved, resource.GetDiskLimitMb()*metrics.MiB)
	metricSet.Set(PortsReserved, float64(task.GetNumPorts()))
}
