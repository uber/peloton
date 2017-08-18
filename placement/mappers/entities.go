package mappers

import (
	"fmt"
	"strings"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/orderings"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"code.uber.internal/infra/peloton/mimir-lib/model/requirements"
)

// TaskToEntity will convert a task to an entity.
func TaskToEntity(task *resmgr.Task) (*placement.Entity, error) {
	entity := placement.NewEntity(fmt.Sprintf("%v-%v", task.Name, task.Id.Value))
	addMetrics(task.Resource, entity.Metrics)
	for _, label := range task.Labels.Labels {
		entity.Relations.Add(makeLabel(*label.Key, *label.Value))
	}
	entity.Ordering = orderings.NewCustomOrdering(
		orderings.Concatenate(
			orderings.Negate(orderings.Metric(orderings.GroupSource, metrics.DiskFree)),
			orderings.Negate(orderings.Metric(orderings.GroupSource, metrics.MemoryFree)),
			orderings.Negate(orderings.Metric(orderings.GroupSource, metrics.FileDescriptorsFree)),
			orderings.Negate(orderings.Metric(orderings.GroupSource, metrics.CPUFree)),
			orderings.Negate(orderings.Metric(orderings.GroupSource, metrics.GPUFree)),
		),
	)
	entity.MetricRequirements = makeMetricRequirements(task.Resource)
	var err error
	entity.AffinityRequirement, err = makeAffinityRequirements(task.Constraint)
	if err != nil {
		return nil, err
	}
	return entity, nil
}

func makeComparison(comparison task.LabelConstraint_Condition) (requirements.Comparison, error) {
	switch comparison {
	case task.LabelConstraint_CONDITION_LESS_THAN:
		return requirements.LessThan, nil
	case task.LabelConstraint_CONDITION_EQUAL:
		return requirements.Equal, nil
	case task.LabelConstraint_CONDITION_GREATER_THAN:
		return requirements.GreaterThan, nil
	default:
		return requirements.Comparison("unknown"), fmt.Errorf("unknown constraint condition %v", comparison)
	}
}

func makeLabel(key, value string) *labels.Label {
	return labels.NewLabel(strings.Split(key+"."+value, ".")...)
}

func makeAffinityRequirements(constraint *task.Constraint) (requirements.AffinityRequirement, error) {
	switch constraint.Type {
	case task.Constraint_LABEL_CONSTRAINT:
		switch constraint.LabelConstraint.Kind {
		case task.LabelConstraint_TASK, task.LabelConstraint_HOST:
			appliesTo := labels.NewLabel("offer", "*")
			labelRelation := makeLabel(
				constraint.LabelConstraint.Label.Key, constraint.LabelConstraint.Label.Value)
			comparison, err := makeComparison(constraint.LabelConstraint.Condition)
			if err != nil {
				return nil, err
			}
			if constraint.LabelConstraint.Kind == task.LabelConstraint_TASK {
				return requirements.NewRelationRequirement(
					appliesTo, labelRelation, comparison, int(constraint.LabelConstraint.Requirement)), nil
			}
			return requirements.NewLabelRequirement(
				appliesTo, labelRelation, comparison, int(constraint.LabelConstraint.Requirement)), nil
		default:
			return nil, fmt.Errorf("unknown relation constraint kind %v", constraint.LabelConstraint.Kind)
		}
	case task.Constraint_AND_CONSTRAINT:
		subRequirements := []requirements.AffinityRequirement{}
		for _, subConstraint := range constraint.AndConstraint.Constraints {
			subRequirement, err := makeAffinityRequirements(subConstraint)
			if err != nil {
				return nil, fmt.Errorf("unexpected error when transforming and-constraint: %v", err)
			}
			subRequirements = append(subRequirements, subRequirement)
		}
		return requirements.NewAndRequirement(subRequirements...), nil
	case task.Constraint_OR_CONSTRAINT:
		subRequirements := []requirements.AffinityRequirement{}
		for _, subConstraint := range constraint.OrConstraint.Constraints {
			subRequirement, err := makeAffinityRequirements(subConstraint)
			if err != nil {
				return nil, fmt.Errorf("unexpected error when transforming or-constraint: %v", err)
			}
			subRequirements = append(subRequirements, subRequirement)
		}
		return requirements.NewOrRequirement(subRequirements...), nil
	default:
		return nil, fmt.Errorf("unknown constraint type %v", constraint.Type)
	}
}

func makeMetricRequirements(resources *task.ResourceConfig) []*requirements.MetricRequirement {
	cpuRequirement := requirements.NewMetricRequirement(
		metrics.CPUFree, requirements.GreaterThanEqual, resources.CpuLimit)
	memoryRequirement := requirements.NewMetricRequirement(
		metrics.MemoryFree, requirements.GreaterThanEqual, resources.MemLimitMb*metrics.MiB)
	diskRequirement := requirements.NewMetricRequirement(
		metrics.DiskFree, requirements.GreaterThanEqual, resources.DiskLimitMb*metrics.MiB)
	fdRequirement := requirements.NewMetricRequirement(
		metrics.FileDescriptorsFree, requirements.GreaterThanEqual, float64(resources.FdLimit))
	gpuRequirement := requirements.NewMetricRequirement(
		metrics.GPUFree, requirements.GreaterThanEqual, resources.GpuLimit)
	return []*requirements.MetricRequirement{
		cpuRequirement, memoryRequirement, diskRequirement, fdRequirement, gpuRequirement,
	}
}

func addMetrics(resources *task.ResourceConfig, metricSet *metrics.MetricSet) {
	metricSet.Set(metrics.CPUUsed, resources.CpuLimit)
	metricSet.Set(metrics.MemoryUsed, resources.MemLimitMb*metrics.MiB)
	metricSet.Set(metrics.DiskUsed, resources.DiskLimitMb*metrics.MiB)
	metricSet.Set(metrics.FileDescriptorsUsed, float64(resources.FdLimit))
	metricSet.Set(metrics.GPUUsed, resources.GpuLimit)
}
