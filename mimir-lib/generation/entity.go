// @generated AUTO GENERATED - DO NOT EDIT! 9f8b9e47d86b5e1a3668856830c149e768e78415
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

package generation

import (
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/orderings"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"code.uber.internal/infra/peloton/mimir-lib/model/requirements"
)

// EntityBuilder is used to generate new entities for use in tests and benchmarks.
type EntityBuilder interface {
	// Name will use the Label.String() method value of the generated value from the label builder.
	Name(template labels.LabelTemplate) EntityBuilder

	// Ordering will use the given custom to create an ordering.
	Ordering(ordering orderings.Custom) EntityBuilder

	// AddRelation will add a relation generated from the given label builder,
	AddRelation(template labels.LabelTemplate) EntityBuilder

	// AddMetric will add the metric generated from the given distribution.
	AddMetric(metricType metrics.MetricType, distribution Distribution) EntityBuilder

	// Requirement will set the affinity requirement builder to the given builder.
	Requirement(builder RequirementBuilder) EntityBuilder

	// Generate will generate an entity that depends on the random source and the time.
	Generate(random Random, time time.Duration) *placement.Entity
}

// NewEntityBuilder will create a new entity builder for generating entities.
func NewEntityBuilder() EntityBuilder {
	custom := orderings.Negate(orderings.Metric(orderings.GroupSource, metrics.DiskFree))
	return &entityBuilder{
		name:        labels.NewLabelTemplate(),
		custom:      custom,
		relations:   map[labels.LabelTemplate]struct{}{},
		metrics:     map[metrics.MetricType]Distribution{},
		requirement: NewAndRequirementBuilder(),
	}
}

type entityBuilder struct {
	name        labels.LabelTemplate
	custom      orderings.Custom
	relations   map[labels.LabelTemplate]struct{}
	metrics     map[metrics.MetricType]Distribution
	requirement RequirementBuilder
}

func (builder *entityBuilder) Name(template labels.LabelTemplate) EntityBuilder {
	builder.name = template
	return builder
}

func (builder *entityBuilder) Ordering(ordering orderings.Custom) EntityBuilder {
	builder.custom = ordering
	return builder
}

func (builder *entityBuilder) AddRelation(template labels.LabelTemplate) EntityBuilder {
	builder.relations[template] = struct{}{}
	return builder
}

func (builder *entityBuilder) AddMetric(metricType metrics.MetricType, distribution Distribution) EntityBuilder {
	builder.metrics[metricType] = distribution
	return builder
}

func (builder *entityBuilder) Requirement(affinityBuilder RequirementBuilder) EntityBuilder {
	builder.requirement = affinityBuilder
	return builder
}

func (builder *entityBuilder) Generate(random Random, time time.Duration) *placement.Entity {
	result := placement.NewEntity(builder.name.Instantiate().String())
	result.Ordering = orderings.NewCustomOrdering(builder.custom)
	result.Requirement = builder.requirement.Generate(random, time)
	for relation := range builder.relations {
		result.Relations.Add(relation.Instantiate())
	}
	for metric, distribution := range builder.metrics {
		result.Metrics.Add(metric, distribution.Value(random, time))
	}
	return result
}

// CreateSchemalessEntityBuilder creates an entity builder for creating entities representing Schemaless databases.
func CreateSchemalessEntityBuilder() (builder EntityBuilder, variables labels.TemplateSet) {
	builder = NewEntityBuilder()
	variables = labels.NewTemplateSet()
	nameTemplate := labels.NewLabelTemplate(fmt.Sprintf("%v-us1-cluster%v-db%v",
		Instance.Variable(), Cluster.Variable(), Database.Variable()))
	variables.Add(nameTemplate)
	scopeTemplate := labels.NewLabelTemplate("host", "*")
	datacenterTemplate := labels.NewLabelTemplate(Datacenter.Name(), Datacenter.Variable())
	variables.Add(datacenterTemplate)
	instanceRelationTemplate := labels.NewLabelTemplate("schemaless", "instance", Instance.Variable())
	variables.Add(instanceRelationTemplate)
	clusterRelationTemplate := labels.NewLabelTemplate(
		"schemaless", "cluster", fmt.Sprintf("percona-cluster-%v-us1-db%v",
			Instance.Variable(), Cluster.Variable()))
	variables.Add(clusterRelationTemplate)
	issueLabelTemplate := labels.NewLabelTemplate("issue", "*")
	volumeLocalTemplate := labels.NewLabelTemplate(VolumeType.Name(), "local")
	volumeZFSTemplate := labels.NewLabelTemplate(VolumeType.Name(), "zfs")
	diskDistribution := NewConstantGaussian(2.2*metrics.TiB, 0.5*metrics.TiB)
	memoryDistribution := NewConstantGaussian(64*metrics.GiB, 0)
	custom := orderings.Negate(orderings.Metric(orderings.GroupSource, metrics.DiskFree))
	requirementBuilder := NewAndRequirementBuilder(
		NewMetricRequirementBuilder(metrics.DiskFree, requirements.GreaterThanEqual, diskDistribution),
		NewMetricRequirementBuilder(metrics.MemoryFree, requirements.GreaterThanEqual, memoryDistribution),
		NewLabelRequirementBuilder(scopeTemplate, datacenterTemplate, requirements.Equal, 1),
		NewLabelRequirementBuilder(nil, issueLabelTemplate, requirements.LessThanEqual, 0),
		NewOrRequirementBuilder(
			NewLabelRequirementBuilder(nil, volumeLocalTemplate, requirements.GreaterThanEqual, 1),
			NewLabelRequirementBuilder(nil, volumeZFSTemplate, requirements.GreaterThanEqual, 1),
		),
		NewRelationRequirementBuilder(scopeTemplate, instanceRelationTemplate, requirements.LessThanEqual, 0),
	)
	builder.Name(nameTemplate).
		Ordering(custom).
		Requirement(requirementBuilder).
		AddRelation(instanceRelationTemplate).
		AddRelation(clusterRelationTemplate).
		AddMetric(metrics.DiskUsed, diskDistribution).
		AddMetric(metrics.MemoryUsed, memoryDistribution)
	return
}

// CreateSchemalessEntities will create a list of entities that represents the databases for all the clusters of a
// Schemaless instance.
func CreateSchemalessEntities(random Random, builder EntityBuilder, templates labels.TemplateSet,
	clusters, perCluster int) []*placement.Entity {
	entities := []*placement.Entity{}
	for cluster := 1; cluster <= clusters; cluster++ {
		for database := 1; database <= perCluster; database++ {
			templates.
				Bind(Cluster.Name(), fmt.Sprintf("%v", cluster)).
				Bind(Database.Name(), fmt.Sprintf("%v", database))
			entities = append(entities, builder.Generate(random, time.Duration(cluster*perCluster+database)))
		}
	}
	return entities
}
