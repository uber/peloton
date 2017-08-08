// @generated AUTO GENERATED - DO NOT EDIT!
// Copyright (c) 2017 Uber Technologies, Inc.
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
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/orderings"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"code.uber.internal/infra/peloton/mimir-lib/model/requirements"
	"fmt"
	"math/rand"
	"time"
)

// EntityBuilder is used to generate new entities for use in tests and benchmarks.
type EntityBuilder interface {
	// Name will use the Label.String() method value of the generated value from the label builder.
	Name(template labels.LabelTemplate) EntityBuilder

	// Mapper will use the given change change metric requirements into usage metrics for the entity.
	Mapper(mapper metrics.TypeMapper) EntityBuilder

	// Ordering will use the given custom to create an ordering.
	Ordering(ordering orderings.Custom) EntityBuilder

	// AddRelation will add a relation generated from the given label builder,
	AddRelation(template labels.LabelTemplate) EntityBuilder

	// AddMetricRequirement will add a metric requirement on the given metric type, bound type and value from the
	// given distribution.
	AddMetricRequirement(metricType metrics.MetricType, comparison requirements.Comparison, value Distribution) EntityBuilder

	// AffinityRequirement will set the affinity requirement builder to the given builder.
	AffinityRequirement(builder AffinityRequirementBuilder) EntityBuilder

	// Generate will generate an entity that depends on the random source and the time.
	Generate(random *rand.Rand, time time.Time) *placement.Entity
}

// NewEntityBuilder will create a new entity builder for generating entities.
func NewEntityBuilder() EntityBuilder {
	mapping := map[metrics.MetricType]metrics.MetricType{
		metrics.DiskFree:   metrics.DiskUsed,
		metrics.MemoryFree: metrics.MemoryUsed,
	}
	custom := orderings.Negate(orderings.Metric(orderings.GroupSource, metrics.DiskFree))
	return &entityBuilder{
		name:                labels.NewLabelTemplate(),
		mapper:              metrics.NewTypeMapper(mapping),
		custom:              custom,
		relations:           map[labels.LabelTemplate]struct{}{},
		metricRequirements:  []*metricRequirement{},
		affinityRequirement: NewAndRequirementBuilder(),
	}
}

type entityBuilder struct {
	name                labels.LabelTemplate
	mapper              metrics.TypeMapper
	custom              orderings.Custom
	relations           map[labels.LabelTemplate]struct{}
	metricRequirements  []*metricRequirement
	affinityRequirement AffinityRequirementBuilder
}

type metricRequirement struct {
	metricType metrics.MetricType
	comparison requirements.Comparison
	value      Distribution
}

type labelRequirement struct {
	appliesTo   labels.LabelTemplate
	label       labels.LabelTemplate
	occurrences int
	comparison  requirements.Comparison
}

type relationRequirement struct {
	appliesTo   labels.LabelTemplate
	relation    labels.LabelTemplate
	occurrences int
	comparison  requirements.Comparison
}

func (builder *entityBuilder) Name(template labels.LabelTemplate) EntityBuilder {
	builder.name = template
	return builder
}

func (builder *entityBuilder) Mapper(mapper metrics.TypeMapper) EntityBuilder {
	builder.mapper = mapper
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

func (builder *entityBuilder) AddMetricRequirement(metricType metrics.MetricType,
	comparison requirements.Comparison, value Distribution) EntityBuilder {
	builder.metricRequirements = append(builder.metricRequirements, &metricRequirement{
		metricType: metricType,
		comparison: comparison,
		value:      value,
	})
	return builder
}

func (builder *entityBuilder) AffinityRequirement(affinityBuilder AffinityRequirementBuilder) EntityBuilder {
	builder.affinityRequirement = affinityBuilder
	return builder
}

func (builder *entityBuilder) Generate(random *rand.Rand, time time.Time) *placement.Entity {
	result := placement.NewEntity(builder.name.Instantiate().String())
	result.Ordering = orderings.NewCustomOrdering(builder.custom)
	for relation := range builder.relations {
		result.Relations.Add(relation.Instantiate())
	}
	for _, metricRequirement := range builder.metricRequirements {
		value := metricRequirement.value.Value(random, time)
		result.MetricRequirements = append(result.MetricRequirements, &requirements.MetricRequirement{
			MetricType: metricRequirement.metricType,
			Comparison: metricRequirement.comparison,
			Value:      value,
		})
		result.Metrics.Add(metricRequirement.metricType, value)
	}
	builder.mapper.Map(result.Metrics)
	result.AffinityRequirement = builder.affinityRequirement.Generate(random, time)
	return result
}

// CreateSchemalessEntityBuilder creates an entity builder for creating entities representing Schemaless databases.
func CreateSchemalessEntityBuilder() (builder EntityBuilder, variables labels.TemplateSet) {
	builder = NewEntityBuilder()
	variables = labels.NewTemplateSet()
	nameTemplate := labels.NewLabelTemplate(fmt.Sprintf("%v-us1-cluster%v-db%v",
		Instance.Variable(), Cluster.Variable(), Database.Variable()))
	variables.Add(nameTemplate)
	hostTemplate := labels.NewLabelTemplate("host", "*")
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
	mapping := map[metrics.MetricType]metrics.MetricType{
		metrics.DiskFree:   metrics.DiskUsed,
		metrics.MemoryFree: metrics.MemoryUsed,
	}
	custom := orderings.Negate(orderings.Metric(orderings.GroupSource, metrics.DiskFree))
	affinityBuilder := NewAndRequirementBuilder(
		NewLabelRequirementBuilder(hostTemplate, datacenterTemplate, requirements.Equal, 1),
		NewLabelRequirementBuilder(hostTemplate, issueLabelTemplate, requirements.LessThanEqual, 0),
		NewOrRequirementBuilder(
			NewLabelRequirementBuilder(hostTemplate, volumeLocalTemplate, requirements.GreaterThanEqual, 1),
			NewLabelRequirementBuilder(hostTemplate, volumeZFSTemplate, requirements.GreaterThanEqual, 1),
		),
		NewRelationRequirementBuilder(hostTemplate, instanceRelationTemplate, requirements.LessThanEqual, 0),
	)
	builder.Name(nameTemplate).
		Mapper(metrics.NewTypeMapper(mapping)).
		Ordering(custom).
		AffinityRequirement(affinityBuilder).
		AddRelation(instanceRelationTemplate).
		AddRelation(clusterRelationTemplate).
		AddMetricRequirement(metrics.DiskFree, requirements.GreaterThanEqual, diskDistribution).
		AddMetricRequirement(metrics.MemoryFree, requirements.GreaterThanEqual, memoryDistribution)
	return
}

// CreateSchemalessEntities will create a list of entities that represents the databases for all the clusters of a
// Schemaless instance.
func CreateSchemalessEntities(random *rand.Rand, builder EntityBuilder, templates labels.TemplateSet,
	clusters, perCluster int) []*placement.Entity {
	entities := []*placement.Entity{}
	for cluster := 1; cluster <= clusters; cluster++ {
		for database := 1; database <= perCluster; database++ {
			templates.
				Bind(Cluster.Name(), fmt.Sprintf("%v", cluster)).
				Bind(Database.Name(), fmt.Sprintf("%v", database))
			entities = append(entities, builder.Generate(random, time.Now()))
		}
	}
	return entities
}
