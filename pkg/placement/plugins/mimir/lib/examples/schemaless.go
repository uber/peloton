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

package examples

import (
	"fmt"
	"time"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation/orderings"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation/placement"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation/requirements"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	source "github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/orderings"
	mPlacement "github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
	mRequirements "github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/requirements"
)

// CreateSchemalessEntityBuilder creates an entity builder for creating entities representing Schemaless databases.
func CreateSchemalessEntityBuilder() (builder placement.EntityBuilder, variables labels.TemplateSet) {
	builder = placement.NewEntityBuilder()
	variables = labels.NewTemplateSet()
	nameTemplate := labels.NewTemplate(fmt.Sprintf("%v-us1-cluster%v-db%v",
		Instance.Variable(), Cluster.Variable(), Database.Variable()))
	variables.Add(nameTemplate)
	scopeTemplate := labels.NewTemplate("host", "*")
	datacenterTemplate := labels.NewTemplate(Datacenter.Name(), Datacenter.Variable())
	variables.Add(datacenterTemplate)
	instanceRelationTemplate := labels.NewTemplate("schemaless", "instance", Instance.Variable())
	variables.Add(instanceRelationTemplate)
	clusterRelationTemplate := labels.NewTemplate(
		"schemaless", "cluster", fmt.Sprintf("percona-cluster-%v-us1-db%v",
			Instance.Variable(), Cluster.Variable()))
	variables.Add(clusterRelationTemplate)
	issueLabelTemplate := labels.NewTemplate("issue", "*")
	volumeLocalTemplate := labels.NewTemplate(VolumeType.Name(), "local")
	volumeZFSTemplate := labels.NewTemplate(VolumeType.Name(), "zfs")
	diskDistribution := generation.NewConstantGaussian(2.2*metrics.TiB, 0)
	memoryDistribution := generation.NewConstantGaussian(64*metrics.GiB, 0)
	orderingBuilder := orderings.NewOrderingBuilder(orderings.Negate(orderings.Metric(source.GroupSource, metrics.DiskFree)))
	requirementBuilder := requirements.NewAndRequirementBuilder(
		requirements.NewMetricRequirementBuilder(metrics.DiskFree, mRequirements.GreaterThanEqual, diskDistribution),
		requirements.NewMetricRequirementBuilder(metrics.MemoryFree, mRequirements.GreaterThanEqual, memoryDistribution),
		requirements.NewLabelRequirementBuilder(scopeTemplate, datacenterTemplate, mRequirements.Equal, 1),
		requirements.NewLabelRequirementBuilder(nil, issueLabelTemplate, mRequirements.LessThanEqual, 0),
		requirements.NewOrRequirementBuilder(
			requirements.NewLabelRequirementBuilder(nil, volumeLocalTemplate, mRequirements.GreaterThanEqual, 1),
			requirements.NewLabelRequirementBuilder(nil, volumeZFSTemplate, mRequirements.GreaterThanEqual, 1),
		),
		requirements.NewRelationRequirementBuilder(scopeTemplate, instanceRelationTemplate, mRequirements.LessThanEqual, 0),
	)
	builder.Name(nameTemplate).
		Ordering(orderingBuilder).
		Requirement(requirementBuilder).
		AddRelation(instanceRelationTemplate).
		AddRelation(clusterRelationTemplate).
		AddMetric(metrics.DiskUsed, diskDistribution).
		AddMetric(metrics.MemoryUsed, memoryDistribution)
	return
}

// CreateSchemalessEntities will create a list of entities that represents the databases for all the clusters of a
// Schemaless instance.
func CreateSchemalessEntities(random generation.Random, builder placement.EntityBuilder, templates labels.TemplateSet,
	clusters, perCluster int) []*mPlacement.Entity {
	var entities []*mPlacement.Entity
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

// CreateHostGroupsBuilder will create a builder to generate groups representing hosts that belong to a rack in a
// datacenter.
func CreateHostGroupsBuilder() (builder placement.GroupBuilder, templates labels.TemplateSet) {
	builder = placement.NewGroupBuilder()
	templates = labels.NewTemplateSet()
	nameFormat := fmt.Sprintf("schemadock%v-%v", Host.Variable(), Datacenter.Variable())
	nameTemplate := labels.NewTemplate(nameFormat)
	templates.Add(nameTemplate)
	hostTemplate := labels.NewTemplate(Host.Name(), nameFormat)
	templates.Add(hostTemplate)
	rackTemplate := labels.NewTemplate(Rack.Name(), Rack.Variable())
	templates.Add(rackTemplate)
	datacenterTemplate := labels.NewTemplate(Datacenter.Name(), Datacenter.Variable())
	templates.Add(datacenterTemplate)
	volumeTemplate := labels.NewTemplate(VolumeType.Name(), "local")
	memoryDistribution := generation.NewDiscrete(map[float64]float64{128 * metrics.GiB: 1, 256 * metrics.GiB: 5})
	builder.Name(nameTemplate).
		AddLabel(hostTemplate).
		AddLabel(rackTemplate).
		AddLabel(datacenterTemplate).
		AddLabel(volumeTemplate).
		AddMetric(metrics.DiskTotal, generation.NewUniformDiscrete(6*metrics.TiB)).
		AddMetric(metrics.DiskFree, generation.NewUniformDiscrete(6*metrics.TiB)).
		AddMetric(metrics.MemoryTotal, memoryDistribution).
		AddMetric(metrics.MemoryFree, memoryDistribution).
		AddMetric(metrics.DiskUsed, generation.NewUniformDiscrete(0)).
		AddMetric(metrics.MemoryUsed, generation.NewUniformDiscrete(0))
	return
}

// CreateHostGroups will create a given number of groups representing hosts distributed over a given number of racks
// and all belonging to the same datacenter.
func CreateHostGroups(random generation.Random, builder placement.GroupBuilder, templates labels.TemplateSet, racks,
	hosts int) []*mPlacement.Group {
	var groups []*mPlacement.Group
	for i := 0; i < hosts; i++ {
		datacenter := templates.Mappings()[Datacenter.Name()]
		templates.Bind(Rack.Name(), fmt.Sprintf("%v-a%v", datacenter, i%racks))
		templates.Bind(Host.Name(), fmt.Sprintf("%v", i))
		group := builder.Generate(random, time.Duration(i))
		group.Metrics.Update()
		groups = append(groups, group)
	}
	return groups
}
