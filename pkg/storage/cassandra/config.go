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

package cassandra

import "github.com/uber/peloton/pkg/storage/cassandra/impl"

// Replica is the config for Cassandra replicas
type Replica struct {
	// Name of the replica config, i.e. replication_factor for
	// SimpleStategy or datacenter1 for NetworkTopologyStrategy
	Name string `yaml:"name"`
	// Value of the replica config such as number of replicas
	Value int `yaml:"value"`
}

// Replication is the config for Cassandra replication
type Replication struct {
	// Strategy controls the replication strategy. Only two strategies
	// are supported: SimpleStrategy and NetworkTopologyStrategy
	Strategy string `yaml:"strategy"`
	// Replicas controls the number of replicas of the keyspace. For
	// SimpleStrategy, it is a single replication_factor like 3. For
	// NetworkTopologyStrategy, it will be a list of <datacenter,
	// replicas> pairs like {'dc1': '3', 'dc2': '3'}
	Replicas []*Replica `yaml:"replicas"`
}

// Config is the config for cassandra Store
type Config struct {
	CassandraConn *impl.CassandraConn `yaml:"connection"`
	StoreName     string              `yaml:"store_name"`
	Migrations    string              `yaml:"migrations"`
	// MaxParallelBatches controls the maximum number of go routines run to create tasks
	MaxParallelBatches int `yaml:"max_parallel_batches"`
	// MaxUpdatesPerJob controls the maximum number of
	// updates per job kept in the database
	MaxUpdatesPerJob int `yaml:"max_updates_job"`
	// Replication controls the replication config of the keyspace
	Replication *Replication `yaml:"replication"`
}
