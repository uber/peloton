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

package objects

import (
	pelotonstore "github.com/uber/peloton/pkg/storage"
	"github.com/uber/peloton/pkg/storage/cassandra"
	escassandra "github.com/uber/peloton/pkg/storage/connectors/cassandra"
	"github.com/uber/peloton/pkg/storage/objects/base"
	"github.com/uber/peloton/pkg/storage/orm"

	"github.com/uber-go/tally"
)

// Objs is a global list of storage objects. Every storage object will be added
// using an init method to this list. This list will be used when creating the
// ORM client.
var Objs []base.Object

// Store contains ORM client as well as metrics
type Store struct {
	oClient orm.Client
	metrics *pelotonstore.Metrics
}

// NewCassandraStore creates a new Cassandra storage client
func NewCassandraStore(
	config *cassandra.Config,
	scope tally.Scope,
) (*Store, error) {
	connector, err := escassandra.NewCassandraConnector(config, scope)
	if err != nil {
		return nil, err
	}
	// TODO: Load up all objects automatically instead of explicitly adding
	// them here. Might need to add some Go init() magic to do this.
	oclient, err := orm.NewClient(connector, Objs...)
	if err != nil {
		return nil, err
	}
	return &Store{
		oClient: oclient,
		metrics: pelotonstore.NewMetrics(scope),
	}, nil
}
