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

package impl

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"

	"github.com/uber-go/tally"
	"github.com/uber/peloton/pkg/storage/cassandra/api"
	qb "github.com/uber/peloton/pkg/storage/querybuilder"
)

// Store represents connections with Cassandra server nodes.
// the store object is used by multiple go routines.
// concurrency represents number of active go routines using the store.
type Store struct {
	cSession       *gocql.Session
	keySpace       string
	closeMu        sync.RWMutex
	scope          tally.Scope
	concurrency    int32
	maxBatch       int
	maxConcurrency int32
	metrics        Metrics
}

// Metrics is a struct for tracking execute statement / executeBatch statements
// failure / success counters
type Metrics struct {
	ExecuteSuccess tally.Counter
	ExecuteFail    tally.Counter

	ExecuteBatchSuccess tally.Counter
	ExecuteBatchFail    tally.Counter
}

// NewMetrics function creates a Metrics struct
func NewMetrics(scope tally.Scope) Metrics {

	executeScope := scope.SubScope(executeName)
	executeSuccessScope := executeScope.Tagged(map[string]string{"result": "success"})
	executeFailScope := executeScope.Tagged(map[string]string{"result": "fail"})

	executeBatchScope := scope.SubScope(executeBatchName)
	executeBatchSuccessScope := executeBatchScope.Tagged(map[string]string{"result": "success"})
	executeBatchFailScope := executeBatchScope.Tagged(map[string]string{"result": "fail"})

	metrics := Metrics{
		ExecuteSuccess: executeSuccessScope.Counter(executeName),
		ExecuteFail:    executeFailScope.Counter(executeName),

		ExecuteBatchSuccess: executeBatchSuccessScope.Counter(executeBatchName),
		ExecuteBatchFail:    executeBatchFailScope.Counter(executeBatchName),
	}
	return metrics
}

const (
	executeName      = "execute"
	executeBatchName = "executeBatch"
)

// uuidIn UUID conversion, input: qb.UUID, output: gocql.UUID
func (s *Store) convertUUID(args []interface{}) {
	for i, arg := range args {
		if qb.IsUUID(arg) {
			args[i] = arg.(qb.UUID).UUID
		} else {
			qbUUIDs, isQbUUIDArray := arg.([]qb.UUID)
			if isQbUUIDArray && len(qbUUIDs) > 0 && qb.IsUUID(qbUUIDs[0]) {
				var UUIDs []gocql.UUID
				for _, subarg := range qbUUIDs {
					UUIDs = append(UUIDs, subarg.UUID)
				}
				args[i] = UUIDs
			}
		}
	}
}

func (s *Store) createExecutor(stmt api.Statement) (Executor, error) {
	var executor Executor
	if stmt.StmtType() == qb.SelectStmtType {
		executor = ReadExecutor{
			ExecutorBase: ExecutorBase{s: s},
		}
	} else {
		executor = WriteExecutor{
			ExecutorBase: ExecutorBase{s: s},
		}
		if stmt.IsCAS() { // TODO: performance
			executor = CASExecutor{WriteExecutor: executor.(WriteExecutor)}
		}
	}
	return executor, nil
}

// Execute a query and return a ResultSet
func (s *Store) Execute(ctx context.Context, stmt api.Statement) (api.ResultSet, error) {
	executor, err := s.createExecutor(stmt)
	if err != nil {
		return nil, err
	}

	// query execution
	var rs api.ResultSet
	f := func() error {
		var err error
		rs, err = executor.Execute(ctx, stmt)
		return err
	}
	err = f()
	if err == nil {
		s.metrics.ExecuteSuccess.Inc(1)
	} else {
		s.metrics.ExecuteFail.Inc(1)
	}

	return rs, err
}

// ExecuteBatch make a single RPC call for multiple statement execution.
// It ensures all statements are eventually executed
func (s *Store) ExecuteBatch(ctx context.Context, stmts []api.Statement) error {
	var executor Executor
	executor = WriteExecutor{
		ExecutorBase: ExecutorBase{s: s},
	}

	_, err := executor.ExecuteBatch(ctx, stmts)
	if err == nil {
		s.metrics.ExecuteBatchSuccess.Inc(1)
	} else {
		s.metrics.ExecuteBatchFail.Inc(1)
	}
	return err
}

// NewQuery creates a QueryBuilder object
func (s *Store) NewQuery() api.QueryBuilder {
	return &QueryBuilder{}
}

// NewEntity creates a Table object
func (s *Store) NewEntity() api.Table {
	return nil
}

// Name returns the name of this datastore
func (s *Store) Name() string {
	return s.keySpace
}

// close ends the session
func (s *Store) close() {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	s.cSession.Close()
	log.WithField("store", s.String()).Info("store closed")
}

func (s *Store) isClosed() bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	return s.cSession.Closed()
}

func (s *Store) sendLatency(ctx context.Context, name string, d time.Duration) {
	if s.scope == nil {
		return
	}
	sc := s.scope
	if tags, ok := tagsFromContext(ctx); ok {
		sc = sc.Tagged(tags)
	}
	sc.Timer(name).Record(d)
}

// String returns a string representation of the store object
func (s *Store) String() string {
	return fmt.Sprintf("Cassandra.Store[%s]", s.keySpace)
}
