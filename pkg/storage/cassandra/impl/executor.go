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

	"github.com/gocql/gocql"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/storage/cassandra/api"
	qb "github.com/uber/peloton/pkg/storage/querybuilder"

	log "github.com/sirupsen/logrus"
)

// Executor interface. execution sequence is Allowed->{Execute, ExecuteBatch}->PostExecute
type Executor interface {
	Execute(ctx context.Context, stmt api.Statement) (api.ResultSet, error)
	ExecuteBatch(ctx context.Context, stmts []api.Statement) (api.ResultSet, error)
}

// ExecutorBase base for all executors
type ExecutorBase struct {
	s *Store
}

// ReadExecutor supports select
type ReadExecutor struct {
	ExecutorBase
}

// WriteExecutor supports insert/update/delete
type WriteExecutor struct {
	ExecutorBase
}

// CASExecutor compare and set
type CASExecutor struct {
	WriteExecutor
}

func (b ExecutorBase) buildQuery(ctx context.Context, stmt api.Statement) (*gocql.Query, map[string]interface{}, error) {
	// start := time.Now()
	s := b.store()

	uql, args, options, err := stmt.ToUql()
	if err != nil {
		log.WithError(err).Error("Tosql failed")
		return nil, nil, err
	}

	s.convertUUID(args)
	qu := s.cSession.Query(uql, args...).WithContext(ctx)

	if queryOverrides, ok := queryOverridesFromContext(ctx); ok {
		if isConsistencyOverridden(queryOverrides) {
			qu.Consistency(queryOverrides.Consistency.Value)
		}
	}

	// s.sendLatency(ctx, "build_latency", time.Since(start))
	return qu, options, nil
}

func (b ExecutorBase) store() *Store {
	return b.s
}

// Execute a read statement
func (r ReadExecutor) Execute(ctx context.Context, stmt api.Statement) (api.ResultSet, error) {
	s := r.store()
	qu, options, err := r.buildQuery(ctx, stmt)
	if err != nil {
		return nil, err
	}

	selectStmt := stmt.(qb.SelectBuilder)
	if psize, ok := options["PageSize"]; ok && psize.(int) > 0 {
		qu.PageSize(psize.(int))
	}

	if disable, ok := options["DisableAutoPage"]; ok && disable.(bool) {
		qu.PageState(selectStmt.GetPagingState())
	}
	rs := &ResultSet{
		rawIter: qu.Iter(),
		store:   s,
	}
	// s.sendLatency(ctx, "execute_latency", time.Duration(qu.Latency()))
	return rs, nil
}

// ExecuteBatch is not supported on Read
func (r ReadExecutor) ExecuteBatch(ctx context.Context, stmts []api.Statement) (api.ResultSet, error) {
	return nil, api.ErrUnsupported
}

// Execute a write
func (w WriteExecutor) Execute(ctx context.Context, stmt api.Statement) (api.ResultSet, error) {
	qu, _, err := w.buildQuery(ctx, stmt)

	if err != nil {
		return nil, err
	}
	err = qu.Exec()
	// w.store().sendLatency(ctx, "execute_latency", time.Duration(qu.Latency()))
	qu.Release()
	if err != nil {
		log.WithError(err).Debug("Exec failed")
	}
	return nil, err
}

// ExecuteBatch sends batch of write operations
func (w WriteExecutor) ExecuteBatch(ctx context.Context, stmts []api.Statement) (api.ResultSet, error) {
	s := w.store()
	batch := s.cSession.NewBatch(gocql.LoggedBatch)
	var (
		uql  string
		err  error
		args []interface{}
	)

	if len(stmts) > w.s.maxBatch {
		log.WithField("batch_size", len(stmts)).Warn("This query could timeout")
	}
	for _, stmt := range stmts {
		if stmt.StmtType() == qb.SelectStmtType {
			return nil, api.ErrUnsupported
		}

		uql, args, err = stmt.ToSQL()
		if err != nil {
			log.WithError(err).Error("Tosql failed")
			return nil, err
		}
		log.WithFields(log.Fields{common.DBUqlLogField: uql,
			common.DBArgsLogField: args}).
			Debug("cql and args")
		s.convertUUID(args)
		batch.WithContext(ctx).Query(uql, args...)
	}
	if queryOverrides, ok := queryOverridesFromContext(ctx); ok {
		if isConsistencyOverridden(queryOverrides) {
			batch.Cons = queryOverrides.Consistency.Value
		}
	}
	err = s.cSession.ExecuteBatch(batch)
	// s.sendLatency(ctx, "execute_latency", time.Duration(batch.Latency()))
	if err != nil {
		log.WithError(err).WithField("uql", uql).Error("ExecuteBatch failed")
	}
	return nil, err
}

// Execute CAS insert or update
func (c CASExecutor) Execute(ctx context.Context, stmt api.Statement) (api.ResultSet, error) {
	s := c.store()
	qu, _, err := c.buildQuery(ctx, stmt)
	if err != nil {
		return nil, err
	}
	qu.SerialConsistency(gocql.LocalSerial)

	dest := make(map[string]interface{})
	applied, err := qu.MapScanCAS(dest)
	rs := &CASResultSet{
		ResultSet: ResultSet{store: s},
		applied:   applied,
		dest:      dest,
	}
	// s.sendLatency(ctx, "execute_latency", time.Duration(qu.Latency()))
	qu.Release()
	if err != nil {
		log.WithError(err).Error("MapScanCAS failed")
	}
	return rs, err
}

// ExecuteBatch is not supported with CAS
func (c CASExecutor) ExecuteBatch(ctx context.Context, stmts []api.Statement) (api.ResultSet, error) {
	return nil, api.ErrUnsupported
}

// isConsistencyOverridden safely determines whether or not to apply a consistency override
func isConsistencyOverridden(queryOverrides *api.QueryOverrides) bool {
	return queryOverrides != nil && queryOverrides.Consistency != nil
}

// queryOverridesFromContext retrieves query overrides from the context if they exist
func queryOverridesFromContext(ctx context.Context) (*api.QueryOverrides, bool) {
	s, ok := ctx.Value(api.QueryOverridesKey).(*api.QueryOverrides)
	return s, ok
}
