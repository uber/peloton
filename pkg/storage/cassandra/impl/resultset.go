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
	log "github.com/sirupsen/logrus"

	"github.com/uber/peloton/pkg/storage/cassandra/api"
	qb "github.com/uber/peloton/pkg/storage/querybuilder"
)

// ResultSet is the result of a executed query
type ResultSet struct {
	rawIter *gocql.Iter
	store   *Store
}

// CASResultSet is returned from a Compare-and-Set operation
type CASResultSet struct {
	ResultSet
	applied bool
	dest    map[string]interface{}
}

const (
	allName  = "all"
	oneName  = "one"
	nextName = "next"
)

func (rs *ResultSet) findAll(ctx context.Context) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	f := func() error {
		var err error
		defer rs.rawIter.Close()
		result, err = rs.rawIter.SliceMap()

		if err != nil {
			log.WithError(err).Error("SliceMap failed")
			return err
		}
		// UUID conversion, input: gocql.UUID, output: qb.UUID
		for i, mapItem := range result {
			for k, v := range mapItem {
				if qb.IsUUID(v) {
					result[i][k] = qb.UUID{UUID: v.(gocql.UUID)}
				}
			}
		}
		return nil
	}
	err := f()
	// TODO: slu figure out if instrumentation can be done using open source
	//err := Decorate(f, Trace(ctx, allName), Instrument(ctx, rs.store, allName))()
	return result, err
}

// All returns all results in a SliceMap and cleans up itself.
// Client will not be able to use the ResultSet after this call.
func (rs *ResultSet) All(ctx context.Context) ([]map[string]interface{}, error) {
	if rs.rawIter == nil {
		log.Debug("no iterator")
		return nil, api.ErrInvalidUsage
	}
	return rs.findAll(ctx)
}

// One is deprecated. use Next instead
func (rs *ResultSet) One(ctx context.Context, dest ...interface{}) bool {
	if rs.rawIter == nil {
		return false
	}

	var hasMore bool
	f := func() error {
		tmp := make([]interface{}, len(dest))
		copy(tmp, dest)
		for i := range tmp {
			v := tmp[i]
			if v != nil && qb.IsUUID(v) {
				tmp[i] = &gocql.UUID{}
			}
		}
		hasMore = rs.rawIter.Scan(tmp...)
		for i := range dest {
			v := dest[i]
			if v != nil && qb.IsUUID(v) {
				dest[i].(*qb.UUID).UUID = *(tmp[i].(*gocql.UUID))
			}
		}
		if !hasMore {
			return rs.rawIter.Close()
		}
		return nil
	}
	Decorate(f, Trace(ctx, oneName), Count(ctx, rs.store, oneName))()
	return hasMore
}

// Next returns the next row out of the ResultSet. an empty map is returned if
// there is nothing left.
func (rs *ResultSet) Next(ctx context.Context) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	if rs.rawIter == nil {
		log.Debug("no iterator")
		return m, api.ErrUnsupported
	}

	var f api.FuncType
	f = func() error {
		more := rs.rawIter.MapScan(m)
		for k, v := range m {
			if qb.IsUUID(v) {
				m[k] = qb.UUID{UUID: v.(gocql.UUID)}
			}
		}
		if !more {
			return rs.rawIter.Close()
		}
		return nil
	}
	err := Decorate(f, Trace(ctx, nextName), Count(ctx, rs.store, nextName))()
	return m, err
}

// Applied shows whether a set was done in a compare-and-set operation
func (rs *ResultSet) Applied() bool {
	return false
}

// PagingState returns the pagination token as opaque bytes so that caller can pass in for future queries.
func (rs *ResultSet) PagingState() []byte {
	return rs.rawIter.PageState()
}

// Close closes the iterator and returns any errors that happened during
// the query or the iteration.
func (rs *ResultSet) Close() error {
	if rs.rawIter == nil {
		return nil
	}
	return rs.rawIter.Close()
}

// All returns a single result for non-applied CAS operations
func (rs *CASResultSet) All(ctx context.Context) ([]map[string]interface{}, error) {
	r := make([]map[string]interface{}, 1)
	r[0] = rs.dest
	return r, nil
}

// One is deprecated
func (rs *CASResultSet) One(ctx context.Context, dest ...interface{}) bool {
	return false
}

// Applied shows whether a set was done in a compare-and-set operation
func (rs *CASResultSet) Applied() bool {
	return rs.applied
}

// Next returns the result if cas was not applied
func (rs *CASResultSet) Next(ctx context.Context) (map[string]interface{}, error) {
	return rs.dest, nil
}

// PagingState has no value for CAS operations
func (rs *CASResultSet) PagingState() []byte {
	return nil
}
