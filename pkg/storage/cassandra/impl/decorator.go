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
	"sync/atomic"

	log "github.com/sirupsen/logrus"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/peloton/pkg/storage/cassandra/api"
)

// Decorate invokes all the decorators on the function, usage:
// Decorate(aFunc, Instrument(s), Trace(c, "execute"))()
func Decorate(ef api.FuncType, decorators ...api.Decorator) api.FuncType {
	for _, decorator := range decorators {
		ef = decorator(ef)
	}
	return ef
}

/* TODO: slu figure out if it is possible to keep the instrumentation not using uber go-common
// Instrument decorator sends timing and counter metrics
func Instrument(ctx context.Context, s *Store, funcName string) api.Decorator {
	scope := s.scope
	return func(ef api.FuncType) api.FuncType {
		return func() error {
			if scope == nil {
				return ef()
			}
			if tags, ok := tagsFromContext(ctx); ok {
				scope = scope.Tagged(tags)
			}

			concurrentRoutines := atomic.LoadInt32(&s.concurrency)
			scope.Gauge("usage").Update(int64(concurrentRoutines))
			return scope.InstrumentedCall(funcName).Exec(ef)
		}
	}
}
*/

// Count decorator sends success and failure count metrics
func Count(ctx context.Context, s *Store, funcName string) api.Decorator {
	scope := s.scope
	return func(ef api.FuncType) api.FuncType {
		return func() error {
			if scope == nil {
				return ef()
			}
			if tags, ok := tagsFromContext(ctx); ok {
				scope = scope.Tagged(tags)
			}
			errors := scope.Counter(fmt.Sprintf("%s.errors", funcName))
			success := scope.Counter(fmt.Sprintf("%s.success", funcName))
			if err := ef(); err != nil {
				errors.Inc(1)
				return err
			}
			success.Inc(1)
			return nil
		}
	}
}

// Trace decorator starts a new span for the underlying function call
func Trace(ctx context.Context, funcName string) api.Decorator {
	return func(ef api.FuncType) api.FuncType {
		return func() error {
			span := opentracing.SpanFromContext(ctx)
			if span != nil {
				child := opentracing.StartSpan(funcName, opentracing.ChildOf(span.Context()))
				defer child.Finish()
			}
			return ef()
		}
	}
}

// Safeguard ensures that the connection is neither closed nor overflooded
func Safeguard(s *Store) api.Decorator {
	return func(ef api.FuncType) api.FuncType {
		return func() error {
			if s.isClosed() {
				log.Debug("store already closed")
				return api.ErrClosed
			}

			atomic.AddInt32(&s.concurrency, 1)
			defer atomic.AddInt32(&s.concurrency, -1)
			if atomic.LoadInt32(&s.concurrency) > int32(s.maxConcurrency) {
				log.Debugf("over capacity %d", s.maxConcurrency)
				return api.ErrOverCapacity
			}
			return ef()
		}
	}
}

func tagsFromContext(ctx context.Context) (map[string]string, bool) {
	s, ok := ctx.Value(api.TagKey).(map[string]string)
	return s, ok
}
