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

package concurrency

import (
	"context"
	"sync"
)

// Mapper maps inputs into outputs.
type Mapper interface {
	Map(ctx context.Context, input interface{}) (output interface{}, err error)
}

// MapperFunc is an adaptor to allow the use of ordinary functions as Mappers.
type MapperFunc func(ctx context.Context, input interface{}) (output interface{}, err error)

// Map calls f.
func (f MapperFunc) Map(ctx context.Context, input interface{}) (output interface{}, err error) {
	return f(ctx, input)
}

type mapResult struct {
	output interface{}
	err    error
}

// Map applies m.Map to inputs using numWorkers goroutines. Collects the
// outputs or stops early if error is encountered.
func Map(
	ctx context.Context,
	m Mapper,
	inputs []interface{},
	numWorkers int,
) (outputs []interface{}, err error) {

	inputc := make(chan interface{})
	resultc := make(chan *mapResult)

	var wg sync.WaitGroup

	// Ensure all channel sends/recvs have a release valve if we encounter
	// an early error.
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		// Make sure all workers have exited before return
		wg.Wait()
	}()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case i, ok := <-inputc:
					if !ok {
						return
					}
					o, err := m.Map(ctx, i)
					select {
					case resultc <- &mapResult{o, err}:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		for _, i := range inputs {
			select {
			case inputc <- i:
			case <-ctx.Done():
				return
			}
		}
		close(inputc)  // Signal to workers there are no more inputs.
		wg.Wait()      // Wait for workers to finish in-progress work.
		close(resultc) // Signal to consumer that work is finished.
	}()

	for {
		select {
		case r, ok := <-resultc:
			if !ok {
				return outputs, nil
			}
			if r.err != nil {
				return nil, r.err
			}
			if r.output != nil {
				outputs = append(outputs, r.output)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
