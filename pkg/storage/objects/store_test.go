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
	"fmt"

	"github.com/uber-go/tally"
)

// testStore is the ORM store to use for tests
var testStore *Store

// Create and initialize a store for object tests
func setupTestStore() {
	conf := GenerateTestCassandraConfig()

	if testStore != nil { // && testSession != nil {
		return
	}

	MigrateSchema(conf)

	s, err := NewCassandraStore(
		conf,
		tally.NewTestScope(
			"", map[string]string{}),
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup test store: %v", err))
	}

	testStore = s
}
