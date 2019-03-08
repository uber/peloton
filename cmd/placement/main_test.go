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

package main

import (
	"testing"

	"github.com/uber/peloton/pkg/placement/config"

	"github.com/stretchr/testify/assert"
)

func TestOverridePlacementStrategy(t *testing.T) {

	tt := []struct {
		name          string
		taskType      string
		wantStrategy  config.PlacementStrategy
		wantFetchTask bool
	}{
		{
			name:          "stateless tasks should use mimir strategy",
			taskType:      "STATELESS",
			wantStrategy:  config.Mimir,
			wantFetchTask: true,
		},
		{
			name:          "stateful tasks should use mimir strategy",
			taskType:      "STATEFUL",
			wantStrategy:  config.Mimir,
			wantFetchTask: true,
		},
		{
			name:          "batch tasks should use batch strategy",
			taskType:      "BATCH",
			wantStrategy:  config.Batch,
			wantFetchTask: false,
		},
		{
			name:          "daemon tasks should use batch strategy",
			taskType:      "DAEMON",
			wantStrategy:  config.Batch,
			wantFetchTask: false,
		},
	}

	for _, test := range tt {
		cfg := &config.Config{}
		overridePlacementStrategy(test.taskType, cfg)
		assert.Equal(
			t,
			test.wantStrategy,
			cfg.Placement.Strategy,
			test.name)
		assert.Equal(
			t,
			test.wantFetchTask,
			cfg.Placement.FetchOfferTasks,
			test.name)
	}
}
