package main

import (
	"testing"

	"github.com/uber/peloton/placement/config"

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
