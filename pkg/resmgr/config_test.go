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

package resmgr

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/uber/peloton/pkg/common/config"

	"github.com/stretchr/testify/assert"
)

const testConfig = `
  http_port: 5290
  grpc_port: 5394
  task_scheduling_period: 100ms
  entitlement_calculation_period: 60s
  task_reconciliation_period: 1h
  task:
    placing_timeout: 10m
    launching_timeout: 20m
    preempting_timeout: 10m
  preemption:
    task_preemption_period: 60s
    sustained_over_allocation_count: 5
    enabled: true
`

func writeFile(t *testing.T, contents string) string {
	f, err := ioutil.TempFile("", "configtest")
	assert.NoError(t, err)
	defer f.Close()

	_, err = f.Write([]byte(contents))
	assert.NoError(t, err)
	return f.Name()
}

func TestResourceManagerConfig(t *testing.T) {
	cfgFile := writeFile(t, testConfig)
	defer os.Remove(cfgFile)
	var testConfig Config
	err := config.Parse(&testConfig, cfgFile)
	assert.NoError(t, err)

	assert.Equal(t, 5290, testConfig.HTTPPort)
	assert.Equal(t, 5394, testConfig.GRPCPort)
	assert.Equal(t, 100*time.Millisecond, testConfig.TaskSchedulingPeriod)
	assert.Equal(t, 60*time.Second, testConfig.EntitlementCaculationPeriod)
	assert.Equal(t, 1*time.Hour, testConfig.TaskReconciliationPeriod)
	assert.Equal(t, 10*time.Minute, testConfig.RmTaskConfig.PlacingTimeout)
	assert.Equal(t, 20*time.Minute, testConfig.RmTaskConfig.LaunchingTimeout)
	assert.Equal(t, 10*time.Minute, testConfig.RmTaskConfig.PreemptingTimeout)
	assert.Equal(t, 1*time.Minute, testConfig.PreemptionConfig.TaskPreemptionPeriod)
	assert.Equal(t, 5, testConfig.PreemptionConfig.SustainedOverAllocationCount)
	assert.Equal(t, true, testConfig.PreemptionConfig.Enabled)
}
