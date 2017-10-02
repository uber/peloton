package resmgr

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/common/config"

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
	assert.Equal(t, 1*time.Minute, testConfig.PreemptionConfig.TaskPreemptionPeriod)
	assert.Equal(t, 5, testConfig.PreemptionConfig.SustainedOverAllocationCount)
	assert.Equal(t, true, testConfig.PreemptionConfig.Enabled)
}
