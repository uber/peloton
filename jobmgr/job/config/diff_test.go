package jobconfig

import (
	"io/ioutil"
	"sort"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func TestCalculateJobDiff_IsNoop(t *testing.T) {
	oldConfig := getConfig(oldConfig, t)
	id := &peloton.JobID{
		Value: uuid.New(),
	}
	diff := CalculateJobDiff(id, oldConfig, oldConfig)
	assert.True(t, diff.IsNoop())
}

func TestCalculateJobDiff(t *testing.T) {
	validNewConfig := getConfig(newConfig, t)
	oldConfig := getConfig(oldConfig, t)
	id := &peloton.JobID{
		Value: uuid.New(),
	}
	diff := CalculateJobDiff(id, oldConfig, validNewConfig)
	assert.False(t, diff.IsNoop())
	expectedInstanceIDs := makeRange(int(oldConfig.InstanceCount), int(validNewConfig.InstanceCount))
	var actualInstanceIDs []int
	for instanceID := range diff.InstancesToAdd {
		actualInstanceIDs = append(actualInstanceIDs, int(instanceID))
	}
	sort.Ints(expectedInstanceIDs)
	sort.Ints(actualInstanceIDs)
	assert.Equal(t, expectedInstanceIDs, actualInstanceIDs)
}

func makeRange(min, max int) []int {
	a := make([]int, max-min)
	for i := range a {
		a[i] = min + i
	}
	return a
}

func getConfig(config string, t *testing.T) *job.JobConfig {
	var jobConfig job.JobConfig
	buffer, err := ioutil.ReadFile(config)
	assert.NoError(t, err)
	err = yaml.Unmarshal(buffer, &jobConfig)
	assert.NoError(t, err)
	return &jobConfig
}
