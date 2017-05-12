package updater

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

const (
	_invalidNewConfig = "testdata/invalid_new_config.yaml"
	_validNewConfig   = "testdata/valid_new_config.yaml"
	_oldConfig        = "testdata/old_config.yaml"
)

func getConfig(config string, t *testing.T) *job.JobConfig {
	var jobConfig job.JobConfig
	buffer, err := ioutil.ReadFile(config)
	assert.NoError(t, err)
	err = yaml.Unmarshal(buffer, &jobConfig)
	assert.NoError(t, err)
	return &jobConfig
}

func TestValidateInvalidConfig(t *testing.T) {
	invalidNewConfig := getConfig(_invalidNewConfig, t)
	oldConfig := getConfig(_oldConfig, t)
	err := validateNewConfig(oldConfig, invalidNewConfig)
	assert.Error(t, err)
	expectedErrors := `5 errors occurred:

* updating Name not supported
* updating Labels not supported
* updating OwningTeam not supported
* updating LdapGroups not supported
* updating DefaultConfig not supported`
	assert.Equal(t, err.Error(), expectedErrors)
}

func TestValidateValidConfig(t *testing.T) {
	validNewConfig := getConfig(_validNewConfig, t)
	oldConfig := getConfig(_oldConfig, t)
	err := validateNewConfig(oldConfig, validNewConfig)
	assert.NoError(t, err)
}

func TestCalculateJobDiff_IsNoop(t *testing.T) {
	oldConfig := getConfig(_oldConfig, t)
	id := &peloton.JobID{
		Value: uuid.New(),
	}
	diff, err := CalculateJobDiff(id, oldConfig, oldConfig)
	assert.NoError(t, err)
	assert.True(t, diff.IsNoop())
}

func TestCalculateJobDiff(t *testing.T) {
	validNewConfig := getConfig(_validNewConfig, t)
	oldConfig := getConfig(_oldConfig, t)
	id := &peloton.JobID{
		Value: uuid.New(),
	}
	diff, err := CalculateJobDiff(id, oldConfig, validNewConfig)
	assert.NoError(t, err)
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
