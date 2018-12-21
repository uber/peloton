package job

import (
	"testing"

	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"

	"github.com/stretchr/testify/assert"
)

func TestGetJobEntityVersion(t *testing.T) {
	tests := []struct {
		configVersion       uint64
		desiredStateVersion uint64
		workflowVersion     uint64
		entityVersion       string
	}{
		{
			configVersion: 1, desiredStateVersion: 2, workflowVersion: 2, entityVersion: "1-2-2",
		},
		{
			configVersion: 10, desiredStateVersion: 4, workflowVersion: 3, entityVersion: "10-4-3",
		},
	}

	for _, test := range tests {
		assert.Equal(t,
			GetJobEntityVersion(test.configVersion, test.desiredStateVersion, test.workflowVersion).GetValue(),
			test.entityVersion)
	}
}

func TestParseJobEntityVersion(t *testing.T) {
	tests := []struct {
		entityVersion       string
		configVersion       uint64
		desiredStateVersion uint64
		workflowVersion     uint64
		hasError            bool
	}{
		{
			entityVersion: "1-1-1", configVersion: 1, desiredStateVersion: 1, workflowVersion: 1, hasError: false,
		},
		{
			entityVersion: "10-2-3", configVersion: 10, desiredStateVersion: 2, workflowVersion: 3, hasError: false,
		},
		{
			entityVersion: "a", configVersion: 0, desiredStateVersion: 0, workflowVersion: 0, hasError: true,
		},
	}

	for _, test := range tests {
		configVersion, desiredStateVersion, workflowVersion, err := ParseJobEntityVersion(&v1alphapeloton.EntityVersion{
			Value: test.entityVersion,
		})
		if test.hasError {
			assert.Error(t, err)
		} else {
			assert.Equal(t, configVersion, test.configVersion)
			assert.Equal(t, desiredStateVersion, test.desiredStateVersion)
			assert.Equal(t, workflowVersion, test.workflowVersion)
		}
	}
}

func TestGetPodEntityVersion(t *testing.T) {
	tests := []struct {
		configVersion uint64
		entityVersion string
	}{
		{
			configVersion: 1, entityVersion: "1",
		},
		{
			configVersion: 10, entityVersion: "10",
		},
	}

	for _, test := range tests {
		assert.Equal(t,
			GetPodEntityVersion(test.configVersion).GetValue(),
			test.entityVersion)
	}
}

func TestParsePodEntityVersion(t *testing.T) {
	tests := []struct {
		entityVersion   string
		configVersion   uint64
		workflowVersion uint64
		hasError        bool
	}{
		{
			entityVersion: "1", configVersion: 1, hasError: false,
		},
		{
			entityVersion: "10", configVersion: 10, hasError: false,
		},
		{
			entityVersion: "a", configVersion: 0, hasError: true,
		},
	}

	for _, test := range tests {
		configVersion, err := ParsePodEntityVersion(&v1alphapeloton.EntityVersion{
			Value: test.entityVersion,
		})
		if test.hasError {
			assert.Error(t, err)
		} else {
			assert.Equal(t, configVersion, test.configVersion)
		}
	}
}
