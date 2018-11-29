package job

import (
	"testing"

	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"

	"github.com/stretchr/testify/assert"
)

func TestGetEntityVersion(t *testing.T) {
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
		assert.Equal(t, GetEntityVersion(test.configVersion).GetValue(), test.entityVersion)
	}
}

func TestParseEntityVersion(t *testing.T) {
	tests := []struct {
		entityVersion string
		configVersion uint64
		hasError      bool
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
		configVersion, err := ParseEntityVersion(&v1alphapeloton.EntityVersion{
			Value: test.entityVersion,
		})
		if test.hasError {
			assert.Error(t, err)
		} else {
			assert.Equal(t, configVersion, test.configVersion)
		}
	}
}
