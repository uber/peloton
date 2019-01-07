package label

import (
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"

	"github.com/stretchr/testify/assert"
)

// TestBuildAuroraJobKeyLabels checks BuildAuroraJobKeyLabels returns
// labels correctly based on number of non-empty parameters.
func TestBuildAuroraJobKeyLabels(t *testing.T) {
	role := "test-role"
	env := "test-env"
	name := "test-name"

	// role + environment + name
	labels := BuildAuroraJobKeyLabels(role, env, name)
	assert.Equal(t, 3, len(labels))
	for _, label := range labels {
		switch label.Key() {
		case _auroraJobKeyRoleKey:
			assert.Equal(t, label.Value(), role)
		case _auroraJobKeyEnvironmentKey:
			assert.Equal(t, label.Value(), env)
		case _auroraJobKeyNameKey:
			assert.Equal(t, label.Value(), name)
		default:
			assert.Fail(t, "unexpected label key: \"%s\"", label.Key())
		}
	}

	// role + environment
	labels = BuildAuroraJobKeyLabels(role, env, "")
	assert.Equal(t, 2, len(labels))
	for _, label := range labels {
		switch label.Key() {
		case _auroraJobKeyRoleKey:
			assert.Equal(t, label.Value(), role)
		case _auroraJobKeyEnvironmentKey:
			assert.Equal(t, label.Value(), env)
		default:
			assert.Fail(t, "unexpected label key: \"%s\"", label.Key())
		}
	}

	// role
	labels = BuildAuroraJobKeyLabels(role, "", "")
	assert.Equal(t, 1, len(labels))
	for _, label := range labels {
		switch label.Key() {
		case _auroraJobKeyRoleKey:
			assert.Equal(t, label.Value(), role)
		default:
			assert.Fail(t, "unexpected label key: \"%s\"", label.Key())
		}
	}
}

// TestBuildAuroraJobKeyFromLabels checks BuildAuroraJobKeyFromLabels
// returns aurora JobKey correctly based on input peloton labels.
func TestBuildAuroraJobKeyFromLabels(t *testing.T) {
	role := "test-role"
	env := "test-env"
	name := "test-name"
	roleLabel := &peloton.Label{
		Key:   _auroraJobKeyRoleKey,
		Value: role,
	}
	envLabel := &peloton.Label{
		Key:   _auroraJobKeyEnvironmentKey,
		Value: env,
	}
	nameLabel := &peloton.Label{
		Key:   _auroraJobKeyNameKey,
		Value: name,
	}

	// role + environment + name
	labels := []*peloton.Label{roleLabel, envLabel, nameLabel}
	jobKey := BuildAuroraJobKeyFromLabels(labels)
	assert.NotNil(t, jobKey)
	assert.Equal(t, role, *jobKey.Role)
	assert.Equal(t, env, *jobKey.Environment)
	assert.Equal(t, name, *jobKey.Name)

	// role + environment
	labels = []*peloton.Label{roleLabel, envLabel}
	jobKey = BuildAuroraJobKeyFromLabels(labels)
	assert.NotNil(t, jobKey)
	assert.Equal(t, role, *jobKey.Role)
	assert.Equal(t, env, *jobKey.Environment)
	assert.Nil(t, jobKey.Name)

	// role + environment
	labels = []*peloton.Label{roleLabel}
	jobKey = BuildAuroraJobKeyFromLabels(labels)
	assert.NotNil(t, jobKey)
	assert.Equal(t, role, *jobKey.Role)
	assert.Nil(t, jobKey.Environment)
	assert.Nil(t, jobKey.Name)
}
