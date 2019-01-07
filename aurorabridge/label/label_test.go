package label

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBuild checks peloton label is created correctly.
func TestBuild(t *testing.T) {
	key := "test-key"
	value := "test-value"
	label := &rawLabel{
		key:   key,
		value: value,
	}

	pelotonLabel := Build(label)
	assert.Equal(t, key, pelotonLabel.GetKey())
	assert.Equal(t, value, pelotonLabel.GetValue())
}

// TestBuildMany check a list of peloton lables are created correctly.
func TestBuildMany(t *testing.T) {
	key1 := "test-key-1"
	value1 := "test-value-1"
	key2 := "test-key-2"
	value2 := "test-value-2"
	labels := []Label{
		&rawLabel{
			key:   key1,
			value: value1,
		},
		&rawLabel{
			key:   key2,
			value: value2,
		},
	}

	pelotonLabels := BuildMany(labels)
	for _, pelotonLabel := range pelotonLabels {
		switch pelotonLabel.GetKey() {
		case key1:
			assert.Equal(t, value1, pelotonLabel.GetValue())
		case key2:
			assert.Equal(t, value2, pelotonLabel.GetValue())
		default:
			assert.Fail(t, "unexpected label found: \"%s\"",
				pelotonLabel.GetKey())
		}
	}
}
