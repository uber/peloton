package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestLabelKeyToEnvVarName tests LabelKeyToEnvVarName
func TestLabelKeyToEnvVarName(t *testing.T) {
	assert.Equal(t, "PELOTON_JOB_ID", LabelKeyToEnvVarName("peloton.job_id"))
}
