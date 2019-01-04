package update

import (
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/stretchr/testify/assert"
)

func TestHasUpdate(t *testing.T) {
	hasUpdateTests := []struct {
		*job.RuntimeInfo
		result bool
	}{
		{nil, false},
		{&job.RuntimeInfo{}, false},
		{&job.RuntimeInfo{UpdateID: &peloton.UpdateID{}}, false},
		{&job.RuntimeInfo{UpdateID: &peloton.UpdateID{
			Value: "update-id",
		}}, true},
	}

	for i, test := range hasUpdateTests {
		assert.Equal(t, HasUpdate(test.RuntimeInfo), test.result,
			"test %d fails", i)
	}
}

func TestHasFailedUpdate(t *testing.T) {
	hasUpdateFailedTests := []struct {
		failureCount uint32
		maxAttempts  uint32
		result       bool
	}{
		{100, 0, false},
		{1, 1, true},
		{2, 1, true},
	}

	for i, test := range hasUpdateFailedTests {
		assert.Equal(t,
			HasFailedUpdate(
				&task.RuntimeInfo{FailureCount: test.failureCount},
				test.maxAttempts),
			test.result,
			"test %d fails", i)
	}
}
