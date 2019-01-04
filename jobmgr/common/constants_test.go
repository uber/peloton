package common

import (
	"reflect"
	"testing"

	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/stretchr/testify/assert"
)

func TestTaskRuntimeInfoFieldNames(t *testing.T) {
	fieldNames := []string{
		AgentIDField,
		CompletionTimeField,
		FailureCountField,
		GoalStateField,
		MesosTaskIDField,
		MessageField,
		PortsField,
		PrevMesosTaskIDField,
		ReasonField,
		RevisionField,
		StartTimeField,
		StateField,
		VolumeIDField,
		HostField,
		ConfigVersionField,
		DesiredConfigVersionField,
		HealthyField,
	}

	taskRuntimeType := reflect.TypeOf(pbtask.RuntimeInfo{})
	for _, fieldName := range fieldNames {
		_, exist := taskRuntimeType.FieldByName(fieldName)
		assert.True(t, exist, "field %s is expected to exist", fieldName)
	}
}
