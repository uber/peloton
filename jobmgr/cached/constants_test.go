package cached

import (
	"reflect"
	"testing"

	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

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
	}

	taskRuntimeType := reflect.TypeOf(pbtask.RuntimeInfo{})
	for _, fieldName := range fieldNames {
		_, exist := taskRuntimeType.FieldByName(fieldName)
		assert.True(t, exist, "field %s is expected to exist", fieldName)
	}
}
