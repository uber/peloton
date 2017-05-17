package main

import (
	"fmt"
	"math"
	"testing"

	pt "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"github.com/stretchr/testify/assert"
)

func TestParseJobCreate(t *testing.T) {
	cfg := "../../example/testjob.yaml"
	path := "/infra/compute"
	cmd, err := app.Parse([]string{"job", "create", path, cfg})
	assert.Nil(t, err)
	assert.Equal(t, cmd, jobCreate.FullCommand())
	assert.Equal(t, *jobCreateConfig, cfg)

	cmd, err = app.Parse([]string{"job", "create", path, cfg})
	assert.Nil(t, err)
	assert.Equal(t, cmd, jobCreate.FullCommand())
	assert.Equal(t, *jobCreateConfig, cfg)
}

func TestParseJobCancel(t *testing.T) {
	job := "foojobid"
	for _, c := range []string{"cancel", "delete"} {
		cmd, err := app.Parse([]string{"job", c, job})
		assert.Nil(t, err)
		assert.Equal(t, jobDelete.FullCommand(), cmd)
		assert.Equal(t, job, *jobDeleteName)
	}
}

func TestParseTaskGet(t *testing.T) {
	job, instance := "foojobid", uint32(123)
	cmd, err := app.Parse([]string{"task", "get", job, fmt.Sprintf("%d", instance)})
	assert.Nil(t, err)
	assert.Equal(t, taskGet.FullCommand(), cmd)
	assert.Equal(t, *taskGetJobName, job)
	assert.Equal(t, instance, *taskGetInstanceID)
}

func TestParseTaskList(t *testing.T) {
	job := "foojobid"
	cmd, err := app.Parse([]string{"task", "list", job})
	assert.Nil(t, err)
	assert.Equal(t, taskList.FullCommand(), cmd)
	assert.Equal(t, *taskListJobName, job)
	assert.Equal(t, uint32(0), taskListInstanceRange.From)
	assert.Equal(t, uint32(math.MaxUint32), taskListInstanceRange.To)
}

func TestParseTaskListWithRange(t *testing.T) {
	job := "foojobid"
	cmd, err := app.Parse([]string{"task", "list", job, "-r", "4:6"})
	assert.Nil(t, err)
	assert.Equal(t, taskList.FullCommand(), cmd)
	assert.Equal(t, *taskListJobName, job)
	assert.Equal(t, pt.InstanceRange{uint32(4), uint32(6)}, *taskListInstanceRange)
}

func TestRangeParsing(t *testing.T) {
	expected := map[string]pt.InstanceRange{
		":":     {uint32(0), uint32(math.MaxUint32)},
		":100":  {uint32(0), uint32(100)},
		"5:":    {uint32(5), uint32(math.MaxUint32)},
		"55:99": {uint32(55), uint32(99)},
	}
	for s, expect := range expected {
		ir, err := parseRangeFromString(s)
		assert.Nil(t, err)
		assert.Equal(t, expect, ir)
	}
}

func TestParseTaskStartWithRanges(t *testing.T) {
	job := "foojobid"
	expected := []*pt.InstanceRange{
		{uint32(3), uint32(6)},
		{uint32(0), uint32(50)},
		{uint32(5), uint32(math.MaxUint32)},
	}
	cmd, err := app.Parse([]string{"task", "start", job, "-r", "3:6", "-r", ":50", "-r", "5:"})
	assert.Nil(t, err)
	assert.Equal(t, taskStart.FullCommand(), cmd)
	assert.Equal(t, *taskStartJobName, job)
	assert.Equal(t, fmt.Sprintf("%v", expected), fmt.Sprintf("%v", *taskStartInstanceRanges))
}
