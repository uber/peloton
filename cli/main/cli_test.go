package main

import (
	"fmt"
	"math"
	"testing"

	pt "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"github.com/stretchr/testify/assert"
)

const (
	testJobID = "481d565e-28da-457d-8434-f6bb7faa0e95"
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

func TestParseJobDelete(t *testing.T) {
	job := "foojobid"
	cmd, err := app.Parse([]string{"job", "delete", job})
	assert.Nil(t, err)
	assert.Equal(t, jobDelete.FullCommand(), cmd)
	assert.Equal(t, job, *jobDeleteName)
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
	assert.Equal(t, pt.InstanceRange{From: uint32(4), To: uint32(6)}, *taskListInstanceRange)
}

func TestRangeParsing(t *testing.T) {
	expected := map[string]pt.InstanceRange{
		":":     {From: uint32(0), To: uint32(math.MaxUint32)},
		":100":  {From: uint32(0), To: uint32(100)},
		"5:":    {From: uint32(5), To: uint32(math.MaxUint32)},
		"55:99": {From: uint32(55), To: uint32(99)},
	}
	for s, expect := range expected {
		ir, err := parseRangeFromString(s)
		assert.Nil(t, err)
		assert.Equal(t, expect, ir)
	}
}

func TestRangeParsingError(t *testing.T) {
	expected := []string{"55:56:57", "try:56", "56:try"}
	for _, s := range expected {
		_, err := parseRangeFromString(s)
		assert.NotNil(t, err)
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

func TestParseJobGet(t *testing.T) {
	jobID := testJobID
	cmd, err := app.Parse([]string{"job", "get", jobID})
	assert.Nil(t, err)
	assert.Equal(t, cmd, jobGet.FullCommand())
	assert.Equal(t, *jobGetName, jobID)
}

func TestParseJobStatus(t *testing.T) {
	jobID := testJobID
	cmd, err := app.Parse([]string{"job", "status", jobID})
	assert.Nil(t, err)
	assert.Equal(t, cmd, jobStatus.FullCommand())
	assert.Equal(t, *jobStatusName, jobID)
}

func TestTaskQuery(t *testing.T) {
	jobID := testJobID
	cmd, err := app.Parse([]string{"task", "query", jobID, "--names=taskName", "--hosts=taskHost", "--sort=state", "--sortorder=DESC"})
	assert.Nil(t, err)
	assert.Equal(t, cmd, taskQuery.FullCommand())
	assert.Equal(t, *taskQueryJobName, jobID)
	assert.Equal(t, *taskQuerySortBy, "state")
	assert.Equal(t, *taskQuerySortOrder, "DESC")
	assert.Equal(t, *taskQueryTaskNames, "taskName")
	assert.Equal(t, *taskQueryTaskHosts, "taskHost")

	// test default value
	cmd, err = app.Parse([]string{"task", "query", jobID})
	assert.Equal(t, cmd, taskQuery.FullCommand())
	assert.Equal(t, *taskQueryJobName, jobID)
	assert.Equal(t, *taskQuerySortOrder, "ASC")

	// test invalid input
	cmd, err = app.Parse([]string{"task", "query", jobID, "--sort=state", "--sortorder=XXX"})
	assert.NotNil(t, err)

}
