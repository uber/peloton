package statechanges

import (
	"math/rand"
	"sort"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"github.com/stretchr/testify/assert"
)

func TestSortTaskEvents(t *testing.T) {
	var events []*task.TaskEvent

	totalEvents := uint32(15)

	for i := uint32(0); i < totalEvents; i++ {
		t := time.Duration(rand.Int31n(1000)) * time.Second
		event := &task.TaskEvent{
			State:     task.TaskState_PENDING,
			Message:   "",
			Timestamp: time.Now().Add(t).UTC().Format(time.RFC3339),
			Hostname:  "peloton-test-host",
			Reason:    "",
		}
		events = append(events, event)
	}

	sort.Sort(TaskEventByTime(events))

	for i := uint32(1); i < totalEvents; i++ {
		prev, err := time.Parse(time.RFC3339, events[i-1].Timestamp)
		assert.NoError(t, err)
		after, err := time.Parse(time.RFC3339, events[i].Timestamp)
		assert.NoError(t, err)
		assert.True(t, prev.Before(after) || prev.Equal(after))
	}
}
