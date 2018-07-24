package statechanges

import (
	"math/rand"
	"sort"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"github.com/stretchr/testify/suite"
)

type SortTaskEventsTestSuite struct {
	suite.Suite
	totalEvents uint32
	events      []*task.TaskEvent
}

func (suite *SortTaskEventsTestSuite) SetupTest() {
	suite.totalEvents = uint32(15)

	for i := uint32(0); i < suite.totalEvents; i++ {
		t := time.Duration(rand.Int31n(1000)) * time.Second
		event := &task.TaskEvent{
			State:     task.TaskState_PENDING,
			Message:   "",
			Timestamp: time.Now().Add(t).UTC().Format(time.RFC3339),
			Hostname:  "peloton-test-host",
			Reason:    "",
		}
		suite.events = append(suite.events, event)
	}
}

func TestPelotonTaskUpdater(t *testing.T) {
	suite.Run(t, new(SortTaskEventsTestSuite))
}

// Test the happy case of sorting
func (suite *SortTaskEventsTestSuite) TestSortTaskEvents() {
	sort.Sort(TaskEventByTime(suite.events))
	for i := uint32(1); i < suite.totalEvents; i++ {
		prev, err := time.Parse(time.RFC3339, suite.events[i-1].Timestamp)
		suite.NoError(err)
		after, err := time.Parse(time.RFC3339, suite.events[i].Timestamp)
		suite.NoError(err)
		suite.True(prev.Before(after) || prev.Equal(after))
	}
}

// Testing when there are invalid time field
func (suite *SortTaskEventsTestSuite) TestSortTaskEventsInvlaidTime() {
	suite.events[4].Timestamp = ""
	suite.True(TaskEventByTime(suite.events).Less(int(3), int(4)))
	suite.False(TaskEventByTime(suite.events).Less(int(4), int(5)))
}

func (suite *SortTaskEventsTestSuite) TestTaskEventListByTime() {
	suite.events[1].Timestamp = ""
	testEvents := []*task.GetEventsResponse_Events{}

	for i := uint32(0); i < uint32(3); i++ {
		eventList := task.GetEventsResponse_Events{
			Event: []*task.TaskEvent{suite.events[i]},
		}
		testEvents = append(testEvents, &eventList)
	}
	testEvents = append(testEvents, nil)
	taskEventListByTime := TaskEventListByTime(testEvents)

	sort.Sort(taskEventListByTime)

	suite.True(taskEventListByTime.Less(int(0), int(1)))
	suite.True(taskEventListByTime.Less(int(1), int(2)))
	suite.True(taskEventListByTime.Less(int(0), int(2)))
	suite.False(taskEventListByTime.Less(int(2), int(1)))
	suite.False(taskEventListByTime.Less(int(3), int(3)))
}
