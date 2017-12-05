package statechanges

import (
	"time"

	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

// TaskEventByTime is special type to sort lists of events by time
type TaskEventByTime []*task.GetEventsResponse_Events

func (e TaskEventByTime) Len() int { return len(e) }

func (e TaskEventByTime) Swap(i, j int) { e[i], e[j] = e[j], e[i] }

// Sort the list of event lists based on the timestamp of the first event in each list
func (e TaskEventByTime) Less(i, j int) bool {
	ei := e[i].GetEvent()
	ej := e[j].GetEvent()
	// These will never be empty, but we can have that check for precaution
	if len(ei) == 0 || len(ej) == 0 {
		log.WithFields(log.Fields{
			"events_list_i": ei,
			"events_list_j": ej,
		}).Error("empty events list")
		return false
	}

	// This should never fail, but we don't want to crash, so catch the error here
	tsi, err := time.Parse(time.RFC3339, ei[0].GetTimestamp())
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"events_list_i": ei,
			"events_list_j": ej,
		}).Error("failed to parse timestamp")
		return false
	}

	// This should never fail, but we don't want to crash, so catch the error here
	tsj, err := time.Parse(time.RFC3339, ej[0].GetTimestamp())
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"events_list_i": ei,
			"events_list_j": ej,
		}).Error("failed to parse timestamp")
		return false
	}

	return tsi.Before(tsj)
}
