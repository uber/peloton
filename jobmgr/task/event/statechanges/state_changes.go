package statechanges

import (
	"time"

	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
)

// TaskEventByTime is special type to task events by time
type TaskEventByTime []*task.TaskEvent

func (e TaskEventByTime) Len() int { return len(e) }

func (e TaskEventByTime) Swap(i, j int) { e[i], e[j] = e[j], e[i] }

// Sort the list of event lists based on the timestamp of the first event in each list
func (e TaskEventByTime) Less(i, j int) bool {
	ei := e[i]
	ej := e[j]

	// This should never fail, but we don't want to crash, so catch the error here
	tsi, err := time.Parse(time.RFC3339, ei.GetTimestamp())
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"event_i": ei,
			"event_j": ej,
		}).Error("failed to parse timestamp")
		return false
	}

	// This should never fail, but we don't want to crash, so catch the error here
	// We assume an invalid time is infinity large
	tsj, err := time.Parse(time.RFC3339, ej.GetTimestamp())
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"event_i": ei,
			"event_j": ej,
		}).Error("failed to parse timestamp")
		return true
	}

	return tsi.Before(tsj)
}

// TaskEventListByTime is special type to sort lists of events by time
type TaskEventListByTime []*task.GetEventsResponse_Events

func (e TaskEventListByTime) Len() int { return len(e) }

func (e TaskEventListByTime) Swap(i, j int) { e[i], e[j] = e[j], e[i] }

// Sort the list of event lists based on the timestamp of the first event in each list
func (e TaskEventListByTime) Less(i, j int) bool {
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
	// We assume an invalid time is infinity large
	tsj, err := time.Parse(time.RFC3339, ej[0].GetTimestamp())
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"events_list_i": ei,
			"events_list_j": ej,
		}).Error("failed to parse timestamp")
		return true
	}

	return tsi.Before(tsj)
}
