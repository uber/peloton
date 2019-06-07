// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package watchevent

import (
	"fmt"
	"sync"

	halphapb "github.com/uber/peloton/.gen/peloton/api/v1alpha/host"
	"github.com/uber/peloton/.gen/peloton/private/eventstream"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/uber/peloton/pkg/hostmgr/metrics"
	"go.uber.org/yarpc/yarpcerrors"
)

// StopSignal is an event sent through event client Signal channel
// indicating a stop event for the specific watcher.
type StopSignal int

const (
	// StopSignalUnknown indicates a unspecified StopSignal.
	StopSignalUnknown StopSignal = iota
	// StopSignalCancel indicates the watch is cancelled by the user.
	StopSignalCancel
	// StopSignalOverflow indicates the watch is aborted due to event
	// overflow.
	StopSignalOverflow
)

// String returns a user-friendly name for the specific StopSignal
func (s StopSignal) String() string {
	switch s {
	case StopSignalCancel:
		return "cancel"
	case StopSignalOverflow:
		return "overflow"
	default:
		return "unknown"
	}
}

// WatchProcessor interface is a central controller which handles watch
// client lifecycle, and task / job event fan-out.
type WatchProcessor interface {
	// NewEventClient creates a new watch client for mesos task event changes.
	// Returns the watch id and a new instance of EventClient.
	NewEventClient(topic Topic) (string, *EventClient, error)

	// StopEventClients stops all the event clients on leadership change.
	StopEventClients()

	// StopEventClient stops a event watch client. Returns "not-found" error
	// if the corresponding watch client is not found.
	StopEventClient(watchID string) error

	// NotifyEventChange receives mesos task event, and notifies all the clients
	// which are interested in the event.
	NotifyEventChange(event interface{})
}

// watchProcessor is an implementation of WatchProcessor interface.
type watchProcessor struct {
	sync.Mutex
	bufferSize        int
	maxClient         int
	eventClients      map[string]*EventClient
	topicEventClients map[Topic]map[string]bool
	metrics           *metrics.Metrics
}

// Topic define the event object type processor supported
type Topic string

// List of topic currently supported by watch processor
const (
	EventStream Topic = "eventstream"
	HostSummary Topic = "hostSummary"
	INVALID     Topic = ""
)

var processor *watchProcessor
var onceInitWatchProcessor sync.Once

// EventClient represents a client which interested in task event changes.
type EventClient struct {
	Input  chan interface{}
	Signal chan StopSignal
}

// newWatchProcessor should only be used in unit tests.
// Call InitWatchProcessor for regular case use.
func NewWatchProcessor(
	cfg Config,
	watchEventMetric *metrics.Metrics,
) *watchProcessor {
	cfg.normalize()
	return &watchProcessor{
		bufferSize:        cfg.BufferSize,
		maxClient:         cfg.MaxClient,
		eventClients:      make(map[string]*EventClient),
		topicEventClients: make(map[Topic]map[string]bool),
		metrics:           watchEventMetric,
	}
}

// InitWatchProcessor initializes WatchProcessor singleton.
func InitWatchProcessor(
	cfg Config,
	watchEventMetric *metrics.Metrics,
) {
	onceInitWatchProcessor.Do(func() {
		processor = NewWatchProcessor(cfg, watchEventMetric)
	})
}

// GetWatchProcessor returns WatchProcessor singleton.
func GetWatchProcessor() WatchProcessor {
	return processor
}

// NewWatchID creates a new watch id UUID string for the specific
// watch client
func NewWatchID(topic Topic) string {
	return fmt.Sprintf("%s_%s_%s", topic, "watch", uuid.New())
}

// Retrieve the topic from the event object received during NotifyEventChange
func GetTopicFromTheEvent(event interface{}) Topic {
	switch event.(type) {
	case *eventstream.Event:
		return EventStream
	case *halphapb.HostSummary:
		return HostSummary
	default:
		return INVALID
	}
}

// Map the string receive from input to a right topic
func GetTopicFromInput(topic string) Topic {
	switch topic {
	case "eventstream":
		return EventStream
	case "hostSummary":
		return HostSummary
	default:
		return INVALID
	}
}

// NewEventClient creates a new watch client for task event changes.
// Returns the watch id and a new instance of EventClient.
func (p *watchProcessor) NewEventClient(topic Topic) (string, *EventClient, error) {
	p.Lock()
	defer p.Unlock()

	if len(p.eventClients) >= p.maxClient {
		return "", nil, yarpcerrors.ResourceExhaustedErrorf("max client reached")
	}

	watchID := NewWatchID(topic)
	p.eventClients[watchID] = &EventClient{
		Input: make(chan interface{}, p.bufferSize),
		// Make buffer size 1 so that sender is not blocked when sending
		// the Signal
		Signal: make(chan StopSignal, 1),
	}
	if p.topicEventClients[topic] == nil {
		p.topicEventClients[topic] = make(map[string]bool)
	}
	p.topicEventClients[topic][watchID] = true

	log.WithField("watch_id", watchID).Info("task watch client created")
	return watchID, p.eventClients[watchID], nil
}

// StopEventClients stops all the event clients on host manager leader change
func (p *watchProcessor) StopEventClients() {
	p.Lock()
	defer p.Unlock()

	for watchID := range p.eventClients {
		p.stopEventClient(watchID, StopSignalCancel)
	}
}

// StopEventClient stops a event watch client. Returns "not-found" error
// if the corresponding watch client is not found.
func (p *watchProcessor) StopEventClient(watchID string) error {
	p.Lock()
	defer p.Unlock()

	return p.stopEventClient(watchID, StopSignalCancel)
}

func (p *watchProcessor) stopEventClient(
	watchID string,
	Signal StopSignal,
) error {
	c, ok := p.eventClients[watchID]
	if !ok {
		return yarpcerrors.NotFoundErrorf(
			"watch_id %s not exist for task watch client", watchID)
	}

	log.WithFields(log.Fields{
		"watch_id": watchID,
		"signal":   Signal,
	}).Info("stopping  watch client")

	c.Signal <- Signal
	delete(p.eventClients, watchID)
	for _, watchIdMap := range p.topicEventClients {
		delete(watchIdMap, watchID)
	}

	return nil
}

// NotifyTaskChange receives mesos task update event, and notifies all the clients
// which are interested in the task update event.
func (p *watchProcessor) NotifyEventChange(
	event interface{}) {
	sw := p.metrics.WatchProcessorLockDuration.Start()
	p.Lock()
	defer p.Unlock()
	sw.Stop()

	// get topic  of the event
	topic := GetTopicFromTheEvent(event)
	if topic == "" {
		log.WithFields(log.Fields{
			"event": event,
			"topic": string(topic),
		}).Warn("topic  not supported, please register topic to the watch processor")

	} else {
		for watchID := range p.topicEventClients[topic] {
			select {
			case p.eventClients[watchID].Input <- event:
			default:
				log.WithField("watch_id", watchID).
					Warn("event overflow for task watch client")
				p.stopEventClient(watchID, StopSignalOverflow)
			}
		}
	}
}
