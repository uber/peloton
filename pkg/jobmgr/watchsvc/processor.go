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

package watchsvc

import (
	"fmt"
	"sync"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/watch"

	"github.com/uber/peloton/pkg/common/util"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

// StopSignal is an event sent through task / job client Signal channel
// indicating a stnop event for the specific watcher.
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

// ClientType is a enum string to be embedded in the watch id
// returned to the client, used to indicate the watch client type.
type ClientType string

const (
	// ClientTypeTask indicates the watch id belongs to a task watch client
	ClientTypeTask ClientType = "task"
	// ClientTypeJob indicates the watch id belongs to a job watch client
	ClientTypeJob ClientType = "job"
)

func (t ClientType) String() string {
	return string(t)
}

// WatchProcessor interface is a central controller which handles watch
// client lifecycle, and task / job event fan-out.
type WatchProcessor interface {
	// NewTaskClient creates a new watch client for task event changes.
	// Returns the watch id and a new instance of TaskClient.
	NewTaskClient(filter *watch.PodFilter) (string, *TaskClient, error)

	// StopTaskClient stops a task watch client. Returns "not-found" error
	// if the corresponding watch client is not found.
	StopTaskClient(watchID string) error

	// NotifyTaskChange receives pod event, and notifies all the clients
	// which are interested in the pod.
	NotifyTaskChange(pod *pod.PodSummary, podLabels []*peloton.Label)
}

// watchProcessor is an implementation of WatchProcessor interface.
type watchProcessor struct {
	sync.Mutex
	bufferSize  int
	maxClient   int
	taskClients map[string]*TaskClient
	jobClients  map[string]*JobClient
	metrics     *Metrics
}

var processor *watchProcessor
var onceInitWatchProcessor sync.Once

// TaskClient represents a client which interested in task event changes.
type TaskClient struct {
	Filter *watch.PodFilter
	Input  chan *pod.PodSummary
	Signal chan StopSignal
}

// JobClient represents a client which interested in job event changes.
type JobClient struct {
	Filter *watch.StatelessJobFilter
	Input  chan *stateless.JobSummary
	Signal chan StopSignal
}

// newWatchProcessor should only be used in unit tests.
// Call InitWatchProcessor for regular case use.
func newWatchProcessor(
	cfg Config,
	parent tally.Scope,
) *watchProcessor {
	cfg.normalize()
	return &watchProcessor{
		bufferSize:  cfg.BufferSize,
		maxClient:   cfg.MaxClient,
		taskClients: make(map[string]*TaskClient),
		jobClients:  make(map[string]*JobClient),
		metrics:     NewMetrics(parent),
	}
}

// InitWatchProcessor initializes WatchProcessor singleton.
func InitWatchProcessor(
	cfg Config,
	parent tally.Scope,
) {
	onceInitWatchProcessor.Do(func() {
		processor = newWatchProcessor(cfg, parent)
	})
}

// GetWatchProcessor returns WatchProcessor singleton.
func GetWatchProcessor() WatchProcessor {
	return processor
}

// NewWatchID creates a new watch id UUID string for the specific
// watch client type
func NewWatchID(clientType ClientType) string {
	return fmt.Sprintf("%s_%s", clientType, uuid.New())
}

// NewTaskClient creates a new watch client for task event changes.
// Returns the watch id and a new instance of TaskClient.
func (p *watchProcessor) NewTaskClient(filter *watch.PodFilter) (string, *TaskClient, error) {
	sw := p.metrics.ProcessorLockDuration.Start()
	p.Lock()
	defer p.Unlock()
	sw.Stop()

	if len(p.taskClients) >= p.maxClient {
		return "", nil, yarpcerrors.ResourceExhaustedErrorf("max client reached")
	}

	watchID := NewWatchID(ClientTypeTask)
	p.taskClients[watchID] = &TaskClient{
		Input: make(chan *pod.PodSummary, p.bufferSize),
		// Make buffer size 1 so that sender is not blocked when sending
		// the Signal
		Signal: make(chan StopSignal, 1),
		Filter: filter,
	}

	log.WithField("watch_id", watchID).Info("task watch client created")
	return watchID, p.taskClients[watchID], nil
}

// StopTaskClient stops a task watch client. Returns "not-found" error
// if the corresponding watch client is not found.
func (p *watchProcessor) StopTaskClient(watchID string) error {
	sw := p.metrics.ProcessorLockDuration.Start()
	p.Lock()
	defer p.Unlock()
	sw.Stop()

	return p.stopTaskClient(watchID, StopSignalCancel)
}

func (p *watchProcessor) stopTaskClient(
	watchID string,
	Signal StopSignal,
) error {
	c, ok := p.taskClients[watchID]
	if !ok {
		return yarpcerrors.NotFoundErrorf(
			"watch_id %s not exist for task watch client", watchID)
	}

	log.WithFields(log.Fields{
		"watch_id": watchID,
		"Signal":   Signal,
	}).Info("stopping task watch client")

	c.Signal <- Signal
	delete(p.taskClients, watchID)

	return nil
}

// NotifyTaskChange receives pod event, and notifies all the clients
// which are interested in the pod.
func (p *watchProcessor) NotifyTaskChange(
	pod *pod.PodSummary,
	podLabels []*peloton.Label) {
	sw := p.metrics.ProcessorLockDuration.Start()
	p.Lock()
	defer p.Unlock()
	sw.Stop()

	for watchID, c := range p.taskClients {
		filterCheckPass := true

		if c.Filter != nil {

			// Check the job ID filter
			if c.Filter.GetJobId() != nil {
				jobID, _, err := util.ParseTaskID(pod.GetPodName().GetValue())
				if err != nil {
					// Cannot parse podName to match the jobID, assume that
					// filter does not match.
					filterCheckPass = false
				}

				if jobID != c.Filter.GetJobId().GetValue() {
					// job id filter did not match
					filterCheckPass = false
				}

				// check the podname filter next
				if filterCheckPass == true && len(c.Filter.GetPodNames()) > 0 {
					found := false
					for _, podName := range c.Filter.GetPodNames() {
						if podName.GetValue() == pod.GetPodName().GetValue() {
							found = true
							break
						}
					}
					if found == false {
						// pod name filter did not match
						filterCheckPass = false
					}
				}
			}

			if filterCheckPass == false {
				continue
			}

			// Check the pod label filter next
			for _, labelFilter := range c.Filter.GetLabels() {
				found := false
				for _, labelPod := range podLabels {
					if labelFilter.GetKey() == labelPod.GetKey() &&
						labelFilter.GetValue() == labelPod.GetValue() {
						found = true
						break
					}
				}

				if found == false {
					// label filter did not match
					filterCheckPass = false
					break
				}
			}
		}

		if filterCheckPass == false {
			continue
		}

		select {
		case c.Input <- pod:
		default:
			log.WithField("watch_id", watchID).
				Warn("event overflow for task watch client")
			p.stopTaskClient(watchID, StopSignalOverflow)
		}
	}
}
