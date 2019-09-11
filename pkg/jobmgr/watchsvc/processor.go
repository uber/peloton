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

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
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

	// StopTaskClients stops all the task clients on leadership change.
	StopTaskClients()

	// NotifyPodChange receives pod event, and notifies all the clients
	// which are interested in the pod.
	NotifyPodChange(pod *pod.PodSummary, podLabels []*peloton.Label)

	// NewJobClient creates a new watch client for job event changes.
	// Returns the watch id and an new instance of JobClient.
	NewJobClient(filter *watch.StatelessJobFilter) (string, *JobClient, error)

	// StopJobClient stops a job watch client. Returns "not-found" error
	// if the corresponding watch client is not found.
	StopJobClient(watchID string) error

	// StopJobClients stops all the job clients on leadership change.
	StopJobClients()

	// NotifyJobChange receives job event, and notifies all the clients
	// which are interested in the job.
	NotifyJobChange(job *stateless.JobSummary)
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

type podFilter struct {
	jobID    string
	podNames map[string]struct{}
	labels   []*peloton.Label
}

// TaskClient represents a client which interested in task event changes.
type TaskClient struct {
	Input  chan *pod.PodSummary
	Signal chan StopSignal

	filter *podFilter
}

type jobFilter struct {
	jobIDs map[string]struct{}
	labels []*peloton.Label
}

// JobClient represents a client which interested in job event changes.
type JobClient struct {
	Input  chan *stateless.JobSummary
	Signal chan StopSignal

	filter *jobFilter
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

	podFilter := &podFilter{}
	if filter != nil {
		if filter.GetJobId() != nil {
			podFilter.jobID = filter.GetJobId().GetValue()
		}

		if len(filter.GetPodNames()) > 0 {
			podFilter.podNames = map[string]struct{}{}
			for _, podName := range filter.GetPodNames() {
				podFilter.podNames[podName.GetValue()] = struct{}{}
			}
		}

		if len(filter.GetLabels()) > 0 {
			podFilter.labels = filter.GetLabels()
		}
	}

	watchID := NewWatchID(ClientTypeTask)
	p.taskClients[watchID] = &TaskClient{
		Input: make(chan *pod.PodSummary, p.bufferSize),
		// Make buffer size 1 so that sender is not blocked when sending
		// the Signal
		Signal: make(chan StopSignal, 1),
		filter: podFilter,
	}

	log.WithField("watch_id", watchID).
		WithField("filter", filter).
		Info("task watch client created")
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

// StopTaskClients stops all the task clients on job manager leader change
func (p *watchProcessor) StopTaskClients() {
	sw := p.metrics.ProcessorLockDuration.Start()
	p.Lock()
	defer p.Unlock()
	sw.Stop()

	for watchID := range p.taskClients {
		p.stopTaskClient(watchID, StopSignalCancel)
	}
}

func (p *watchProcessor) stopTaskClient(
	watchID string,
	signal StopSignal,
) error {
	c, ok := p.taskClients[watchID]
	if !ok {
		return yarpcerrors.NotFoundErrorf(
			"watch_id %s not exist for task watch client", watchID)
	}

	log.WithFields(log.Fields{
		"watch_id": watchID,
		"signal":   signal,
	}).Info("stopping task watch client")

	c.Signal <- signal
	delete(p.taskClients, watchID)

	return nil
}

// NotifyPodChange receives pod event, and notifies all the clients
// which are interested in the pod.
func (p *watchProcessor) NotifyPodChange(
	pod *pod.PodSummary,
	podLabels []*peloton.Label) {
	sw := p.metrics.ProcessorLockDuration.Start()
	p.Lock()
	defer p.Unlock()
	sw.Stop()

	for watchID, c := range p.taskClients {
		filterCheckPass := func() bool {
			if c.filter != nil {
				// Check job ID filter
				if len(c.filter.jobID) > 0 {
					jobID, _, err := util.ParseTaskID(pod.GetPodName().GetValue())
					if err != nil {
						// Cannot parse podName to match the jobID, assume that
						// filter does not match.
						return false
					}

					if jobID != c.filter.jobID {
						// job id filter did not match
						return false
					}
				}

				// Check pod name filter
				if len(c.filter.podNames) > 0 {
					if _, ok := c.filter.podNames[pod.GetPodName().GetValue()]; !ok {
						return false
					}
				}

				// Check pod label filter
				for _, labelFilter := range c.filter.labels {
					found := false
					for _, labelPod := range podLabels {
						if labelFilter.GetKey() == labelPod.GetKey() &&
							labelFilter.GetValue() == labelPod.GetValue() {
							found = true
							break
						}
					}

					if !found {
						// label filter did not match
						return false
					}
				}
			}

			return true
		}()

		if !filterCheckPass {
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

// NewJobClient creates a new watch client for job event changes.
// Returns the watch id and an new instance of JobClient.
func (p *watchProcessor) NewJobClient(filter *watch.StatelessJobFilter) (string, *JobClient, error) {
	sw := p.metrics.ProcessorLockDuration.Start()
	p.Lock()
	defer p.Unlock()
	sw.Stop()

	if len(p.jobClients) >= p.maxClient {
		return "", nil, yarpcerrors.ResourceExhaustedErrorf("max client reached")
	}

	jobFilter := &jobFilter{}
	if filter != nil {
		if len(filter.GetJobIds()) > 0 {
			jobFilter.jobIDs = map[string]struct{}{}
			for _, jobID := range filter.GetJobIds() {
				jobFilter.jobIDs[jobID.GetValue()] = struct{}{}
			}
		}

		if len(filter.GetLabels()) > 0 {
			jobFilter.labels = filter.GetLabels()
		}
	}

	watchID := NewWatchID(ClientTypeJob)
	p.jobClients[watchID] = &JobClient{
		Input: make(chan *stateless.JobSummary, p.bufferSize),
		// Make buffer size 1 so that sender is not blocked when sending
		// the Signal
		Signal: make(chan StopSignal, 1),
		filter: jobFilter,
	}

	log.WithField("watch_id", watchID).
		WithField("filter", filter).
		Info("job watch client created")
	return watchID, p.jobClients[watchID], nil
}

// StopJobClient stops a job watch client. Returns "not-found" error
// if the corresponding watch client is not found.
func (p *watchProcessor) StopJobClient(watchID string) error {
	sw := p.metrics.ProcessorLockDuration.Start()
	p.Lock()
	defer p.Unlock()
	sw.Stop()

	return p.stopJobClient(watchID, StopSignalCancel)
}

// StopTaskClients stops all the task clients on job manager leader change
func (p *watchProcessor) StopJobClients() {
	sw := p.metrics.ProcessorLockDuration.Start()
	p.Lock()
	defer p.Unlock()
	sw.Stop()

	for watchID := range p.jobClients {
		p.stopJobClient(watchID, StopSignalCancel)
	}
}

func (p *watchProcessor) stopJobClient(
	watchID string,
	signal StopSignal,
) error {
	c, ok := p.jobClients[watchID]
	if !ok {
		return yarpcerrors.NotFoundErrorf(
			"watch_id %s not exist for job watch client", watchID)
	}

	log.WithFields(log.Fields{
		"watch_id": watchID,
		"Signal":   signal,
	}).Info("stopping job watch client")

	c.Signal <- signal
	delete(p.jobClients, watchID)

	return nil
}

// NotifyJobChange receives job event, and notifies all the clients
// which are interested in the job.
func (p *watchProcessor) NotifyJobChange(job *stateless.JobSummary) {
	sw := p.metrics.ProcessorLockDuration.Start()
	p.Lock()
	defer p.Unlock()
	sw.Stop()

	for watchID, c := range p.jobClients {
		filterCheckPass := func() bool {
			if c.filter != nil {
				// Check job IDs filter
				if len(c.filter.jobIDs) > 0 {
					jobID := job.GetJobId().GetValue()
					if _, ok := c.filter.jobIDs[jobID]; !ok {
						return false
					}
				}

				// Check job label filter
				for _, labelFilter := range c.filter.labels {
					found := false
					for _, labelJob := range job.GetLabels() {
						if labelFilter.GetKey() == labelJob.GetKey() &&
							labelFilter.GetValue() == labelJob.GetValue() {
							found = true
							break
						}
					}

					if !found {
						return false
					}
				}
			}

			return true
		}()

		if !filterCheckPass {
			continue
		}

		select {
		case c.Input <- job:
		default:
			log.WithField("watch_id", watchID).
				Warn("event overflow for job watch client")
			p.stopJobClient(watchID, StopSignalOverflow)
		}
	}
}
