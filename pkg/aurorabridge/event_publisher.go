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

package aurorabridge

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/Jeffail/gabs"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/ptoa"

	statelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	podsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/watch"
	watchsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/watch/svc"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/pkg/common/util"

	log "github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

const (
	// context timeout to initiate the watch stream
	rpcTimeout = 60 * time.Second

	// ensures that pod state chage watch is running, on leader
	// change or network issue
	ensureWatchRunningPeriod = 10 * time.Second

	// time to retry on creating
	// watch stream again after failure
	watchFailureBackoff = 2 * time.Second

	// podBuckets defines number of buckets to create for pods
	// summaries to publish
	podBuckets = 25

	// podBucketSize defines number of pod summaries to buffer without
	// blocking the watch stream to add more pods.
	podBucketSize = 1000
)

// EventPublisher sets up a watch on pod state change event and then
// publishes them to kafka stream.
type EventPublisher interface {
	// Start the watch on pod state changes and
	// publish of kafka
	Start()

	// Stop the watch on pod state changes and
	// publishing to kafka
	Stop()
}

type podEvent struct {
	pod       *pod.PodSummary
	timestamp string
}

type eventPublisher struct {
	jobClient   statelesssvc.JobServiceYARPCClient
	podClient   podsvc.PodServiceYARPCClient
	watchClient watchsvc.WatchServiceYARPCClient

	// channels to block for cleanup on lost leadership
	stopEnsureWatchPod chan struct{}
	stopWatchPod       chan struct{}

	// flag to identify is process is leader or not
	elected atomic.Bool

	// isEnsureWatchPodRunning drives to desired state of always making sure
	// steam to watch pod state change is running
	isEnsureWatchPodRunning atomic.Bool

	// isWatchPodRunning flag indicates whether watch pod go-routine
	// is running to receive the pod state changes.
	isWatchPodRunning atomic.Bool

	// isPublisherWorkersRunning ensures the pod event publisher workers
	// are spawed once only for entire lifecycle
	isPublisherWorkersRunning atomic.Bool

	// publish events is a flag to determine whether to publish task state
	// change events to kafka or not
	publishEvents bool

	// kafka rest proxy url
	kafkaURL string

	// http client to post pod state changes on kafka-rest-proxy
	client *http.Client

	// the buckets for pods, each bucket persists pod events for same pod
	// in order they were received from watch stream
	buckets []chan *podEvent
}

// NewEventPublisher return event publisher to stream pod state changes
// to kafka
func NewEventPublisher(
	kafkaURL string,
	jobClient statelesssvc.JobServiceYARPCClient,
	podClient podsvc.PodServiceYARPCClient,
	watchClient watchsvc.WatchServiceYARPCClient,
	client *http.Client,
	publishEvents bool,
) EventPublisher {

	return &eventPublisher{
		jobClient:          jobClient,
		podClient:          podClient,
		watchClient:        watchClient,
		kafkaURL:           kafkaURL,
		client:             client,
		publishEvents:      publishEvents,
		stopEnsureWatchPod: make(chan struct{}),
		stopWatchPod:       make(chan struct{}),
	}
}

// Start starts the event publisher by setting up watch on
// pod state change and then publishing them to kafka
func (e *eventPublisher) Start() {
	if !e.publishEvents {
		return
	}

	if !e.isPublisherWorkersRunning.Swap(true) {
		e.buckets = make([]chan *podEvent, podBuckets)

		for i := 0; i < podBuckets; i++ {
			e.buckets[i] = make(chan *podEvent, podBucketSize)
			go func(bucket chan *podEvent) {
				for podEvent := range bucket {
					if podEvent.pod.GetStatus().GetState() == pod.PodState_POD_STATE_DELETED ||
						podEvent.pod.GetStatus().GetState() == pod.PodState_POD_STATE_INITIALIZED {
						// pod is initialized or deleted, skip
						continue
					}
					e.publishEvent(podEvent.pod, podEvent.timestamp)
				}
			}(e.buckets[i])
		}
	}

	log.Info("Start event publisher")
	e.elected.Store(true)
	go e.ensureWatchPodRunning()
}

// Stop terminates the watch for pod state change event
func (e *eventPublisher) Stop() {
	e.elected.Store(false)
	if !e.publishEvents {
		return
	}

	if e.isWatchPodRunning.Load() {
		e.stopWatchPod <- struct{}{}
	}

	if e.isEnsureWatchPodRunning.Load() {
		e.stopEnsureWatchPod <- struct{}{}
	}

	if e.isPublisherWorkersRunning.Swap(false) {
		for i := 0; i < podBuckets; i++ {
			close(e.buckets[i])
		}
	}

	log.Info("Stop event publisher")
}

// ensures that the watch pod is always running
func (e *eventPublisher) ensureWatchPodRunning() {
	if e.isEnsureWatchPodRunning.Swap(true) {
		// another ensureWatchPodRunning is already started
		return
	}
	defer e.isEnsureWatchPodRunning.Store(false)
	log.Info("Starting ensureWatchPodRunning")

	ctx, cancelFunc := context.WithTimeout(
		context.Background(),
		rpcTimeout)
	defer cancelFunc()

	var stream watchsvc.WatchServiceServiceWatchYARPCClient
	var err error

	for {
		if e.elected.Load() && !e.isWatchPodRunning.Load() {
			// TODO (varung): Add pod filter to watch on desired labels
			stream, err = e.watchClient.Watch(
				ctx,
				&watchsvc.WatchRequest{
					PodFilter: &watch.PodFilter{
						Labels: []*peloton.Label{
							common.BridgePodLabel,
						},
					},
				},
			)
			if err != nil {
				log.WithError(err).Error("failed start watch pod state change event")

				// TODO (varung): Explore the option of exponential backoff
				time.Sleep(watchFailureBackoff)
				continue
			}

			log.Info("Starting watchPod")
			go e.watchPod(stream)
		}

		select {
		case <-e.stopEnsureWatchPod:
			if err := stream.CloseSend(); err != nil {
				log.WithError(err).Error("error on closing the stream")
			}
			log.Info("Stopping ensureWatchPodRunning")
			return
		default:
			time.Sleep(ensureWatchRunningPeriod)
		}
	}
}

// watch for pod state change events and handles toggles watchRunning
// flag to indicate stream is closed either due to job manager leader change
// or network issue which indicates ensureWatchPodRunning method
// to start watch again
func (e *eventPublisher) watchPod(
	stream watchsvc.WatchServiceServiceWatchYARPCClient,
) {
	if e.isWatchPodRunning.Swap(true) {
		// another watchPod is already started
		return
	}
	defer e.isWatchPodRunning.Store(false)

	for {
		// TODO (varung): in a lost leadership scenario, receive pod will block
		// exiting the go routine until it receives a event
		msg, ok := e.receivePod(stream)
		if !ok {
			log.Info("Stopping watchPod")
			return
		}

		for _, pod := range msg.GetPods() {
			podName := pod.GetPodName().GetValue()
			index := common.Hash(podName) % uint32(podBuckets)
			e.buckets[index] <- &podEvent{
				pod:       pod,
				timestamp: time.Now().UTC().Format(time.RFC3339),
			}
		}

		select {
		case <-e.stopWatchPod:
			log.Info("Stopping watchPod")
			return
		default:
		}
	}
}

// receive a message on pod state change event stream
func (e *eventPublisher) receivePod(
	stream watchsvc.WatchServiceServiceWatchYARPCClient,
) (*watchsvc.WatchResponse, bool) {
	msg, err := stream.Recv()

	switch err {
	case nil:
		return msg, true
	case io.EOF:
		log.Info("stream EOF reached")
	default:
		log.WithError(err).Error("error reading from stream")
	}

	return nil, false
}

type taskStateChange struct {
	Task     *api.ScheduledTask  `json:"task,omitempty"`
	OldState *api.ScheduleStatus `json:"oldState,omitempty"`
}

// publishes the pod state change event to kafka
func (e *eventPublisher) publishEvent(
	podSummary *pod.PodSummary,
	receivedTimestamp string,
) {
	podName := podSummary.GetPodName()
	podID := podSummary.GetStatus().GetPodId()

	logFields := log.WithFields(log.Fields{
		"pod_id":     podName.GetValue(),
		"pod_status": podID.GetValue(),
	})

	logFields.Debug("received pod state change event")

	task, err := e.getTaskStateChange(podSummary, receivedTimestamp)
	if err != nil {
		logFields.WithError(err).Error("unable to get task state change")
		return
	}

	message, err := json.Marshal(task)
	if err != nil {
		logFields.WithError(err).Error("unable to marshal task state change")
		return
	}

	// Modify json string to match aurora
	fixedMessage, err := modifyTaskStateChangeJSON(message)
	if err != nil {
		logFields.WithError(err).Error("unable to fix task state change json string")
		// fallback to original message if failed to fix
		fixedMessage = message
	}

	if err := e.postToKafkaRestProxy(fixedMessage); err != nil {
		logFields.WithError(err).Error("unable to write to kafka")
		return
	}

	logFields.Debug("successful publish to kafka")
}

// gets task state event to publish
func (e *eventPublisher) getTaskStateChange(
	podSummary *pod.PodSummary,
	receivedTimestamp string,
) (*taskStateChange, error) {
	podName := podSummary.GetPodName()

	// Get JobInfo
	ctx, cancelFunc := context.WithTimeout(
		context.Background(),
		rpcTimeout)
	defer cancelFunc()
	jobID, _, err := util.ParseTaskID(podName.GetValue())
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse pod name to derive jobID")
	}

	jobReq := &statelesssvc.GetJobRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		SummaryOnly: true,
	}
	jobSummary, err := e.jobClient.GetJob(ctx, jobReq)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get job info")
	}

	// Get PodInfo
	ctx, cancelFunc = context.WithTimeout(
		context.Background(),
		rpcTimeout)
	defer cancelFunc()
	infoReq := &podsvc.GetPodRequest{
		PodName: podName,
		Limit:   1,
	}
	podInfo, err := e.podClient.GetPod(ctx, infoReq)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get pod info")
	}

	// Construct PodEvent from received PodSummary
	podEvents := []*pod.PodEvent{
		convertPodStatusToPodEvent(podSummary.GetStatus(), receivedTimestamp),
	}

	// Get ScheduledTask
	task, err := ptoa.NewScheduledTask(
		jobSummary.GetSummary(),
		podInfo.GetCurrent().GetSpec(),
		podEvents,
	)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get scheduled task")
	}

	return &taskStateChange{
		Task:     task,
		OldState: task.GetStatus().Ptr(), // dummy value
	}, nil
}

// modifyTaskStateChangeJSON fixes json serialization difference between
// thriftrw implementation and aurora. Specifically,
//
// thriftrw serializes TaskStateChange object into json string:
// {
//    "task": {
//        "assignedTask": {
//            "task": {
//                "container": {
//                    "docker": {
//                        "image": "image",
//                        "parameters": [{
//                            "name": "name",
//                            "value": "value"
//                        }]
//                    }
//                }
//            }
//        }
//    }
// }
//
// aurora serializes the object into json string:
// {
//    "task": {
//        "assignedTask": {
//            "task": {
//                "container": {
//                    "setField": "DOCKER",
//                    "value": {
//                        "image": "image",
//                        "parameters": [{
//                            "name": "name",
//                            "value": "value"
//                        }]
//                    }
//                }
//            }
//        }
//    }
// }
func modifyTaskStateChangeJSON(data []byte) ([]byte, error) {
	// Deserialize json string into gabs container
	taskStateChange, err := gabs.ParseJSON(data)
	if err != nil {
		return nil, err
	}

	// Parse aurora container object inside TaskConfig
	container := taskStateChange.Path("task.assignedTask.task.container")
	dockerContainer := container.Path("docker")
	mesosContainer := container.Path("mesos")

	var containerValue *gabs.Container
	var containerType string
	if dockerContainer != nil {
		containerValue = dockerContainer
		containerType = "DOCKER"
	} else if mesosContainer != nil {
		containerValue = mesosContainer
		containerType = "MESOS"
	} else {
		// nothing to fix, return original
		return data, nil
	}

	// Create a new json object to overwrite existing
	newContainer, err := taskStateChange.ObjectP("task.assignedTask.task.container")
	if err != nil {
		return nil, err
	}

	newContainer.Set(containerType, "setField")
	newContainer.Set(containerValue.Data(), "value")

	// Serialize modified json object into string
	return taskStateChange.Bytes(), nil
}

// post to kafka rest proxy
func (e *eventPublisher) postToKafkaRestProxy(message []byte) error {
	req, err := http.NewRequest("POST", e.kafkaURL, bytes.NewBuffer(message))
	req.Header.Set("Producer-Type", "reliable")
	req.Header.Set("Content-Type", "application/vnd.kafka.binary.v1")

	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return err
	}

	return nil
}

// convertPodStatusToPodEvent converts PodStatus object to PodEvent.
func convertPodStatusToPodEvent(
	podStatus *pod.PodStatus,
	receivedTimestamp string,
) *pod.PodEvent {
	return &pod.PodEvent{
		PodId:          podStatus.GetPodId(),
		ActualState:    podStatus.GetState().String(),
		DesiredState:   podStatus.GetDesiredState().String(),
		Timestamp:      receivedTimestamp,
		Version:        podStatus.GetVersion(),
		DesiredVersion: podStatus.GetDesiredVersion(),
		AgentId:        podStatus.GetHostId(),
		Hostname:       podStatus.GetHost(),
		Message:        podStatus.GetMessage(),
		Reason:         podStatus.GetReason(),
		PrevPodId:      podStatus.GetPrevPodId(),
		Healthy:        "", // TODO(kxu): to be filled
		DesiredPodId:   podStatus.GetDesiredPodId(),
	}
}
