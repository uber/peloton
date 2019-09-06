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
	"context"
	"strings"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/watch/svc"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

// ServiceHandler implements peloton.api.v1alpha.watch.svc.WatchService
type ServiceHandler struct {
	metrics   *Metrics
	processor WatchProcessor
}

// NewServiceHandler initializes a new instance of ServiceHandler
func NewServiceHandler(
	metrics *Metrics,
	processor WatchProcessor,
) *ServiceHandler {
	return &ServiceHandler{
		metrics:   metrics,
		processor: processor,
	}
}

// InitV1AlphaWatchServiceHandler initializes the Watch Service Handler,
// and registers with yarpc dispatcher.
func InitV1AlphaWatchServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	config Config,
) WatchProcessor {
	InitWatchProcessor(config, parent)
	processor := GetWatchProcessor()

	handler := NewServiceHandler(NewMetrics(parent), processor)
	d.Register(svc.BuildWatchServiceYARPCProcedures(handler))

	return processor
}

// watchPod implements the watch handler for pod events.
func (h *ServiceHandler) watchPod(
	req *svc.WatchRequest,
	stream svc.WatchServiceServiceWatchYARPCServer,
) error {
	log.WithField("request", req).
		Debug("starting new pod watch")

	watchID, watchClient, err := h.processor.NewTaskClient(req.GetPodFilter())
	if err != nil {
		log.WithError(err).
			Warn("failed to create pod watch client")
		return err
	}

	defer func() {
		h.processor.StopTaskClient(watchID)
	}()

	initResp := &svc.WatchResponse{
		WatchId: watchID,
	}
	if err := stream.Send(initResp); err != nil {
		log.WithField("watch_id", watchID).
			WithError(err).
			Warn("failed to send initial response for pod watch")
		return err
	}

	for {
		select {
		case p := <-watchClient.Input:
			resp := &svc.WatchResponse{
				WatchId: watchID,
				Pods:    []*pod.PodSummary{p},
			}
			if err := stream.Send(resp); err != nil {
				log.WithField("watch_id", watchID).
					WithError(err).
					Warn("failed to send response for pod watch")
				return err
			}
		case s := <-watchClient.Signal:
			log.WithFields(log.Fields{
				"watch_id": watchID,
				"signal":   s,
			}).Debug("received signal")

			err := handleSignal(
				watchID,
				s,
				map[StopSignal]tally.Counter{
					StopSignalCancel:   h.metrics.WatchPodCancel,
					StopSignalOverflow: h.metrics.WatchPodOverflow,
				},
			)

			if !yarpcerrors.IsCancelled(err) {
				log.WithField("watch_id", watchID).
					WithError(err).
					Warn("watch stopped due to signal")
			}

			return err
		}
	}
}

// watchJob implements the watch handler for job events.
func (h *ServiceHandler) watchJob(
	req *svc.WatchRequest,
	stream svc.WatchServiceServiceWatchYARPCServer,
) error {
	log.WithField("request", req).
		Debug("starting new job watch")

	watchID, watchClient, err := h.processor.NewJobClient(req.GetStatelessJobFilter())
	if err != nil {
		log.WithError(err).
			Warn("failed to create job watch client")
		return err
	}

	defer func() {
		h.processor.StopJobClient(watchID)
	}()

	initResp := &svc.WatchResponse{
		WatchId: watchID,
	}
	if err := stream.Send(initResp); err != nil {
		log.WithField("watch_id", watchID).
			WithError(err).
			Warn("failed to send initial response for job watch")
		return err
	}

	for {
		select {
		case j := <-watchClient.Input:
			resp := &svc.WatchResponse{
				WatchId:       watchID,
				StatelessJobs: []*stateless.JobSummary{j},
			}
			if err := stream.Send(resp); err != nil {
				log.WithField("watch_id", watchID).
					WithError(err).
					Warn("failed to send response for job watch")
				return err
			}
		case s := <-watchClient.Signal:
			log.WithFields(log.Fields{
				"watch_id": watchID,
				"signal":   s,
			}).Debug("received signal")

			err := handleSignal(
				watchID,
				s,
				map[StopSignal]tally.Counter{
					StopSignalCancel:   h.metrics.WatchJobCancel,
					StopSignalOverflow: h.metrics.WatchJobOverflow,
				},
			)

			if !yarpcerrors.IsCancelled(err) {
				log.WithField("watch_id", watchID).
					WithError(err).
					Warn("watch stopped due to signal")
			}

			return err
		}
	}
}

// Watch creates a watch to get notified about changes to Peloton objects.
// Changed objects are streamed back to the caller till the watch is
// cancelled.
func (h *ServiceHandler) Watch(
	req *svc.WatchRequest,
	stream svc.WatchServiceServiceWatchYARPCServer,
) error {
	// Create watch for pod
	if req.GetPodFilter() != nil {
		return h.watchPod(req, stream)
	}

	// Create watch for job
	if req.GetStatelessJobFilter() != nil {
		return h.watchJob(req, stream)
	}

	err := yarpcerrors.InvalidArgumentErrorf("not supported watch type")
	log.Warn("not supported watch type")
	return err
}

// handleSignal converts StopSignal to appropriate yarpcerror
func handleSignal(
	watchID string,
	s StopSignal,
	metrics map[StopSignal]tally.Counter,
) error {
	c := metrics[s]
	if c != nil {
		c.Inc(1)
	}

	switch s {
	case StopSignalCancel:
		return yarpcerrors.CancelledErrorf("watch cancelled: %s", watchID)
	case StopSignalOverflow:
		return yarpcerrors.AbortedErrorf("event overflow: %s", watchID)
	default:
		return yarpcerrors.InternalErrorf("unexpected signal: %s", s)
	}
}

// Cancel cancels a watch. The watch stream will get an error indicating
// watch was cancelled and the stream will be closed.
func (h *ServiceHandler) Cancel(
	ctx context.Context,
	req *svc.CancelRequest,
) (*svc.CancelResponse, error) {
	watchID := req.GetWatchId()

	if strings.HasPrefix(watchID, ClientTypeTask.String()) {
		err := h.processor.StopTaskClient(watchID)
		if err != nil {
			if yarpcerrors.IsNotFound(err) {
				h.metrics.CancelNotFound.Inc(1)
			}

			log.WithField("watch_id", watchID).
				WithError(err).
				Warn("failed to stop task client")

			return nil, err
		}

		return &svc.CancelResponse{}, nil
	}

	err := yarpcerrors.NotFoundErrorf("invalid watch id")
	log.WithFields(log.Fields{
		"watch_id": watchID,
	}).Warn("invalid watch id")
	return nil, err
}

// NewTestServiceHandler returns an empty new ServiceHandler ptr for testing.
func NewTestServiceHandler() *ServiceHandler {
	return &ServiceHandler{}
}
