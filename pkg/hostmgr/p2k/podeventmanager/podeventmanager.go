package podeventmanager

import (
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	pbevent "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/event"
	pbeventstreamsvc "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/eventstreamsvc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/cirbuf"
	"github.com/uber/peloton/pkg/common/v1alpha/eventstream"
	"github.com/uber/peloton/pkg/hostmgr/p2k/hostcache"
	"github.com/uber/peloton/pkg/hostmgr/p2k/plugins"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

// PodEventManager is responsible to listen to pod events coming in from k8s or
// mesos clusters and adding them to the event stream. It also updates the host
// cache with the pod event.
type PodEventManager interface {
	// GetEvents returns all outstanding pod events in the event stream.
	GetEvents() ([]*pbevent.Event, error)
}

type podEventManagerImpl struct {
	// when enabledStream is true, pod events are sent to eventStreamHandler.
	// TODO: we need this flag because v1alpha event stream is only tested with
	// k8s. Remove it after we can handle both mesos and k8s.
	enabledStream bool

	// v1alpha event stream handler.
	eventStreamHandler *eventstream.Handler

	// Host cache.
	hostCache hostcache.HostCache
}

func (pem *podEventManagerImpl) GetEvents() ([]*pbevent.Event, error) {
	return pem.eventStreamHandler.GetEvents()
}

func (pem *podEventManagerImpl) Run(podEventCh chan *scalar.PodEvent) {
	for e := range podEventCh {
		if pem.enabledStream {
			err := pem.eventStreamHandler.AddEvent(
				&pbevent.Event{
					PodEvent: e.Event,
				})
			if err != nil {
				log.WithField("pod_event", e.Event).Error("add pod event")
			}
		}
		// This should be called so that we handle resource accounting for
		// k8s pods using the pod status. This will be a noop for Mesos.
		pem.hostCache.HandlePodEvent(e)
	}
}

func New(
	d *yarpc.Dispatcher,
	podEventCh chan *scalar.PodEvent,
	plugin plugins.Plugin,
	hostCache hostcache.HostCache,
	bufferSize int,
	parentScope tally.Scope,
	enabledStream bool,
) PodEventManager {
	pem := &podEventManagerImpl{
		enabledStream: enabledStream,
		eventStreamHandler: eventstream.NewEventStreamHandler(
			bufferSize,
			[]string{common.PelotonJobManager, common.PelotonResourceManager},
			purgedEventsProcessor{plugin: plugin},
			parentScope.SubScope("PodEventStreamHandler")),
		hostCache: hostCache,
	}

	d.Register(pbeventstreamsvc.BuildEventStreamServiceYARPCProcedures(pem.eventStreamHandler))

	go pem.Run(podEventCh)
	return pem
}

type purgedEventsProcessor struct {
	plugin plugins.Plugin
}

func (pep purgedEventsProcessor) EventPurged(events []*cirbuf.CircularBufferItem) {
	for _, e := range events {
		event, ok := e.Value.(*pbpod.PodEvent)
		if !ok {
			// Ignore events other than PodEvent for post processing.
			// Also ignore the case where an ack channel is not supplied. This means there is no need to ack event,
			// which is true for k8s cluster.
			continue
		}
		pep.plugin.AckPodEvent(&scalar.PodEvent{Event: event})
	}
}
