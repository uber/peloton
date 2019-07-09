package podeventmanager

import (
	"context"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	pbevent "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/event"
	pbeventstreamsvc "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/eventstreamsvc"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/cirbuf"
	"github.com/uber/peloton/pkg/common/v1alpha/eventstream"
	"github.com/uber/peloton/pkg/hostmgr/p2k/plugins"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
	"go.uber.org/yarpc"
)

type PodEventManager interface {
	// GetEvents returns all outstanding pod events in the event stream.
	GetEvents() ([]*pbevent.Event, error)
}

type podEventManagerImpl struct {
	eventStreamHandler *eventstream.Handler
}

func (pem *podEventManagerImpl) GetEvents() ([]*pbevent.Event, error) {
	return pem.eventStreamHandler.GetEvents()
}

func (pem *podEventManagerImpl) Run(podEventCh chan *scalar.PodEvent) {
	for pe := range podEventCh {
		err := pem.eventStreamHandler.AddEvent(pe.Event)
		if err != nil {
			log.WithField("pod_event", pe).Error("add podevent")
		}
	}
}

func New(
	d *yarpc.Dispatcher,
	podEventCh chan *scalar.PodEvent,
	plugin plugins.Plugin,
	bufferSize int,
	parentScope tally.Scope,
) PodEventManager {
	pem := &podEventManagerImpl{
		eventStreamHandler: eventstream.NewEventStreamHandler(
			bufferSize,
			[]string{common.PelotonJobManager, common.PelotonResourceManager},
			purgedEventsProcessor{plugin: plugin},
			parentScope.SubScope("PodEventStreamHandler")),
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
		pe, ok := e.Value.(*pbpod.PodEvent)
		if !ok {
			log.WithField("event", e).Warn("unexpected event to purge")
			continue
		}

		pep.plugin.AckPodEvent(context.Background(), &scalar.PodEvent{Event: pe})
	}
}
