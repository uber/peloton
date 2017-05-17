package taskupdate

import (
	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/stretchr/testify/assert"
	"go.uber.org/yarpc"

	"code.uber.internal/infra/peloton/common"
)

func TestNotifyTaskStatusUpdate(t *testing.T) {
	resmgrDispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: common.PelotonResourceManager,
	})

	serviceHandler := InitServiceHandler(resmgrDispatcher)
	var events []*eventstream.Event
	for i := 0; i < 100; i++ {
		event := eventstream.Event{
			Offset:          uint64(1000 + i),
			MesosTaskStatus: &mesos_v1.TaskStatus{},
		}
		events = append(events, &event)
	}
	req := &resmgrsvc.NotifyTaskUpdatesRequest{
		Events: events,
	}
	response, _, _ := serviceHandler.NotifyTaskUpdates(nil, nil, req)
	assert.Equal(t, uint64(1099), response.PurgeOffset)
	assert.Nil(t, response.Error)
}
