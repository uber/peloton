package hostmgr

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	mesos "mesos/v1"
	sched "mesos/v1/scheduler"

	"peloton/api/task/config"
	"peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/hostmgr/offer"
	"code.uber.internal/infra/peloton/util"
	mpb_mocks "code.uber.internal/infra/peloton/yarpc/encoding/mpb/mocks"
)

const (
	offerHoldTime time.Duration = time.Minute * 5
	agent1                      = "agent-1"
	hostname1                   = "hostname1"
	streamID                    = "streamID"
	frameworkID                 = "frameworkID"
)

var (
	rootCtx = context.Background()
)

// A mock implementation of FrameworkInfoProvider
type mockFrameworkInfoProvider struct{}

func (m *mockFrameworkInfoProvider) GetMesosStreamID() string {
	return streamID
}

func (m *mockFrameworkInfoProvider) GetFrameworkID() *mesos.FrameworkID {
	tmp := frameworkID
	return &mesos.FrameworkID{Value: &tmp}
}

func generateOffers(numOffers int) []*mesos.Offer {
	var offers []*mesos.Offer
	for i := 0; i < numOffers; i++ {
		oid := fmt.Sprintf("offer-%d", i)
		aid := fmt.Sprintf("agent-%d", i)
		hostname := fmt.Sprintf("hostname-%d", i)
		offers = append(offers, &mesos.Offer{
			Id:       &mesos.OfferID{Value: &oid},
			AgentId:  &mesos.AgentID{Value: &aid},
			Hostname: &hostname,
			Resources: []*mesos.Resource{
				util.NewMesosResourceBuilder().
					WithName("cpus").
					WithValue(10).
					Build(),
				util.NewMesosResourceBuilder().
					WithName("mem").
					WithValue(10).
					Build(),
				util.NewMesosResourceBuilder().
					WithName("disk").
					WithValue(10).
					Build(),
			},
		})
	}
	return offers
}

func generateLaunchableTasks(numTasks int) []*hostsvc.LaunchableTask {
	var tasks []*hostsvc.LaunchableTask
	for i := 0; i < numTasks; i++ {
		tid := fmt.Sprintf(taskIDFmt, i)
		tmpCmd := defaultCmd
		tasks = append(tasks, &hostsvc.LaunchableTask{
			TaskId: &mesos.TaskID{Value: &tid},
			Config: &config.TaskConfig{
				Name:     fmt.Sprintf("name-%d", i),
				Resource: &defaultResourceConfig,
				Command: &mesos.CommandInfo{
					Value: &tmpCmd,
				},
			},
		})
	}
	return tasks
}

// This checks the happy case of acquire -> release -> acquire
// sequence and verifies that released resources can be used
// again by next acquire call.
func TestAcquireReleaseHostOffers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testScope := tally.NewTestScope("", map[string]string{})
	client := mpb_mocks.NewMockClient(ctrl)
	pool := offer.NewOfferPool(offerHoldTime, client)
	numOffers := 5
	h := &serviceHandler{
		client:    client,
		metrics:   NewMetrics(testScope),
		offerPool: pool,
	}

	pool.AddOffers(generateOffers(numOffers))

	// Empty constraint.
	acquiredResp, _, err := h.AcquireHostOffers(
		rootCtx,
		nil,
		&hostsvc.AcquireHostOffersRequest{
			Constraints: []*hostsvc.Constraint{},
		},
	)

	assert.NoError(t, err)
	assert.NotNil(t, acquiredResp.GetError().GetInvalidConstraints())

	assert.Equal(
		t,
		int64(1),
		testScope.Snapshot().Counters()["acquire_host_offers_invalid"].Value())

	// Matching constraint.
	acquireReq := &hostsvc.AcquireHostOffersRequest{
		Constraints: []*hostsvc.Constraint{
			{
				Limit: uint32(numOffers * 2),
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: &config.ResourceConfig{
						CpuLimit:    10,
						MemLimitMb:  10,
						DiskLimitMb: 10,
					},
				},
			},
		},
	}
	acquiredResp, _, err = h.AcquireHostOffers(
		rootCtx,
		nil,
		acquireReq,
	)

	assert.NoError(t, err)
	assert.Nil(t, acquiredResp.GetError())
	acquiredHostOffers := acquiredResp.GetHostOffers()
	assert.Equal(t, numOffers, len(acquiredHostOffers))

	assert.Equal(
		t,
		int64(1),
		testScope.Snapshot().Counters()["acquire_host_offers"].Value())

	// TODO: Add check for number of HostOffers in placing state.

	// Call AcquireHostOffers again should not give us anything.
	acquiredResp, _, err = h.AcquireHostOffers(
		rootCtx,
		nil,
		acquireReq,
	)

	assert.NoError(t, err)
	assert.Nil(t, acquiredResp.GetError())
	assert.Equal(t, 0, len(acquiredResp.GetHostOffers()))

	assert.Equal(
		t,
		int64(2),
		testScope.Snapshot().Counters()["acquire_host_offers"].Value())

	// Release previously acquired host offers.

	releaseReq := &hostsvc.ReleaseHostOffersRequest{
		HostOffers: acquiredHostOffers,
	}

	releaseResp, _, err := h.ReleaseHostOffers(
		rootCtx,
		nil,
		releaseReq,
	)

	assert.NoError(t, err)
	assert.Nil(t, releaseResp.GetError())

	// TODO: Add check for number of HostOffers in placing state.
	assert.Equal(
		t,
		int64(1),
		testScope.Snapshot().Counters()["release_host_offers"].Value())

	// Acquire again should return non empty result.
	acquiredResp, _, err = h.AcquireHostOffers(
		rootCtx,
		nil,
		acquireReq,
	)

	assert.NoError(t, err)
	assert.Nil(t, acquiredResp.GetError())
	assert.Equal(t, numOffers, len(acquiredResp.GetHostOffers()))
}

// This checks the happy case of acquire -> launch
// sequence.
func TestAcquireAndLaunch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testScope := tally.NewTestScope("", map[string]string{})
	client := mpb_mocks.NewMockClient(ctrl)
	pool := offer.NewOfferPool(offerHoldTime, client)
	// only create one host offer in this test.
	numOffers := 1
	provider := &mockFrameworkInfoProvider{}
	h := &serviceHandler{
		client:                client,
		metrics:               NewMetrics(testScope),
		offerPool:             pool,
		frameworkInfoProvider: provider,
	}

	pool.AddOffers(generateOffers(numOffers))

	// Matching constraint.
	acquireReq := &hostsvc.AcquireHostOffersRequest{
		Constraints: []*hostsvc.Constraint{
			{
				Limit: uint32(1),
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: &config.ResourceConfig{
						CpuLimit:    10,
						MemLimitMb:  10,
						DiskLimitMb: 10,
					},
				},
			},
		},
	}
	acquiredResp, _, err := h.AcquireHostOffers(
		rootCtx,
		nil,
		acquireReq,
	)

	assert.NoError(t, err)
	assert.Nil(t, acquiredResp.GetError())
	acquiredHostOffers := acquiredResp.GetHostOffers()
	assert.Equal(t, 1, len(acquiredHostOffers))

	assert.Equal(
		t,
		int64(1),
		testScope.Snapshot().Counters()["acquire_host_offers"].Value())

	// TODO: Add check for number of HostOffers in placing state.

	// An empty launch request will trigger an error.
	launchReq := &hostsvc.LaunchTasksRequest{
		Hostname: acquiredHostOffers[0].GetHostname(),
		AgentId:  acquiredHostOffers[0].GetAgentId(),
		Tasks:    []*hostsvc.LaunchableTask{},
	}

	launchResp, _, err := h.LaunchTasks(
		rootCtx,
		nil,
		launchReq,
	)

	assert.NoError(t, err)
	assert.NotNil(t, launchResp.GetError().GetInvalidArgument())

	assert.Equal(
		t,
		int64(1),
		testScope.Snapshot().Counters()["launch_tasks_invalid"].Value())

	// Generate some launchable tasks.
	launchReq.Tasks = generateLaunchableTasks(1)

	gomock.InOrder(
		client.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any(),
			).
			Do(func(_ string, msg proto.Message) {
				// Verify call message.
				call := msg.(*sched.Call)
				assert.Equal(t, sched.Call_ACCEPT, call.GetType())
				assert.Equal(t, provider.GetFrameworkID(), call.GetFrameworkId())

				accept := call.GetAccept()
				assert.NotNil(t, accept)
				assert.Equal(t, 1, len(accept.GetOfferIds()))
				assert.Equal(t, "offer-0", accept.GetOfferIds()[0].GetValue())
				assert.Equal(t, 1, len(accept.GetOperations()))
				operation := accept.GetOperations()[0]
				assert.Equal(t, mesos.Offer_Operation_LAUNCH, operation.GetType())
				launch := operation.GetLaunch()
				assert.NotNil(t, launch)
				assert.Equal(t, 1, len(launch.GetTaskInfos()))
				assert.Equal(t, fmt.Sprintf(taskIDFmt, 0), launch.GetTaskInfos()[0].GetTaskId().GetValue())
			}).
			Return(nil),
	)

	launchResp, _, err = h.LaunchTasks(
		rootCtx,
		nil,
		launchReq,
	)

	assert.NoError(t, err)
	assert.Nil(t, launchResp.GetError())
	assert.Equal(
		t,
		int64(1),
		testScope.Snapshot().Counters()["launch_tasks"].Value())
}

// Test happy case of killing task
func TestKillTask(t *testing.T) {
	t1 := "t1"
	t2 := "t2"
	taskIDs := []*mesos.TaskID{
		{Value: &t1},
		{Value: &t2},
	}
	killReq := &hostsvc.KillTasksRequest{
		TaskIds: taskIDs,
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testScope := tally.NewTestScope("", map[string]string{})
	client := mpb_mocks.NewMockClient(ctrl)
	pool := offer.NewOfferPool(offerHoldTime, client)
	provider := &mockFrameworkInfoProvider{}
	h := &serviceHandler{
		client:                client,
		metrics:               NewMetrics(testScope),
		offerPool:             pool,
		frameworkInfoProvider: provider,
	}

	killedTaskIds := make(map[string]bool)
	mockMutex := &sync.Mutex{}

	gomock.InOrder(
		client.EXPECT().
			Call(
				gomock.Eq(streamID),
				gomock.Any(),
			).
			Do(func(_ string, msg proto.Message) {
				// Verify call message.
				call := msg.(*sched.Call)
				assert.Equal(t, sched.Call_KILL, call.GetType())
				assert.Equal(t, provider.GetFrameworkID(), call.GetFrameworkId())

				tid := call.GetKill().GetTaskId()
				assert.NotNil(t, tid)
				mockMutex.Lock()
				defer mockMutex.Unlock()
				killedTaskIds[tid.GetValue()] = true
			}).
			Return(nil).
			Times(2),
	)
	resp, _, err := h.KillTasks(rootCtx, nil, killReq)
	assert.NoError(t, err)
	assert.Nil(t, resp.GetError())
	assert.Equal(t, map[string]bool{"t1": true, "t2": true}, killedTaskIds)

	assert.Equal(
		t,
		int64(2),
		testScope.Snapshot().Counters()["kill_tasks"].Value())
}

// Test some failure cases of killing task
func TestKillTaskFailure(t *testing.T) {
	t1 := "t1"
	t2 := "t2"
	taskIDs := []*mesos.TaskID{
		{Value: &t1},
		{Value: &t2},
	}
	killReq := &hostsvc.KillTasksRequest{
		TaskIds: taskIDs,
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testScope := tally.NewTestScope("", map[string]string{})
	client := mpb_mocks.NewMockClient(ctrl)
	pool := offer.NewOfferPool(offerHoldTime, client)
	provider := &mockFrameworkInfoProvider{}
	h := &serviceHandler{
		client:                client,
		metrics:               NewMetrics(testScope),
		offerPool:             pool,
		frameworkInfoProvider: provider,
	}

	killedTaskIds := make(map[string]bool)
	failedTaskIds := make(map[string]bool)
	mockMutex := &sync.Mutex{}

	// A failed call.
	client.EXPECT().
		Call(
			gomock.Eq(streamID),
			gomock.Any(),
		).
		Do(func(_ string, msg proto.Message) {
			// Verify call message.
			call := msg.(*sched.Call)
			assert.Equal(t, sched.Call_KILL, call.GetType())
			assert.Equal(t, provider.GetFrameworkID(), call.GetFrameworkId())

			tid := call.GetKill().GetTaskId()
			assert.NotNil(t, tid)
			mockMutex.Lock()
			defer mockMutex.Unlock()
			failedTaskIds[tid.GetValue()] = true
		}).
		Return(errors.New("Some error"))

	// A successful call.
	client.EXPECT().
		Call(
			gomock.Eq(streamID),
			gomock.Any(),
		).
		Do(func(_ string, msg proto.Message) {
			// Verify call message.
			call := msg.(*sched.Call)
			assert.Equal(t, sched.Call_KILL, call.GetType())
			assert.Equal(t, provider.GetFrameworkID(), call.GetFrameworkId())

			tid := call.GetKill().GetTaskId()
			assert.NotNil(t, tid)
			mockMutex.Lock()
			defer mockMutex.Unlock()

			killedTaskIds[tid.GetValue()] = true
		}).
		Return(nil)

	resp, _, err := h.KillTasks(rootCtx, nil, killReq)
	assert.NoError(t, err)
	assert.NotNil(t, resp.GetError().GetKillFailure())

	assert.Equal(t, 1, len(killedTaskIds))
	assert.Equal(t, 1, len(failedTaskIds))

	assert.NotEqual(t, killedTaskIds, failedTaskIds)

	assert.Equal(
		t,
		int64(1),
		testScope.Snapshot().Counters()["kill_tasks"].Value())

	assert.Equal(
		t,
		int64(1),
		testScope.Snapshot().Counters()["kill_tasks_fail"].Value())
}
