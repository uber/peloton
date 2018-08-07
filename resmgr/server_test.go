package resmgr

import (
	"encoding/json"
	"testing"

	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/resmgr/preemption"
	pmocks "code.uber.internal/infra/peloton/resmgr/preemption/mocks"
	"code.uber.internal/infra/peloton/resmgr/task"
	"code.uber.internal/infra/peloton/resmgr/task/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

type FakeServerProcess struct{ err error }

func (ms *FakeServerProcess) Start() error { return ms.err }
func (ms *FakeServerProcess) Stop() error  { return ms.err }

var errFake = errors.New("fake error")

func mockSchedulerWithErr(err error, t *testing.T) func() task.Scheduler {
	ctrl := gomock.NewController(t)
	mScheduler := mocks.NewMockScheduler(ctrl)
	mScheduler.EXPECT().Start().Return(err).AnyTimes()
	mScheduler.EXPECT().Stop().Return(err).AnyTimes()
	return func() task.Scheduler {
		return mScheduler
	}
}

func mockPreemptorWithErr(err error, t *testing.T) func() preemption.Preemptor {
	ctrl := gomock.NewController(t)
	mPreemptor := pmocks.NewMockPreemptor(ctrl)
	mPreemptor.EXPECT().Start().Return(err).AnyTimes()
	mPreemptor.EXPECT().Stop().Return(err).AnyTimes()
	return func() preemption.Preemptor {
		return mPreemptor
	}
}

func TestServer_GainedLeadershipCallback(t *testing.T) {

	tt := []struct {
		s   *Server
		err error
	}{
		{
			s: &Server{
				role:           "testResMgr",
				metrics:        NewMetrics(tally.NoopScope),
				resPoolHandler: &FakeServerProcess{errFake},
			},
			err: errFake,
		},
		{
			s: &Server{
				role:            "testResMgr",
				metrics:         NewMetrics(tally.NoopScope),
				resPoolHandler:  &FakeServerProcess{nil},
				recoveryHandler: &FakeServerProcess{errFake},
			},
			err: errFake,
		},
		{
			s: &Server{
				role:             "testResMgr",
				metrics:          NewMetrics(tally.NoopScope),
				resPoolHandler:   &FakeServerProcess{nil},
				recoveryHandler:  &FakeServerProcess{nil},
				getTaskScheduler: mockSchedulerWithErr(errFake, t),
			},
			err: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resPoolHandler:        &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				entitlementCalculator: &FakeServerProcess{errFake},
			},
			err: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resPoolHandler:        &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				entitlementCalculator: &FakeServerProcess{nil},
				reconciler:            &FakeServerProcess{errFake},
			},
			err: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resPoolHandler:        &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				entitlementCalculator: &FakeServerProcess{nil},
				reconciler:            &FakeServerProcess{nil},
				getPreemptor:          mockPreemptorWithErr(errFake, t),
			},
			err: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resPoolHandler:        &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				entitlementCalculator: &FakeServerProcess{nil},
				reconciler:            &FakeServerProcess{nil},
				getPreemptor:          mockPreemptorWithErr(nil, t),
				drainer:               &FakeServerProcess{errFake},
			},
			err: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resPoolHandler:        &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				entitlementCalculator: &FakeServerProcess{nil},
				reconciler:            &FakeServerProcess{nil},
				getPreemptor:          mockPreemptorWithErr(nil, t),
				drainer:               &FakeServerProcess{nil},
			},
			err: nil,
		},
	}

	for _, test := range tt {
		if test.err == nil {
			assert.NoError(t, test.s.GainedLeadershipCallback())
			continue
		}
		assert.EqualError(t, test.err, test.s.GainedLeadershipCallback().Error())
	}
}

func TestServer_LostLeadershipCallback(t *testing.T) {

	tt := []struct {
		s   *Server
		err error
	}{
		{
			s: &Server{
				role:           "testResMgr",
				metrics:        NewMetrics(tally.NoopScope),
				resPoolHandler: &FakeServerProcess{errFake},
			},
			err: errFake,
		},
		{
			s: &Server{
				role:            "testResMgr",
				metrics:         NewMetrics(tally.NoopScope),
				resPoolHandler:  &FakeServerProcess{nil},
				recoveryHandler: &FakeServerProcess{errFake},
			},
			err: errFake,
		},
		{
			s: &Server{
				role:             "testResMgr",
				metrics:          NewMetrics(tally.NoopScope),
				resPoolHandler:   &FakeServerProcess{nil},
				recoveryHandler:  &FakeServerProcess{nil},
				getTaskScheduler: mockSchedulerWithErr(errFake, t),
			},
			err: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resPoolHandler:        &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				entitlementCalculator: &FakeServerProcess{errFake},
			},
			err: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resPoolHandler:        &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				entitlementCalculator: &FakeServerProcess{nil},
				reconciler:            &FakeServerProcess{errFake},
			},
			err: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resPoolHandler:        &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				entitlementCalculator: &FakeServerProcess{nil},
				reconciler:            &FakeServerProcess{nil},
				getPreemptor:          mockPreemptorWithErr(errFake, t),
			},
			err: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resPoolHandler:        &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				entitlementCalculator: &FakeServerProcess{nil},
				reconciler:            &FakeServerProcess{nil},
				getPreemptor:          mockPreemptorWithErr(nil, t),
				drainer:               &FakeServerProcess{errFake},
			},
			err: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resPoolHandler:        &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				entitlementCalculator: &FakeServerProcess{nil},
				reconciler:            &FakeServerProcess{nil},
				getPreemptor:          mockPreemptorWithErr(nil, t),
				drainer:               &FakeServerProcess{nil},
			},
			err: nil,
		},
	}

	for _, test := range tt {
		if test.err == nil {
			assert.NoError(t, test.s.LostLeadershipCallback())
			continue
		}
		assert.EqualError(t, test.err, test.s.LostLeadershipCallback().Error())
	}
}

func TestServer_GetID(t *testing.T) {
	s := NewServer(
		tally.NoopScope,
		80,
		5290,
		&FakeServerProcess{nil},
		&FakeServerProcess{nil},
		&FakeServerProcess{nil},
		&FakeServerProcess{nil},
		&FakeServerProcess{nil})

	assert.NotNil(t, s)
	id := s.GetID()

	var lID leader.ID
	err := json.Unmarshal([]byte(id), &lID)
	assert.NoError(t, err)

	assert.Equal(t, 80, lID.HTTPPort)
	assert.Equal(t, 5290, lID.GRPCPort)
}

func TestServer_ShutDownCallback(t *testing.T) {
	s := NewServer(
		tally.NoopScope,
		80,
		5290,
		&FakeServerProcess{nil},
		&FakeServerProcess{nil},
		&FakeServerProcess{nil},
		&FakeServerProcess{nil},
		&FakeServerProcess{nil})

	assert.NoError(t, s.ShutDownCallback())
}
