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

package resmgr

import (
	"encoding/json"
	"testing"

	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/resmgr/task"
	"github.com/uber/peloton/pkg/resmgr/task/mocks"

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

func TestServer_GainedLeadershipCallback(t *testing.T) {

	tt := []struct {
		s       *Server
		wantErr error
	}{
		{
			s: &Server{
				role:    "testResMgr",
				metrics: NewMetrics(tally.NoopScope),
				resTree: &FakeServerProcess{errFake},
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:            "testResMgr",
				metrics:         NewMetrics(tally.NoopScope),
				resTree:         &FakeServerProcess{nil},
				recoveryHandler: &FakeServerProcess{errFake},
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resTree:               &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				entitlementCalculator: &FakeServerProcess{errFake},
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resTree:               &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				entitlementCalculator: &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(errFake, t),
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resTree:               &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				entitlementCalculator: &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				reconciler:            &FakeServerProcess{errFake},
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resTree:               &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				entitlementCalculator: &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				reconciler:            &FakeServerProcess{nil},
				preemptor:             &FakeServerProcess{errFake},
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resTree:               &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				reconciler:            &FakeServerProcess{nil},
				preemptor:             &FakeServerProcess{nil},
				drainer:               &FakeServerProcess{errFake},
				entitlementCalculator: &FakeServerProcess{nil},
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resTree:               &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				entitlementCalculator: &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				reconciler:            &FakeServerProcess{nil},
				preemptor:             &FakeServerProcess{nil},
				drainer:               &FakeServerProcess{nil},
				batchScorer:           &FakeServerProcess{errFake},
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				resTree:               &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{nil},
				entitlementCalculator: &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				reconciler:            &FakeServerProcess{nil},
				preemptor:             &FakeServerProcess{nil},
				drainer:               &FakeServerProcess{nil},
				batchScorer:           &FakeServerProcess{nil},
			},
			wantErr: nil,
		},
	}

	for _, test := range tt {
		if test.wantErr == nil {
			assert.NoError(t, test.s.GainedLeadershipCallback())
			assert.True(t, test.s.HasGainedLeadership())
			continue
		}
		assert.EqualError(t, test.wantErr, test.s.GainedLeadershipCallback().Error())
		assert.False(t, test.s.HasGainedLeadership())
	}
}

func TestServer_LostLeadershipCallback(t *testing.T) {

	tt := []struct {
		s       *Server
		wantErr error
	}{
		{
			s: &Server{
				role:    "testResMgr",
				metrics: NewMetrics(tally.NoopScope),
				drainer: &FakeServerProcess{errFake},
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:      "testResMgr",
				metrics:   NewMetrics(tally.NoopScope),
				drainer:   &FakeServerProcess{nil},
				preemptor: &FakeServerProcess{errFake},
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:       "testResMgr",
				metrics:    NewMetrics(tally.NoopScope),
				drainer:    &FakeServerProcess{nil},
				preemptor:  &FakeServerProcess{nil},
				reconciler: &FakeServerProcess{errFake},
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:             "testResMgr",
				metrics:          NewMetrics(tally.NoopScope),
				drainer:          &FakeServerProcess{nil},
				preemptor:        &FakeServerProcess{nil},
				reconciler:       &FakeServerProcess{nil},
				getTaskScheduler: mockSchedulerWithErr(errFake, t),
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				drainer:               &FakeServerProcess{nil},
				preemptor:             &FakeServerProcess{nil},
				reconciler:            &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				entitlementCalculator: &FakeServerProcess{errFake},
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				drainer:               &FakeServerProcess{nil},
				preemptor:             &FakeServerProcess{nil},
				reconciler:            &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				entitlementCalculator: &FakeServerProcess{nil},
				recoveryHandler:       &FakeServerProcess{errFake},
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				drainer:               &FakeServerProcess{nil},
				preemptor:             &FakeServerProcess{nil},
				reconciler:            &FakeServerProcess{nil},
				entitlementCalculator: &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				recoveryHandler:       &FakeServerProcess{nil},
				resTree:               &FakeServerProcess{errFake},
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				drainer:               &FakeServerProcess{nil},
				preemptor:             &FakeServerProcess{nil},
				reconciler:            &FakeServerProcess{nil},
				entitlementCalculator: &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				recoveryHandler:       &FakeServerProcess{nil},
				resTree:               &FakeServerProcess{nil},
				batchScorer:           &FakeServerProcess{errFake},
			},
			wantErr: errFake,
		},
		{
			s: &Server{
				role:                  "testResMgr",
				metrics:               NewMetrics(tally.NoopScope),
				drainer:               &FakeServerProcess{nil},
				preemptor:             &FakeServerProcess{nil},
				reconciler:            &FakeServerProcess{nil},
				entitlementCalculator: &FakeServerProcess{nil},
				getTaskScheduler:      mockSchedulerWithErr(nil, t),
				recoveryHandler:       &FakeServerProcess{nil},
				resTree:               &FakeServerProcess{nil},
				batchScorer:           &FakeServerProcess{nil},
			},
			wantErr: nil,
		},
	}

	for _, test := range tt {
		if test.wantErr == nil {
			assert.NoError(t, test.s.LostLeadershipCallback())
			continue
		}
		assert.EqualError(t, test.wantErr, test.s.LostLeadershipCallback().Error())
		assert.False(t, test.s.HasGainedLeadership())
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
		&FakeServerProcess{nil},
		&FakeServerProcess{nil},
		&FakeServerProcess{nil},
	)

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
		&FakeServerProcess{nil},
		&FakeServerProcess{nil},
		&FakeServerProcess{nil},
	)

	assert.NoError(t, s.ShutDownCallback())
	assert.False(t, s.HasGainedLeadership())
}
