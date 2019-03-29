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

package task

import (
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/pkg/resmgr/respool/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

type fakeRecorder struct {
	duration time.Duration
}

func (fr *fakeRecorder) RecordDuration(duration time.Duration) { fr.duration = duration }

func withRecorder(r recorder) recorderGenerator {
	return func(_ tally.Scope) recorder {
		return r
	}
}

func TestTransObs_Observe(t *testing.T) {
	tobs := newTransitionObserver(
		map[string]string{},
		tally.NoopScope,
		defaultRules,
		true,
	)
	fr := &fakeRecorder{}
	tobs.recorderGenerator = withRecorder(fr)

	assert.Equal(t, fr.duration, time.Duration(0))

	tt := []struct {
		currentState   task.TaskState
		expectDuration bool
	}{
		{
			// This is starting state to record so the duration should be zero
			currentState:   task.TaskState_READY,
			expectDuration: false,
		},
		{
			// The rules don't include LAUNCHING to be recorded so it should
			// be zero
			currentState:   task.TaskState_LAUNCHING,
			expectDuration: false,
		},
		{
			// This is the end state to record so the duration > 0
			currentState:   task.TaskState_RUNNING,
			expectDuration: true,
		},
	}

	for _, test := range tt {
		tobs.Observe("", test.currentState)
		if test.expectDuration {
			assert.NotEqual(t, fr.duration, time.Duration(0),
				"duration should be greater than zero")
		} else {
			assert.Equal(t, fr.duration, time.Duration(0),
				"duration should be equal to zero")
		}
	}
}

func TestNewTransitionObserver(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResPool := mocks.NewMockResPool(ctrl)
	mockResPool.EXPECT().GetPath().Return("/").Times(1)

	dto := NewTransitionObserver(
		true,
		tally.NoopScope,
		mockResPool.GetPath(),
	)
	assert.NotNil(t, dto)
}
