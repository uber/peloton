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
	"context"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/uber/peloton/pkg/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

// fake ActiveTasksTracker
type fakeActiveTasksTracker struct{ tasks map[string][]*RMTask }

func (f *fakeActiveTasksTracker) GetActiveTasks(
	jobID string,
	respoolID string,
	states []string) map[string][]*RMTask {
	return f.tasks
}

// fake applier
type fakeApplier struct{ tasks map[string]string }

func (fa *fakeApplier) apply(tasks map[string]string) error {
	fa.tasks = tasks
	return nil
}
func (fa *fakeApplier) getTasks() map[string]string {
	return fa.tasks
}

func TestNewReconciler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pelotonTaskID := uuid.NewRandom().String()
	mesosTaskID := uuid.NewRandom().String()

	rtt := []struct {
		dbMesosTaskID            string
		trackerMesosTaskID       string
		pelotonTaskID            string
		stateInDB                task.TaskState
		stateInTracker           task.TaskState
		expectedTasksToReconcile map[string]string
		expectedError            error
	}{
		{
			// case 1: The task in the DB has finished but not in the tracker
			dbMesosTaskID:      mesosTaskID,
			trackerMesosTaskID: mesosTaskID,
			pelotonTaskID:      pelotonTaskID,
			stateInDB:          task.TaskState_FAILED,
			stateInTracker:     task.TaskState_INITIALIZED,
			expectedTasksToReconcile: map[string]string{
				pelotonTaskID: mesosTaskID,
			},
			expectedError: nil,
		},
		{
			// case 2: The task in the DB and tracker match
			dbMesosTaskID:            mesosTaskID,
			trackerMesosTaskID:       mesosTaskID,
			pelotonTaskID:            pelotonTaskID,
			stateInDB:                task.TaskState_INITIALIZED,
			stateInTracker:           task.TaskState_INITIALIZED,
			expectedTasksToReconcile: map[string]string{},
			expectedError:            nil,
		},
		{
			// case 3: The task in the DB and tracker have different mesos ID's
			dbMesosTaskID:            "differentFromTracker",
			trackerMesosTaskID:       mesosTaskID,
			pelotonTaskID:            pelotonTaskID,
			stateInDB:                task.TaskState_INITIALIZED,
			stateInTracker:           task.TaskState_INITIALIZED,
			expectedTasksToReconcile: map[string]string{},
			expectedError:            nil,
		},
		{
			// case 4: Error reading from db
			dbMesosTaskID:            "",
			trackerMesosTaskID:       mesosTaskID,
			pelotonTaskID:            pelotonTaskID,
			stateInDB:                task.TaskState_INITIALIZED,
			stateInTracker:           task.TaskState_INITIALIZED,
			expectedTasksToReconcile: map[string]string{},
			expectedError: errors.Errorf(
				"1 error occurred:\n\n* unable to get "+
					"task:%s from the database", pelotonTaskID),
		},
	}

	for _, tt := range rtt {
		// setup the reconciler
		mockStore := mocks.NewMockTaskStore(ctrl)
		gomock.InOrder(
			mockStore.EXPECT().GetTaskByID(context.Background(), tt.pelotonTaskID).Return(
				&task.TaskInfo{
					Runtime: &task.RuntimeInfo{
						State: tt.stateInDB,
						MesosTaskId: &mesos_v1.TaskID{
							Value: &tt.dbMesosTaskID,
						},
					},
				},
				tt.expectedError))

		ft := &fakeActiveTasksTracker{
			tasks: map[string][]*RMTask{tt.stateInTracker.String(): {
				{
					task: &resmgr.Task{
						Id: &peloton.TaskID{Value: tt.pelotonTaskID},
						TaskId: &mesos_v1.TaskID{Value: &tt.
							trackerMesosTaskID},
					},
				},
			}}}
		r := NewReconciler(
			ft,
			mockStore,
			tally.NoopScope,
			1*time.Minute,
		)
		fa := &fakeApplier{
			tasks: make(map[string]string),
		}
		r.applier = fa

		// Run the actual test
		err := r.run()
		if tt.expectedError == nil {
			assert.NoError(t, err)
			continue
		}
		assert.Equal(t, tt.expectedError.Error(), err.Error())

		actualTasksToReconcile := fa.getTasks()
		assert.Equal(t, tt.expectedTasksToReconcile, actualTasksToReconcile)
	}
}

func TestReconciler_Start(t *testing.T) {
	r := NewReconciler(
		&fakeActiveTasksTracker{},
		nil,
		tally.NoopScope,
		1*time.Minute,
	)

	defer func() {
		r.Stop()
		//Stopping reconciler again. Should be no-op
		assert.NoError(t, r.Stop())
	}()

	assert.NoError(t, r.Start())
	assert.NotNil(t, r.lifeCycle.StopCh())

	// Starting reconciler again. Should be no-op
	assert.NoError(t, r.Start())
}

func TestReconciler_LogApplier(t *testing.T) {
	l := new(logApplier)
	assert.NoError(t, l.apply(map[string]string{"": ""}))
}
