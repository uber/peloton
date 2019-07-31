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

package event

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	v1pbpeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	pbeventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	v1pbevent "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/event"

	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	goalstatemocks "github.com/uber/peloton/pkg/jobmgr/goalstate/mocks"
	jobmgrtask "github.com/uber/peloton/pkg/jobmgr/task"
	eventsmocks "github.com/uber/peloton/pkg/jobmgr/task/event/mocks"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
)

var uuidStr = uuid.New()
var jobID = &peloton.JobID{Value: uuidStr}

type BucketEventProcessorTestSuite struct {
	suite.Suite

	ctrl            *gomock.Controller
	jobFactory      *cachedmocks.MockJobFactory
	cachedJob       *cachedmocks.MockJob
	cachedTask      *cachedmocks.MockTask
	goalStateDriver *goalstatemocks.MockDriver
	taskStore       *storemocks.MockTaskStore
	handler         *statusUpdate
	statusProcessor *eventsmocks.MockStatusProcessor
}

func TestBucketEventProcessor(t *testing.T) {
	suite.Run(t, new(BucketEventProcessorTestSuite))
}

func (suite *BucketEventProcessorTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.statusProcessor = eventsmocks.NewMockStatusProcessor(suite.ctrl)
	suite.handler = &statusUpdate{
		taskStore:       suite.taskStore,
		jobFactory:      suite.jobFactory,
		goalStateDriver: suite.goalStateDriver,
		metrics:         NewMetrics(tally.NoopScope),
	}
}

func (suite *BucketEventProcessorTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *BucketEventProcessorTestSuite) TestBucketEventProcessor_MesosEvents() {
	var offset uint64

	applier := newBucketEventProcessor(suite.handler, 15, 100)
	applier.start()
	n := uint32(243)

	for i := uint32(0); i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), i, uuidStr)
		pelotonTaskID := fmt.Sprintf("%s-%d", jobID.GetValue(), i)
		taskInfo := &task.TaskInfo{
			Runtime: &task.RuntimeInfo{
				MesosTaskId:   &mesos.TaskID{Value: &mesosTaskID},
				ResourceUsage: jobmgrtask.CreateEmptyResourceUsageMap(),
			},
			InstanceId: i,
			JobId:      jobID,
		}
		suite.taskStore.EXPECT().GetTaskByID(
			context.Background(), pelotonTaskID).Return(taskInfo, nil).Times(3)
		suite.jobFactory.EXPECT().AddJob(jobID).Return(suite.cachedJob).Times(3)
		suite.cachedJob.EXPECT().SetTaskUpdateTime(gomock.Any()).Return().Times(3)
		suite.cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH).Times(5)
		suite.cachedJob.EXPECT().
			CompareAndSetTask(
				context.Background(),
				i,
				gomock.Any(),
				false,
			).Return(nil, nil).
			Times(3)
		suite.goalStateDriver.EXPECT().EnqueueTask(jobID, i, gomock.Any()).Return().Times(3)
		suite.cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return().Times(3)
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1 * time.Second).Times(3)
		suite.goalStateDriver.EXPECT().EnqueueJob(jobID, gomock.Any()).Return().Times(3)
	}
	for i := uint32(0); i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), i, uuidStr)
		state := mesos.TaskState_TASK_STARTING
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}

		offset++
		applier.addV0Event(&pbeventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pbeventstream.Event_MESOS_TASK_STATUS,
		})
	}

	for i := uint32(0); i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), i, uuidStr)
		state := mesos.TaskState_TASK_RUNNING
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}

		offset++
		applier.addV0Event(&pbeventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pbeventstream.Event_MESOS_TASK_STATUS,
		})
	}

	for i := uint32(0); i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), i, uuidStr)
		state := mesos.TaskState_TASK_FINISHED
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}

		offset++
		applier.addV0Event(&pbeventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pbeventstream.Event_MESOS_TASK_STATUS,
		})
	}

	applier.drainAndShutdown()

	for i, bucket := range applier.eventBuckets {
		suite.True(bucket.getProcessedCount() > 0, fmt.Sprintf("bucket %d did not process any event", i))
	}
}

// TestBucketEventProcessor_GetEventProgress tests event progress is correct
func (suite *BucketEventProcessorTestSuite) TestBucketEventProcessor_GetEventProgress() {
	var offset uint64
	n := uint32(243)
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), 0, uuidStr)

	suite.statusProcessor.EXPECT().ProcessListeners(gomock.Any()).Return().AnyTimes()
	suite.statusProcessor.EXPECT().
		ProcessStatusUpdate(gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()

	applier := newBucketEventProcessor(suite.statusProcessor, 15, 100)
	applier.start()

	for i := uint32(0); i < n; i++ {

		offset++
		applier.addV0Event(&pbeventstream.Event{
			Offset: offset,
			Type:   pbeventstream.Event_MESOS_TASK_STATUS,
			MesosTaskStatus: &mesos.TaskStatus{
				TaskId: &mesos.TaskID{
					Value: &mesosTaskID,
				},
			},
		})
	}

	applier.drainAndShutdown()

	suite.Equal(applier.GetEventProgress(), uint64(n))
}

// TestBucketEventProcessor_TransientError tests retry on transient error
func (suite *BucketEventProcessorTestSuite) TestBucketEventProcessor_TransientError() {
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), 0, uuidStr)
	suite.statusProcessor.EXPECT().ProcessListeners(gomock.Any()).Return().AnyTimes()

	// return transient error
	suite.statusProcessor.EXPECT().
		ProcessStatusUpdate(gomock.Any(), gomock.Any()).
		Return(yarpcerrors.AbortedErrorf("transient error"))
	// expect a retry
	suite.statusProcessor.EXPECT().
		ProcessStatusUpdate(gomock.Any(), gomock.Any()).
		Return(nil)

	applier := newBucketEventProcessor(suite.statusProcessor, 15, 100)
	applier.start()

	applier.addV0Event(&pbeventstream.Event{
		Type: pbeventstream.Event_MESOS_TASK_STATUS,
		MesosTaskStatus: &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
		},
	})

	applier.drainAndShutdown()
}

// TestBucketEventProcessor_TransientError tests non-transient error
// which should have no retry
func (suite *BucketEventProcessorTestSuite) TestBucketEventProcessor_NonTransientError() {
	suite.statusProcessor.EXPECT().ProcessListeners(gomock.Any()).Return().AnyTimes()

	// return non-transient error
	suite.statusProcessor.EXPECT().
		ProcessStatusUpdate(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("non-transient error"))

	applier := newBucketEventProcessor(suite.statusProcessor, 15, 100)
	applier.start()

	applier.addV0Event(&pbeventstream.Event{
		Type: pbeventstream.Event_PELOTON_TASK_EVENT,
		PelotonTaskEvent: &task.TaskEvent{
			TaskId: &peloton.TaskID{
				Value: fmt.Sprintf("%s-%d", uuidStr, 0),
			},
		},
	})

	applier.drainAndShutdown()
}

// TestBucketEventProcessor_AddEvent tests AddEvent can parse tasks id of mesos
// and peloton tasks
func (suite *BucketEventProcessorTestSuite) TestBucketEventProcessor_AddEventFails() {
	corruptedID := "corrupted-id"
	applier := newBucketEventProcessor(suite.statusProcessor, 15, 100)
	applier.start()
	err := applier.addV0Event(&pbeventstream.Event{
		Type: pbeventstream.Event_MESOS_TASK_STATUS,
		MesosTaskStatus: &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &corruptedID,
			},
		},
	})
	suite.Error(err)

	err = applier.addV0Event(&pbeventstream.Event{
		Type: pbeventstream.Event_PELOTON_TASK_EVENT,
		PelotonTaskEvent: &task.TaskEvent{
			TaskId: &peloton.TaskID{
				Value: corruptedID,
			},
		},
	})
	suite.Error(err)
}

// TestBucketEventProcessor_StartStop tests that events can be processed
// after a start-stop-start sequence
func (suite *BucketEventProcessorTestSuite) TestBucketEventProcessor_StartStop() {
	var offset uint64
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), 0, uuidStr)

	suite.statusProcessor.EXPECT().ProcessListeners(
		gomock.Any()).Return().AnyTimes()
	suite.statusProcessor.EXPECT().
		ProcessStatusUpdate(gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()

	applier := newBucketEventProcessor(suite.statusProcessor, 15, 100)
	addEvents := func() {
		for i := 0; i < 8; i++ {
			offset++
			applier.addV0Event(&pbeventstream.Event{
				Offset: offset,
				Type:   pbeventstream.Event_MESOS_TASK_STATUS,
				MesosTaskStatus: &mesos.TaskStatus{
					TaskId: &mesos.TaskID{
						Value: &mesosTaskID,
					},
				},
			})
		}
	}
	applier.start()
	addEvents()
	applier.drainAndShutdown()
	suite.Equal(applier.GetEventProgress(), offset)

	addEvents()
	applier.start()
	addEvents()
	applier.drainAndShutdown()
	suite.Equal(applier.GetEventProgress(), offset)
}

func newV1EventPb(podID string, offset uint64, state string) *v1pbevent.Event {
	return &v1pbevent.Event{
		Offset: offset,
		PodEvent: &pbpod.PodEvent{
			PodId:       &v1pbpeloton.PodID{Value: podID},
			ActualState: state,
			Timestamp:   time.Now().Format(time.RFC3339),
			Hostname:    "localhost",
			Healthy:     pbpod.HealthState_HEALTH_STATE_HEALTHY.String(),
		},
	}
}

func (suite *BucketEventProcessorTestSuite) TestV1Event() {
	n := 9
	suite.statusProcessor.EXPECT().ProcessListeners(
		gomock.Any()).Return().AnyTimes()
	suite.statusProcessor.EXPECT().
		ProcessStatusUpdate(gomock.Any(), gomock.Any()).
		Return(nil).MinTimes(6)

	applier := newBucketEventProcessor(suite.statusProcessor, 3, 1)
	applier.start()

	podID := fmt.Sprintf("%s-1-1", uuid.New())
	for i := 0; i < n; i++ {
		ev := newV1EventPb(podID, 1+uint64(i), pbpod.PodState_POD_STATE_RUNNING.String())
		err := applier.addV1Event(ev)
		suite.NoError(err)
	}

	applier.drainAndShutdown()
}
