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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/watch"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

type WatchProcessorTestSuite struct {
	suite.Suite

	ctx       context.Context
	testScope tally.TestScope

	config     Config
	jobID      *peloton.JobID
	instanceID uint32
	podName    *peloton.PodName

	processor WatchProcessor
}

func (suite *WatchProcessorTestSuite) SetupTest() {
	suite.ctx = context.Background()
	suite.testScope = tally.NewTestScope("", map[string]string{})

	suite.config = Config{
		BufferSize: 10,
		MaxClient:  2,
	}
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.instanceID = uint32(1)
	suite.podName = &peloton.PodName{Value: fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)}
	suite.processor = newWatchProcessor(suite.config, suite.testScope)
}

func TestWatchProcessor(t *testing.T) {
	suite.Run(t, &WatchProcessorTestSuite{})
}

// TestInitWatchProcessor tests initialization of WatchProcessor
func (suite *WatchProcessorTestSuite) TestInitWatchProcessor() {
	suite.Nil(GetWatchProcessor())
	InitWatchProcessor(suite.config, suite.testScope)
	suite.NotNil(GetWatchProcessor())
}

// TestTaskClient tests basic setup and teardown of task watch client
func (suite *WatchProcessorTestSuite) TestTaskClient() {
	watchID, c, err := suite.processor.NewTaskClient(nil)
	suite.NoError(err)
	suite.NotEmpty(watchID)
	suite.NotNil(c)

	var wg sync.WaitGroup
	wg.Add(1)
	var stopSignal StopSignal

	go func() {
		defer wg.Done()
		for {
			select {
			case <-c.Input:
			case stopSignal = <-c.Signal:
				return
			}
		}
	}()

	err = suite.processor.StopTaskClient(watchID)
	wg.Wait()

	suite.NoError(err)
	suite.Equal(StopSignalCancel, stopSignal)
}

// TestTaskClient_StopNonexistentClient tests an error will be thrown if
// tearing down a client with unknown watch id.
func (suite *WatchProcessorTestSuite) TestTaskClient_StopNonexistentClient() {
	watchID, c, err := suite.processor.NewTaskClient(nil)
	suite.NoError(err)
	suite.NotEmpty(watchID)
	suite.NotNil(c)

	err = suite.processor.StopTaskClient("00000000-0000-0000-0000-000000000000")
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))
}

// TestTaskClient_StopAllClients tests stop all clients on losing leadership
func (suite *WatchProcessorTestSuite) TestTaskClient_StopAllClients() {
	watchID1, c, err := suite.processor.NewTaskClient(nil)
	suite.NoError(err)
	suite.NotEmpty(watchID1)
	suite.NotNil(c)

	watchID2, c, err := suite.processor.NewTaskClient(nil)
	suite.NoError(err)
	suite.NotEmpty(watchID2)
	suite.NotNil(c)

	suite.processor.StopTaskClients()

	// all clients are alredy stopped
	suite.Error(suite.processor.StopTaskClient(watchID1))
	suite.Error(suite.processor.StopTaskClient(watchID2))
}

// TestTaskClient_MaxClientReached tests an error will be thrown when
// creating a new client if max number of clients is reached.
func (suite *WatchProcessorTestSuite) TestTaskClient_MaxClientReached() {
	for i := 0; i < 3; i++ {
		watchID, c, err := suite.processor.NewTaskClient(nil)
		if i < 2 {
			suite.NoError(err)
			suite.NotEmpty(watchID)
			suite.NotNil(c)
		} else {
			suite.Error(err)
			suite.True(yarpcerrors.IsResourceExhausted(err))
		}
	}
}

// TestTaskClient_EventOverflow tests that a "overflow" stop Signal will be
// sent to the client and the client will be closed if the client buffer is
// overflown.
func (suite *WatchProcessorTestSuite) TestTaskClient_EventOverflow() {
	watchID, c, err := suite.processor.NewTaskClient(nil)
	suite.NoError(err)
	suite.NotEmpty(watchID)
	suite.NotNil(c)

	var wg sync.WaitGroup
	wg.Add(1)
	var stopSignal StopSignal

	go func() {
		defer wg.Done()
		for {
			select {
			case stopSignal = <-c.Signal:
				return
			}
		}
	}()

	// send number of events equal to buffer size
	for i := 0; i < 10; i++ {
		suite.processor.NotifyPodChange(&pod.PodSummary{}, nil)
	}
	time.Sleep(1 * time.Second)
	suite.Equal(StopSignalUnknown, stopSignal)

	// trigger buffer overflow
	suite.processor.NotifyPodChange(&pod.PodSummary{}, nil)
	wg.Wait()
	suite.Equal(StopSignalOverflow, stopSignal)
}

func (suite *WatchProcessorTestSuite) TestTaskClientPodFilter() {
	filter := &watch.PodFilter{
		JobId:    suite.jobID,
		PodNames: []*peloton.PodName{suite.podName},
	}

	var mutex = &sync.Mutex{}
	var wg sync.WaitGroup
	wg.Add(1)
	received := 0

	watchID, c, err := suite.processor.NewTaskClient(filter)
	suite.NoError(err)
	suite.NotEmpty(watchID)
	suite.NotNil(c)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-c.Input:
				mutex.Lock()
				received++
				mutex.Unlock()
			case <-c.Signal:
				return
			}
		}
	}()

	suite.processor.NotifyPodChange(&pod.PodSummary{
		PodName: suite.podName,
	}, nil)

	suite.processor.NotifyPodChange(&pod.PodSummary{
		PodName: &peloton.PodName{Value: "abc-1"},
	}, nil)

	suite.processor.NotifyPodChange(&pod.PodSummary{
		PodName: &peloton.PodName{Value: fmt.Sprintf("%s-%d", suite.jobID, 5)},
	}, nil)

	time.Sleep(1 * time.Second)
	err = suite.processor.StopTaskClient(watchID)
	suite.NoError(err)
	wg.Wait()

	mutex.Lock()
	suite.Equal(received, 1)
	mutex.Unlock()
}

func (suite *WatchProcessorTestSuite) TestTaskClientPodLabelFilter() {
	label1 := &peloton.Label{
		Key:   "key1",
		Value: "value1",
	}
	label2 := &peloton.Label{
		Key:   "key2",
		Value: "value2",
	}

	filter := &watch.PodFilter{
		Labels: []*peloton.Label{label1},
	}

	var mutex = &sync.Mutex{}
	var wg sync.WaitGroup
	wg.Add(1)
	received := 0

	watchID, c, err := suite.processor.NewTaskClient(filter)
	suite.NoError(err)
	suite.NotEmpty(watchID)
	suite.NotNil(c)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-c.Input:
				mutex.Lock()
				received++
				mutex.Unlock()
			case <-c.Signal:
				return
			}
		}
	}()

	suite.processor.NotifyPodChange(
		&pod.PodSummary{},
		[]*peloton.Label{label1},
	)

	suite.processor.NotifyPodChange(
		&pod.PodSummary{},
		[]*peloton.Label{label2},
	)

	suite.processor.NotifyPodChange(
		&pod.PodSummary{},
		[]*peloton.Label{label1, label2},
	)

	time.Sleep(1 * time.Second)
	err = suite.processor.StopTaskClient(watchID)
	suite.NoError(err)
	wg.Wait()

	mutex.Lock()
	suite.Equal(2, received)
	mutex.Unlock()
}

// TestJobClient tests basic setup and teardown of job watch client
func (suite *WatchProcessorTestSuite) TestJobClient() {
	watchID, c, err := suite.processor.NewJobClient(nil)
	suite.NoError(err)
	suite.NotEmpty(watchID)
	suite.NotNil(c)

	var wg sync.WaitGroup
	wg.Add(1)
	var stopSignal StopSignal

	go func() {
		defer wg.Done()
		for {
			select {
			case <-c.Input:
			case stopSignal = <-c.Signal:
				return
			}
		}
	}()

	err = suite.processor.StopJobClient(watchID)
	wg.Wait()

	suite.NoError(err)
	suite.Equal(StopSignalCancel, stopSignal)
}

// TestJobClient_StopNonexistentClient tests an error will be thrown if
// tearing down a client with unknown watch id.
func (suite *WatchProcessorTestSuite) TestJobClient_StopNonexistentClient() {
	watchID, c, err := suite.processor.NewJobClient(nil)
	suite.NoError(err)
	suite.NotEmpty(watchID)
	suite.NotNil(c)

	err = suite.processor.StopJobClient("00000000-0000-0000-0000-000000000000")
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))
}

// TestJobClient_StopAllClients tests stop all clients on losing leadership
func (suite *WatchProcessorTestSuite) TestJobClient_StopAllClients() {
	watchID1, c, err := suite.processor.NewJobClient(nil)
	suite.NoError(err)
	suite.NotEmpty(watchID1)
	suite.NotNil(c)

	watchID2, c, err := suite.processor.NewJobClient(nil)
	suite.NoError(err)
	suite.NotEmpty(watchID2)
	suite.NotNil(c)

	suite.processor.StopJobClients()

	// all clients are alredy stopped
	suite.Error(suite.processor.StopJobClient(watchID1))
	suite.Error(suite.processor.StopJobClient(watchID2))
}

// TestJobClient_MaxClientReached tests an error will be thrown when
// creating a new client if max number of clients is reached.
func (suite *WatchProcessorTestSuite) TestJobClient_MaxClientReached() {
	for i := 0; i < 3; i++ {
		watchID, c, err := suite.processor.NewJobClient(nil)
		if i < 2 {
			suite.NoError(err)
			suite.NotEmpty(watchID)
			suite.NotNil(c)
		} else {
			suite.Error(err)
			suite.True(yarpcerrors.IsResourceExhausted(err))
		}
	}
}

// TestJobClient_EventOverflow tests that a "overflow" stop Signal will be
// sent to the client and the client will be closed if the client buffer is
// overflown.
func (suite *WatchProcessorTestSuite) TestJobClient_EventOverflow() {
	watchID, c, err := suite.processor.NewJobClient(nil)
	suite.NoError(err)
	suite.NotEmpty(watchID)
	suite.NotNil(c)

	var wg sync.WaitGroup
	wg.Add(1)
	var stopSignal StopSignal

	go func() {
		defer wg.Done()
		for {
			select {
			case stopSignal = <-c.Signal:
				return
			}
		}
	}()

	// send number of events equal to buffer size
	for i := 0; i < 10; i++ {
		suite.processor.NotifyJobChange(&stateless.JobSummary{})
	}
	time.Sleep(1 * time.Second)
	suite.Equal(StopSignalUnknown, stopSignal)

	// trigger buffer overflow
	suite.processor.NotifyJobChange(&stateless.JobSummary{})
	wg.Wait()
	suite.Equal(StopSignalOverflow, stopSignal)
}

func (suite *WatchProcessorTestSuite) TestJobClientJobFilter() {
	filter := &watch.StatelessJobFilter{
		JobIds: []*peloton.JobID{suite.jobID},
	}

	var mutex = &sync.Mutex{}
	var wg sync.WaitGroup
	wg.Add(1)
	received := 0

	watchID, c, err := suite.processor.NewJobClient(filter)
	suite.NoError(err)
	suite.NotEmpty(watchID)
	suite.NotNil(c)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-c.Input:
				mutex.Lock()
				received++
				mutex.Unlock()
			case <-c.Signal:
				return
			}
		}
	}()

	suite.processor.NotifyJobChange(&stateless.JobSummary{JobId: suite.jobID})

	suite.processor.NotifyJobChange(&stateless.JobSummary{
		JobId: &peloton.JobID{Value: uuid.NewRandom().String()},
	})

	suite.processor.NotifyJobChange(&stateless.JobSummary{
		JobId: &peloton.JobID{Value: uuid.NewRandom().String()},
	})

	time.Sleep(1 * time.Second)
	err = suite.processor.StopJobClient(watchID)
	suite.NoError(err)
	wg.Wait()

	mutex.Lock()
	suite.Equal(received, 1)
	mutex.Unlock()
}

func (suite *WatchProcessorTestSuite) TestJobClientLabelFilter() {
	label1 := &peloton.Label{
		Key:   "key1",
		Value: "value1",
	}
	label2 := &peloton.Label{
		Key:   "key2",
		Value: "value2",
	}

	filter := &watch.StatelessJobFilter{
		Labels: []*peloton.Label{label1},
	}

	var mutex = &sync.Mutex{}
	var wg sync.WaitGroup
	wg.Add(1)
	received := 0

	watchID, c, err := suite.processor.NewJobClient(filter)
	suite.NoError(err)
	suite.NotEmpty(watchID)
	suite.NotNil(c)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-c.Input:
				mutex.Lock()
				received++
				mutex.Unlock()
			case <-c.Signal:
				return
			}
		}
	}()

	suite.processor.NotifyJobChange(
		&stateless.JobSummary{
			Labels: []*peloton.Label{label1},
		},
	)

	suite.processor.NotifyJobChange(
		&stateless.JobSummary{
			Labels: []*peloton.Label{label2},
		},
	)

	suite.processor.NotifyJobChange(
		&stateless.JobSummary{
			Labels: []*peloton.Label{label1, label2},
		},
	)

	time.Sleep(1 * time.Second)
	err = suite.processor.StopJobClient(watchID)
	suite.NoError(err)
	wg.Wait()

	mutex.Lock()
	suite.Equal(2, received)
	mutex.Unlock()
}
