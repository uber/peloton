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

package watchevent

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	hostMetric "github.com/uber/peloton/pkg/hostmgr/metrics"

	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	halphapb "github.com/uber/peloton/.gen/peloton/api/v1alpha/host"
	pb_eventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	"go.uber.org/yarpc/yarpcerrors"
)

type WatchProcessorTestSuite struct {
	suite.Suite
	ctx              context.Context
	config           Config
	metrics          *hostMetric.Metrics
	processor        WatchProcessor
	topicsSupported  []Topic
	topicEventObject []interface{}
}

func (suite *WatchProcessorTestSuite) SetupTest() {
	suite.ctx = context.Background()
	testScope := tally.NewTestScope("", map[string]string{})
	suite.metrics = hostMetric.NewMetrics(testScope)
	suite.config = Config{
		BufferSize: 10,
		MaxClient:  2,
	}
	suite.processor = NewWatchProcessor(suite.config, suite.metrics)
	suite.topicsSupported = []Topic{EventStream, HostSummary}
	suite.topicEventObject = []interface{}{&pb_eventstream.Event{}, &halphapb.HostSummary{}}
}

func TestWatchProcessor(t *testing.T) {
	suite.Run(t, &WatchProcessorTestSuite{})
}

// TestInitWatchProcessor tests initialization of WatchProcessor
func (suite *WatchProcessorTestSuite) TestInitWatchProcessor() {
	suite.Nil(GetWatchProcessor())
	InitWatchProcessor(suite.config, suite.metrics)
	suite.NotNil(GetWatchProcessor())
}

// TestEventClient tests basic setup and teardown of task watch client
func (suite *WatchProcessorTestSuite) TestEventClient() {
	for _, topic := range suite.topicsSupported {
		watchID, c, err := suite.processor.NewEventClient(topic)
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

		err = suite.processor.StopEventClient(watchID)
		wg.Wait()

		suite.NoError(err)
		suite.Equal(StopSignalCancel, stopSignal)
	}
}

// TestEventClient_StopNonexistentClient tests an error will be thrown if
// tearing down a client with unknown watch id.
func (suite *WatchProcessorTestSuite) TestEventClient_StopNonexistentClient() {
	watchID, c, err := suite.processor.NewEventClient(suite.topicsSupported[0])
	suite.NoError(err)
	suite.NotEmpty(watchID)
	suite.NotNil(c)

	err = suite.processor.StopEventClient("00000000-0000-0000-0000-000000000000")
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))
}

// Test stop all clients on losing leadership
func (suite *WatchProcessorTestSuite) TestEventClient_StopAllClients() {
	watchIdList := []string{}
	for _, topic := range suite.topicsSupported {
		watchID1, c, err := suite.processor.NewEventClient(topic)
		suite.NoError(err)
		suite.NotEmpty(watchID1)
		suite.NotNil(c)
		watchIdList = append(watchIdList, watchID1)
	}
	suite.processor.StopEventClients()

	// all clients are alredy stopped
	for _, watchId := range watchIdList {
		suite.Error(suite.processor.StopEventClient(watchId))
	}
}

// TestEventClient_MaxClientReached tests an error will be thrown when
// creating a new client if max number of clients is reached.
func (suite *WatchProcessorTestSuite) TestEventClient_MaxClientReached() {
	for i := 0; i < 3; i++ {
		watchID, c, err := suite.processor.NewEventClient(suite.topicsSupported[rand.Intn(len(suite.topicsSupported))])
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

// TestEventClient_EventOverflow tests that a "overflow" stop Signal will be
// sent to the client and the client will be closed if the client buffer is
// overflown.
func (suite *WatchProcessorTestSuite) TestEventClient_EventOverflow() {
	for index, topic := range suite.topicsSupported {
		watchID, c, err := suite.processor.NewEventClient(topic)
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
			suite.processor.NotifyEventChange(suite.topicEventObject[index])
		}
		time.Sleep(1 * time.Second)
		suite.Equal(StopSignalUnknown, stopSignal)

		// trigger buffer overflow
		suite.processor.NotifyEventChange(suite.topicEventObject[index])
		wg.Wait()
		suite.Equal(StopSignalOverflow, stopSignal)
	}
}
