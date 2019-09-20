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

package hostmover

import (
	"context"
	"errors"
	"testing"
	"time"

	pb_host "github.com/uber/peloton/.gen/peloton/api/v0/host"
	pb_hostsvc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"
	hostmocks "github.com/uber/peloton/.gen/peloton/api/v0/host/svc/mocks"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pb_respool "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	rc "github.com/uber/peloton/pkg/resmgr/common"
	"github.com/uber/peloton/pkg/resmgr/respool"
	rmtask "github.com/uber/peloton/pkg/resmgr/task"
	taskMocks "github.com/uber/peloton/pkg/resmgr/task/mocks"
	"github.com/uber/peloton/pkg/resmgr/tasktestutil"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

// HostPoolTestSuite is test suite for host pool.
type HostMoverTestSuite struct {
	suite.Suite
	ctrl        *gomock.Controller
	scorer      *batchScorer
	mockHostMgr *hostmocks.MockHostServiceYARPCClient
	mockTracker *taskMocks.MockTracker
	resTree     respool.Tree
}

// batchMetricsProps defines the task properties used by scorer
type batchMetricsProps struct {
	poolID         string
	taskType       resmgr.TaskType
	nonpreemptible bool
	controller     bool
	priority       uint32
	startTime      *time.Time
	state          string
}

func (s *HostMoverTestSuite) SetupSuite() {
	s.ctrl = gomock.NewController(s.T())
	s.mockHostMgr = hostmocks.NewMockHostServiceYARPCClient(s.ctrl)
	s.mockTracker = taskMocks.NewMockTracker(s.ctrl)
	mockResPoolOps := objectmocks.NewMockResPoolOps(s.ctrl)
	mockResPoolOps.EXPECT().GetAll(context.Background()).
		Return(s.getResPools(), nil).AnyTimes()
	mockJobStore := store_mocks.NewMockJobStore(s.ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(s.ctrl)
	s.resTree = respool.NewTree(tally.NoopScope, mockResPoolOps, mockJobStore,
		mockTaskStore, rc.PreemptionConfig{
			Enabled: false,
		})
	rmtask.InitTaskTracker(
		tally.NoopScope,
		tasktestutil.CreateTaskConfig())
}

func (s *HostMoverTestSuite) SetupTest() {
	err := s.resTree.Start()
	s.NoError(err)

	s.scorer = NewBatchScorer(true, s.mockHostMgr).(*batchScorer)
	s.scorer.rmTracker = s.mockTracker
}

func (s *HostMoverTestSuite) TearDownTest() {
	s.NoError(s.resTree.Stop())
}

// TestHostMoverTestSuite runs HostMoverTestSuite.
func TestHostMoverTestSuite(t *testing.T) {
	suite.Run(t, new(HostMoverTestSuite))
}

func (s *HostMoverTestSuite) TestBatchScorerStartEnabled() {
	defer s.scorer.Stop()
	s.scorer.enabled = true
	err := s.scorer.Start()
	s.NoError(err)
	s.NotNil(s.scorer.lifeCycle.StopCh())
}

func (s *HostMoverTestSuite) TestBatchScorerStartDisabled() {
	defer s.scorer.Stop()
	s.scorer.enabled = false
	err := s.scorer.Start()
	s.NoError(err)
	_, ok := <-s.scorer.lifeCycle.StopCh()
	s.False(ok)
}

func (s *HostMoverTestSuite) TestNewBatchScorer() {
	scorer := NewBatchScorer(true,
		s.mockHostMgr)

	s.NotNil(scorer)
}

func (s *HostMoverTestSuite) TestSortHostsByScoresWithErrors() {
	hostPools := []*pb_host.HostPoolInfo{
		{
			Name:  "pool1",
			Hosts: []string{"host1", "host2"},
		},
	}
	resp := &pb_hostsvc.ListHostPoolsResponse{
		Pools: hostPools,
	}

	s.mockHostMgr.
		EXPECT().
		ListHostPools(gomock.Any(), gomock.Any()).
		Return(resp, errors.New("bogus"))
	s.NotNil(s.scorer.sortOnce())

	s.mockHostMgr.
		EXPECT().
		ListHostPools(gomock.Any(), gomock.Any()).
		Return(resp, nil)
	s.NotNil(s.scorer.sortOnce())
}

// TestSortHostsByScores tests the hosts sorted by host metrics
func (s *HostMoverTestSuite) TestSortHostsByScores() {
	hostPools := []*pb_host.HostPoolInfo{
		{
			Name:  "shared",
			Hosts: []string{"host1", "host2"},
		},
	}
	resp := &pb_hostsvc.ListHostPoolsResponse{
		Pools: hostPools,
	}

	// Test tasks in different states are processed
	for _, state := range []string{"LAUNCHED", "RUNNING"} {
		task := s.createRMTask(
			batchMetricsProps{
				poolID:         "respool1",
				nonpreemptible: false,
				taskType:       resmgr.TaskType_BATCH,
				state:          state})
		hostTasks := map[string][]*rmtask.RMTask{
			"host1": {task},
		}

		s.mockHostMgr.
			EXPECT().
			ListHostPools(gomock.Any(), gomock.Any()).
			Return(resp, nil)
		s.mockTracker.
			EXPECT().
			TasksByHosts(gomock.Any(), gomock.Any()).
			Return(hostTasks)
		s.Nil(s.scorer.sortOnce())
		s.NotEmpty(s.scorer.orderedHosts)
	}

	// Test different metrics to order hosts
	startTime1 := time.Now()
	startTime2 := startTime1.Add(time.Second)

	expectedItems := []struct {
		hostTasks map[string][]*rmtask.RMTask
		hosts     []string
	}{
		{
			hostTasks: make(map[string][]*rmtask.RMTask),
			hosts:     []string{"host1", "host2"},
		},
		{
			hostTasks: map[string][]*rmtask.RMTask{
				"host1": {
					s.createRMTask(
						batchMetricsProps{
							poolID:         "respool1",
							nonpreemptible: true,
							taskType:       resmgr.TaskType_BATCH}),
				},
				"host2": {
					s.createRMTask(batchMetricsProps{
						poolID:         "respool1",
						nonpreemptible: false,
						taskType:       resmgr.TaskType_BATCH}),
				},
			},
			hosts: []string{"host2"},
		},
		{
			hostTasks: map[string][]*rmtask.RMTask{
				"host1": {
					s.createRMTask(batchMetricsProps{
						poolID:     "respool1",
						controller: true,
						taskType:   resmgr.TaskType_BATCH}),
				},
				"host2": {
					s.createRMTask(batchMetricsProps{
						poolID:     "respool1",
						controller: false,
						taskType:   resmgr.TaskType_BATCH}),
				},
			},
			hosts: []string{"host2", "host1"},
		},
		{
			hostTasks: map[string][]*rmtask.RMTask{
				"host1": {
					s.createRMTask(batchMetricsProps{
						poolID:   "respool1",
						taskType: resmgr.TaskType_BATCH}),
				},
				"host2": {
					s.createRMTask(batchMetricsProps{
						poolID:   "respool1",
						taskType: resmgr.TaskType_BATCH}),
					s.createRMTask(batchMetricsProps{
						poolID:   "respool1",
						taskType: resmgr.TaskType_BATCH}),
					s.createRMTask(batchMetricsProps{
						poolID:   "respool1",
						taskType: resmgr.TaskType_BATCH}),
				},
				"host3": {
					s.createRMTask(batchMetricsProps{
						poolID:   "respool1",
						taskType: resmgr.TaskType_BATCH}),
					s.createRMTask(batchMetricsProps{
						poolID:   "respool1",
						taskType: resmgr.TaskType_BATCH}),
				},
			},
			hosts: []string{"host1", "host3", "host2"},
		},
		{
			hostTasks: map[string][]*rmtask.RMTask{
				"host1": {
					s.createRMTask(batchMetricsProps{
						poolID:   "respool1",
						priority: 0,
						taskType: resmgr.TaskType_BATCH}),
					s.createRMTask(batchMetricsProps{
						poolID:   "respool1",
						priority: 0,
						taskType: resmgr.TaskType_BATCH}),
					s.createRMTask(batchMetricsProps{
						poolID:   "respool2",
						priority: 10,
						taskType: resmgr.TaskType_BATCH}),
					s.createRMTask(batchMetricsProps{
						poolID:   "respool2",
						priority: 50,
						taskType: resmgr.TaskType_BATCH}),
					s.createRMTask(batchMetricsProps{
						poolID:   "respool3",
						priority: 10,
						taskType: resmgr.TaskType_BATCH}),
					s.createRMTask(batchMetricsProps{
						poolID:   "respool3",
						priority: 20,
						taskType: resmgr.TaskType_BATCH}),
				},
				"host2": {
					s.createRMTask(batchMetricsProps{
						poolID:   "respool1",
						priority: 0,
						taskType: resmgr.TaskType_BATCH}),
					s.createRMTask(batchMetricsProps{
						poolID:   "respool1",
						priority: 0,
						taskType: resmgr.TaskType_BATCH}),
					s.createRMTask(batchMetricsProps{
						poolID:   "respool2",
						priority: 20,
						taskType: resmgr.TaskType_BATCH}),
					s.createRMTask(batchMetricsProps{
						poolID:   "respool2",
						priority: 100,
						taskType: resmgr.TaskType_BATCH}),
					s.createRMTask(batchMetricsProps{
						poolID:   "respool2",
						priority: 10,
						taskType: resmgr.TaskType_BATCH}),
					s.createRMTask(batchMetricsProps{
						poolID:   "respool2",
						priority: 10,
						taskType: resmgr.TaskType_BATCH}),
				},
			},
			hosts: []string{"host2", "host1"},
		},
		{
			hostTasks: map[string][]*rmtask.RMTask{
				"host1": {
					s.createRMTask(batchMetricsProps{
						poolID:    "respool1",
						taskType:  resmgr.TaskType_BATCH,
						startTime: &startTime1}),
				},
				"host2": {
					s.createRMTask(batchMetricsProps{
						poolID:    "respool1",
						taskType:  resmgr.TaskType_BATCH,
						startTime: &startTime2}),
				},
			},
			hosts: []string{"host2", "host1"},
		},
	}

	for _, item := range expectedItems {
		s.mockTracker.EXPECT().TasksByHosts(gomock.Any(), gomock.Any()).
			Return(item.hostTasks)
		s.mockHostMgr.
			EXPECT().
			ListHostPools(gomock.Any(), gomock.Any()).
			Return(resp, nil)
		s.scorer.sortOnce()
		s.EqualValues(item.hosts, s.scorer.orderedHosts)
	}
}

// TestGetHostsByScores tests returning the hosts by scores
func (s *HostMoverTestSuite) TestGetHostsByScores() {
	s.EqualValues([]string{}, s.scorer.GetHostsByScores(2))

	s.scorer.orderedHosts = append(s.scorer.orderedHosts,
		"host1", "host2", "host3")

	s.EqualValues(
		[]string{"host1", "host2"},
		s.scorer.GetHostsByScores(2))
	s.EqualValues(
		[]string{"host1", "host2", "host3"},
		s.scorer.GetHostsByScores(3))
	s.EqualValues(
		[]string{"host1", "host2", "host3"},
		s.scorer.GetHostsByScores(4))
}

func (s *HostMoverTestSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {

	rootID := peloton.ResourcePoolID{Value: "root"}
	policy := pb_respool.SchedulingPolicy_PriorityFIFO

	return map[string]*pb_respool.ResourcePoolConfig{
		"root": {
			Name:   "root",
			Parent: nil,
			Policy: policy,
		},
		"respool0": {
			Name:   "respool0",
			Parent: &rootID,
			Policy: policy,
		},
		"respool1": {
			Name:   "respool1",
			Parent: &rootID,
			Policy: policy,
		},
		"respool2": {
			Name:   "respool2",
			Parent: &rootID,
			Policy: policy,
		},
		"respool3": {
			Name:   "respool3",
			Parent: &rootID,
			Policy: policy,
		},
	}
}

func (s *HostMoverTestSuite) createRMTask(prop batchMetricsProps) *rmtask.RMTask {
	task := &resmgr.Task{
		Name:        "job1-1",
		Priority:    prop.priority,
		Preemptible: !prop.nonpreemptible,
		Controller:  prop.controller,
		Type:        prop.taskType,
	}

	respool, err := s.resTree.Get(&peloton.ResourcePoolID{Value: prop.poolID})
	s.NoError(err)

	rmTask, err := rmtask.CreateRMTask(
		tally.NoopScope,
		task,
		nil,
		respool,
		&rmtask.Config{},
	)
	s.NoError(err)

	if len(prop.state) == 0 {
		err = rmTask.TransitTo("RUNNING")
	} else {
		err = rmTask.TransitTo(prop.state)
	}
	s.NoError(err)

	if prop.startTime != nil {
		rmTask.UpdateStartTime(*prop.startTime)
	}

	return rmTask
}

func (s *HostMoverTestSuite) TestNoTasksHosts() {
	hostPools := []*pb_host.HostPoolInfo{
		{
			Name:  "shared",
			Hosts: []string{"host1", "host2", "host3"},
		},
	}
	resp := &pb_hostsvc.ListHostPoolsResponse{
		Pools: hostPools,
	}

	expectedItems := []struct {
		hostTasks map[string][]*rmtask.RMTask
		hosts     []string
	}{
		{
			hostTasks: map[string][]*rmtask.RMTask{
				"host1": {
					s.createRMTask(
						batchMetricsProps{
							poolID:         "respool1",
							nonpreemptible: false,
							taskType:       resmgr.TaskType_BATCH}),
				},
				"host2": {
					s.createRMTask(batchMetricsProps{
						poolID:         "respool1",
						nonpreemptible: false,
						taskType:       resmgr.TaskType_BATCH}),
					s.createRMTask(batchMetricsProps{
						poolID:         "respool1",
						nonpreemptible: false,
						taskType:       resmgr.TaskType_BATCH}),
				},
			},
			hosts: []string{"host3", "host1", "host2"},
		},
	}

	for _, item := range expectedItems {
		s.mockTracker.EXPECT().TasksByHosts(gomock.Any(), gomock.Any()).
			Return(item.hostTasks)
		s.mockHostMgr.
			EXPECT().
			ListHostPools(gomock.Any(), gomock.Any()).
			Return(resp, nil)
		s.scorer.sortOnce()
		s.EqualValues(item.hosts, s.scorer.orderedHosts)
	}
}
