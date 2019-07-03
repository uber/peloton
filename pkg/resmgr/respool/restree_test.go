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

package respool

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	pb_respool "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/uber/peloton/pkg/common"
	taskutil "github.com/uber/peloton/pkg/common/util/task"
	rc "github.com/uber/peloton/pkg/resmgr/common"
	"github.com/uber/peloton/pkg/resmgr/scalar"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

type resTreeTestSuite struct {
	suite.Suite
	resourceTree Tree
	dispatcher   yarpc.Dispatcher
	resPools     map[string]*respool.ResourcePoolConfig
	allNodes     map[string]*ResPool
	root         *ResPool
	newRoot      *ResPool
	mockCtrl     *gomock.Controller
}

func (s *resTreeTestSuite) SetupSuite() {
	s.mockCtrl = gomock.NewController(s.T())
	s.resourceTree = s.getTree(s.withStore(s.getResPools(), nil))
}

func (s *resTreeTestSuite) TearDownSuite() {
	s.mockCtrl.Finish()
}

// This function test the initialization of the tree.
func (s *resTreeTestSuite) TestNewTree() {
	// Creating local mocks and stores for new tree
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()

	mockResPoolOps := objectmocks.NewMockResPoolOps(mockCtrl)
	mockResPoolOps.EXPECT().GetAll(context.Background()).
		Return(s.getResPools(), nil).AnyTimes()
	mockJobStore := store_mocks.NewMockJobStore(mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(mockCtrl)

	// Initialize the local tree
	resTree := NewTree(
		tally.NoopScope,
		mockResPoolOps,
		mockJobStore,
		mockTaskStore,
		rc.PreemptionConfig{Enabled: false},
	)
	s.NotNil(resTree)
}

func (s *resTreeTestSuite) SetupTest() {
	s.NoError(s.resourceTree.Start())
}

func (s *resTreeTestSuite) TearDownTest() {
	s.NoError(s.resourceTree.Stop())
}

func (s *resTreeTestSuite) TestGetChildren() {
	rt, ok := s.resourceTree.(*tree)
	s.Equal(true, ok)
	list := rt.root.Children()
	s.Equal(list.Len(), 3)
	n := rt.resPools["respool1"]
	list = n.Children()
	s.Equal(list.Len(), 2)
	n = rt.resPools["respool2"]
	list = n.Children()
	s.Equal(list.Len(), 2)
}

func (s *resTreeTestSuite) TestResourceConfig() {
	rt, ok := s.resourceTree.(*tree)
	s.Equal(true, ok)
	n := rt.resPools["respool1"]
	s.Equal(n.ID(), "respool1")
	for _, res := range n.Resources() {
		if res.Kind == "cpu" {
			s.Equal(res.Reservation, 100.00,
				"Reservation is not Equal")
			s.Equal(res.Limit, 1000.00, "Limit is not equal")
		}
		if res.Kind == "memory" {
			s.Equal(res.Reservation, 100.00, "Reservation is not Equal")
			s.Equal(res.Limit, 1000.00, "Limit is not equal")
		}
		if res.Kind == "disk" {
			s.Equal(res.Reservation, 100.00, "Reservation is not Equal")
			s.Equal(res.Limit, 1000.00, "Limit is not equal")
		}
		if res.Kind == "gpu" {
			s.Equal(res.Reservation, 2.00, "Reservation is not Equal")
			s.Equal(res.Limit, 4.00, "Limit is not equal")
		}
	}
}

func (s *resTreeTestSuite) TestPendingQueueError() {
	rt, ok := s.resourceTree.(*tree)
	s.Equal(true, ok)
	// Task -1
	jobID1 := &peloton.JobID{
		Value: "job1",
	}
	taskID1 := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", jobID1.Value, 1),
	}
	taskItem1 := &resmgr.Task{
		Name:     "job1-1",
		Priority: 0,
		JobId:    jobID1,
		Id:       taskID1,
	}
	err := rt.resPools["respool1"].EnqueueGang(makeTaskGang(taskItem1))
	s.EqualError(
		err,
		"resource pool respool1 is not a leaf node",
	)
}

func (s *resTreeTestSuite) TestPendingQueue() {
	rt, ok := s.resourceTree.(*tree)
	s.Equal(true, ok)
	// Task -1
	jobID1 := &peloton.JobID{
		Value: "job1",
	}
	taskID1 := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", jobID1.Value, 1),
	}
	taskItem1 := &resmgr.Task{
		Name:     "job1-1",
		Priority: 0,
		JobId:    jobID1,
		Id:       taskID1,
		Resource: &task.ResourceConfig{
			CpuLimit:    1,
			DiskLimitMb: 10,
			GpuLimit:    0,
			MemLimitMb:  100,
		},
		Preemptible: true,
	}
	rt.resPools["respool11"].EnqueueGang(makeTaskGang(taskItem1))

	// Task -2
	jobID2 := &peloton.JobID{
		Value: "job1",
	}
	taskID2 := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", jobID2.Value, 2),
	}
	taskItem2 := &resmgr.Task{
		Name:     "job1-2",
		Priority: 0,
		JobId:    jobID2,
		Id:       taskID2,
		Resource: &task.ResourceConfig{
			CpuLimit:    1,
			DiskLimitMb: 10,
			GpuLimit:    0,
			MemLimitMb:  100,
		},
		Preemptible: true,
	}
	rt.resPools["respool11"].SetNonSlackEntitlement(s.getEntitlement())
	rt.resPools["respool11"].EnqueueGang(makeTaskGang(taskItem2))

	gangList3, err := rt.resPools["respool11"].DequeueGangs(1)
	if err != nil {
		s.Fail("Dequeue should not fail")
	}
	if len(gangList3) != 1 {
		s.Fail("Dequeue should return single task gang")
	}
	gang := gangList3[0]
	if len(gang.Tasks) != 1 {
		s.Fail("Dequeue single task gang should be length 1")
	}
	t1 := gang.Tasks[0]
	s.Equal(t1.JobId.Value, "job1", "Should get Job-1")
	s.Equal(t1.Id.GetValue(), "job1-1", "Should get Job-1 and Task-1")

	gangList4, err2 := rt.resPools["respool11"].DequeueGangs(1)
	if err2 != nil {
		s.Fail("Dequeue should not fail")
	}
	if len(gangList4) != 1 {
		s.Fail("Dequeue should return single task gang")
	}
	gang = gangList4[0]
	if len(gang.Tasks) != 1 {
		s.Fail("Dequeue single task gang should be length 1")
	}
	t2 := gang.Tasks[0]
	s.Equal(t2.JobId.Value, "job1", "Should get Job-1")
	s.Equal(t2.Id.GetValue(), "job1-2", "Should get Job-1 and Task-1")
}

func (s *resTreeTestSuite) TestUpsertExistingResourcePoolConfig() {
	select {
	default:
	case <-s.resourceTree.UpdatedChannel():
		s.Fail("update channel should be empty")
	}

	mockExistingResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool23",
	}

	mockParentPoolID := &peloton.ResourcePoolID{
		Value: "respool22",
	}

	mockResourcePoolConfig := &respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*respool.ResourceConfig{
			{
				Reservation: 10,
				Kind:        "cpu",
				Limit:       50,
				Share:       2,
			},
		},
		Policy: respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockParentPoolID.Value,
	}

	err := s.resourceTree.Upsert(mockExistingResourcePoolID, mockResourcePoolConfig)
	s.NoError(err)

	<-s.resourceTree.UpdatedChannel()
}

func (s *resTreeTestSuite) TestUpsertNewResourcePoolConfig() {
	mockExistingResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool24",
	}

	mockParentPoolID := &peloton.ResourcePoolID{
		Value: "respool23",
	}

	mockResourcePoolConfig := &respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*respool.ResourceConfig{
			{
				Reservation: 10,
				Kind:        "cpu",
				Limit:       50,
				Share:       2,
			},
		},
		Policy: respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockParentPoolID.Value,
	}

	err := s.resourceTree.Upsert(mockExistingResourcePoolID, mockResourcePoolConfig)
	s.NoError(err)
}

func (s *resTreeTestSuite) TestUpsertNewResourcePoolConfigError() {
	mockExistingResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool200",
	}

	mockParentPoolID := &peloton.ResourcePoolID{
		Value: "respool23",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 10,
				Kind:        "cpu",
				Limit:       50,
				Share:       2,
			},
		},
		Name: mockParentPoolID.Value,
	}

	err := s.resourceTree.Upsert(mockExistingResourcePoolID, mockResourcePoolConfig)
	s.EqualError(
		err,
		"failed to insert resource pool: respool200: error creating resource"+
			" pool respool200: invalid queue type",
	)
}

func (s *resTreeTestSuite) TestResourcePoolPath() {
	// Get Root
	resPool, err := s.resourceTree.GetByPath(&respool.ResourcePoolPath{
		Value: "/",
	})
	s.NoError(err)
	s.Equal(resPool.Name(), "root")
	s.Equal(resPool.GetPath(), "/")
	s.True(resPool.IsRoot())

	// Get respool1
	resPool, err = s.resourceTree.GetByPath(&respool.ResourcePoolPath{
		Value: "/respool1",
	})
	s.NoError(err)
	s.Equal(resPool.Name(), "respool1")
	s.Equal(resPool.GetPath(), "/respool1")
	s.Equal(resPool.Parent().Name(), "root")
	s.False(resPool.IsRoot())

	// Get respool11
	resPool, err = s.resourceTree.GetByPath(&respool.ResourcePoolPath{
		Value: "/respool1/respool11",
	})
	s.NoError(err)
	s.Equal(resPool.Name(), "respool11")
	s.Equal(resPool.GetPath(), "/respool1/respool11")
	s.Equal(resPool.GetPath(), "/respool1/respool11")
	s.Equal(resPool.Parent().Name(), "respool1")
	s.False(resPool.IsRoot())

	// Get non-existent pool
	resPool, err = s.resourceTree.GetByPath(&respool.ResourcePoolPath{
		Value: "/doesnotexist",
	})
	s.Error(err)

	// Get non-existent pool
	resPool, err = s.resourceTree.GetByPath(&respool.ResourcePoolPath{
		Value: "/respool1/respool11/doesnotexist",
	})
	s.Error(err)

	// Get on uninitialized tree fails.
	s.resourceTree.(*tree).root = nil
	resPool, err = s.resourceTree.GetByPath(&respool.ResourcePoolPath{
		Value: "/",
	})
	s.Error(err)
}

func (s *resTreeTestSuite) TestGetAllNodes() {
	nodes := s.resourceTree.GetAllNodes(false)
	s.Equal(10, nodes.Len())

	nodes = s.resourceTree.GetAllNodes(true)
	s.Equal(5, nodes.Len())
}

func (s *resTreeTestSuite) TestGet() {
	tt := []struct {
		respoolID string
		err       string
	}{
		{
			respoolID: "root",
			err:       "",
		},
		{
			respoolID: "doesnotexist",
			err:       "resource pool (doesnotexist) not found",
		},
	}

	for _, t := range tt {
		node, err := s.resourceTree.Get(&peloton.ResourcePoolID{Value: t.respoolID})
		if err != nil {
			s.Nil(node)
			s.EqualError(err, t.err)
			continue
		}
		s.NoError(err)
		s.Equal(t.respoolID, node.ID())
	}
}

func (s *resTreeTestSuite) TestConvertTask() {
	ti := &task.TaskInfo{
		JobId: &peloton.JobID{
			Value: "job-1",
		},
		InstanceId: 1,
		Config:     &task.TaskConfig{},
		Runtime:    &task.RuntimeInfo{},
	}
	jobConfig := &job.JobConfig{
		SLA: &job.SlaConfig{
			Preemptible: true,
			Priority:    12,
		},
	}

	rmtask := taskutil.ConvertTaskToResMgrTask(ti, jobConfig)
	s.NotNil(rmtask)
	s.EqualValues(rmtask.Priority, 12)
	s.EqualValues(rmtask.Preemptible, true)

	ti = &task.TaskInfo{
		JobId: &peloton.JobID{
			Value: "job-2",
		},
		InstanceId: 1,
		Config:     &task.TaskConfig{},
		Runtime:    &task.RuntimeInfo{},
	}
	jobConfig = &job.JobConfig{}

	rmtask = taskutil.ConvertTaskToResMgrTask(ti, jobConfig)
	s.NotNil(rmtask)
	s.EqualValues(rmtask.Priority, 0)
	s.EqualValues(rmtask.Preemptible, false)
}

func (s *resTreeTestSuite) TestDelete() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()

	resourceTree := s.getTree(s.withStore(s.getResPools(), nil))
	resourceTree.Start()

	s.Equal(10, resourceTree.GetAllNodes(false).Len())

	// delete respool 11
	resourceTree.Delete(&peloton.ResourcePoolID{Value: "respool11"})
	s.Equal(9, resourceTree.GetAllNodes(false).Len())
	s.Equal(4, resourceTree.GetAllNodes(true).Len())

	// delete respool 1
	resourceTree.Delete(&peloton.ResourcePoolID{Value: "respool1"})
	s.Equal(7, resourceTree.GetAllNodes(false).Len())
	s.Equal(3, resourceTree.GetAllNodes(true).Len())
}

func TestPelotonResPool(t *testing.T) {
	suite.Run(t, new(resTreeTestSuite))
}

// TestStartError Tests Start method for error
func (s *resTreeTestSuite) TestStartError() {

	tt := []struct {
		msg         string
		poolConfigs map[string]*respool.ResourcePoolConfig
		err         error
		reterr      error
	}{
		{
			msg:         "error from store",
			poolConfigs: nil,
			err:         errors.New("error from store"),
			reterr:      nil,
		},
		{
			msg:         "tree start failure",
			poolConfigs: nil,
			err:         nil,
			reterr:      errors.New("failed to start tree"),
		},
		{
			msg:         "tree start successful",
			poolConfigs: make(map[string]*respool.ResourcePoolConfig),
			err:         nil,
			reterr:      nil,
		},
	}

	for _, t := range tt {
		resourceTree := s.getTree(s.withStore(t.poolConfigs, t.err))
		err := resourceTree.Start()
		if t.err != nil || t.reterr != nil {
			s.Error(err, t.msg)
			if t.reterr != nil {
				s.Contains(err.Error(), t.reterr.Error(), t.msg)
			} else {
				s.Contains(err.Error(), t.err.Error(), t.msg)
			}
		} else {
			s.NoError(err, t.msg)
		}
	}
}

// TestBuildTreeError Tests Build tree method for the errors
func (s *resTreeTestSuite) TestBuildTreeError() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	resourceTree := s.getTree(
		s.withStore(nil, nil),
	)
	_, err := resourceTree.buildTree("", nil, make(map[string]*respool.ResourcePoolConfig))
	s.Error(err)
	s.Contains(err.Error(), "error creating resource pool")
}

// Test utils -----
// ________________

func (s *resTreeTestSuite) withStore(
	pool map[string]*respool.ResourcePoolConfig,
	err error) ormobjects.ResPoolOps {
	mockResPoolOps := objectmocks.NewMockResPoolOps(s.mockCtrl)
	mockResPoolOps.
		EXPECT().
		GetAll(context.Background()).
		Return(pool, err).
		AnyTimes()
	return mockResPoolOps
}

// Creates and returns the Tree with respool store
func (s *resTreeTestSuite) getTree(respoolOps ormobjects.ResPoolOps) *tree {
	return &tree{
		respoolOps:  respoolOps,
		root:        nil,
		metrics:     NewMetrics(tally.NoopScope),
		resPools:    make(map[string]ResPool),
		jobStore:    nil,
		taskStore:   nil,
		scope:       tally.NoopScope,
		updatedChan: make(chan struct{}, 1),
	}
}

// Returns resource configs
func (s *resTreeTestSuite) getResourceConfig() []*respool.ResourceConfig {

	resConfigs := []*respool.ResourceConfig{
		{
			Share:       1,
			Kind:        "cpu",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "memory",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "disk",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "gpu",
			Reservation: 2,
			Limit:       4,
		},
	}
	return resConfigs
}

// Returns resource pools
func (s *resTreeTestSuite) getResPools() map[string]*respool.ResourcePoolConfig {

	rootID := peloton.ResourcePoolID{Value: common.RootResPoolID}
	policy := respool.SchedulingPolicy_PriorityFIFO

	return map[string]*respool.ResourcePoolConfig{
		// NB: root resource pool node is not stored in the database
		"respool1": {
			Name:      "respool1",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool2": {
			Name:      "respool2",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool3": {
			Name:      "respool3",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool11": {
			Name:      "respool11",
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool12": {
			Name:      "respool12",
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool21": {
			Name:      "respool21",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool22": {
			Name:      "respool22",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool23": {
			Name:   "respool23",
			Parent: &peloton.ResourcePoolID{Value: "respool22"},
			Resources: []*respool.ResourceConfig{
				{
					Kind:        "cpu",
					Reservation: 50,
					Limit:       100,
					Share:       1,
				},
			},
			Policy: policy,
		},
		"respool99": {
			Name:   "respool99",
			Parent: &peloton.ResourcePoolID{Value: "respool21"},
			Resources: []*respool.ResourceConfig{
				{
					Kind:        "cpu",
					Reservation: 50,
					Limit:       100,
					Share:       1,
				},
			},
			Policy: policy,
		},
	}
}

func (s *resTreeTestSuite) getEntitlement() *scalar.Resources {
	return &scalar.Resources{
		CPU:    100,
		MEMORY: 1000,
		DISK:   100,
		GPU:    2,
	}
}
