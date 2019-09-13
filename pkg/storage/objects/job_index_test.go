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

package objects

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/pkg/errors"
	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/pkg/storage/objects/base"
	ormmocks "github.com/uber/peloton/pkg/storage/orm/mocks"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

type JobIndexObjectTestSuite struct {
	suite.Suite
	config                                *job.JobConfig
	configNoInst                          *job.JobConfig
	runtime                               *job.RuntimeInfo
	configMarshaled                       string
	labelsMarshaled                       string
	runtimeMarshaled                      string
	slaMarshaled                          string
	createTime, startTime, completionTime time.Time
}

func (s *JobIndexObjectTestSuite) SetupTest() {
	setupTestStore()
	s.buildConfig()
	s.buildRuntime()
}

func TestJobIndexObjectSuite(t *testing.T) {
	suite.Run(t, new(JobIndexObjectTestSuite))
}

// TestCreateDeleteJobIndex tests creating/deleting JobIndexObject in DB
func (s *JobIndexObjectTestSuite) TestCreateDeleteJobIndex() {
	db := NewJobIndexOps(testStore)
	ctx := context.Background()

	testcases := []struct {
		description string
		id          string
		config      *job.JobConfig
		runtime     *job.RuntimeInfo
		expectedObj *JobIndexObject
	}{
		{
			description: "With config and runtime",
			id:          uuid.New(),
			config:      s.config,
			runtime:     s.runtime,
			expectedObj: &JobIndexObject{
				JobType:        1,
				Name:           "my-test-job",
				Owner:          "peloton",
				RespoolID:      "infinite",
				Config:         s.configMarshaled,
				InstanceCount:  8,
				Labels:         s.labelsMarshaled,
				RuntimeInfo:    s.runtimeMarshaled,
				State:          "RUNNING",
				CreationTime:   s.createTime,
				StartTime:      s.startTime,
				CompletionTime: s.completionTime,
				SLA:            s.slaMarshaled,
			},
		},
		{
			description: "With config only",
			id:          uuid.New(),
			config:      s.config,
			runtime:     nil,
			expectedObj: &JobIndexObject{
				JobType:       1,
				Name:          "my-test-job",
				Owner:         "peloton",
				RespoolID:     "infinite",
				Config:        s.configMarshaled,
				InstanceCount: 8,
				Labels:        s.labelsMarshaled,
				SLA:           s.slaMarshaled,
			},
		},
		{
			description: "With runtime only",
			id:          uuid.New(),
			config:      nil,
			runtime:     s.runtime,
			expectedObj: &JobIndexObject{
				RuntimeInfo:    s.runtimeMarshaled,
				State:          "RUNNING",
				CreationTime:   s.createTime,
				StartTime:      s.startTime,
				CompletionTime: s.completionTime,
			},
		},
	}

	delta, err := time.ParseDuration("1s")
	s.NoError(err)
	for _, tc := range testcases {
		jobID := &peloton.JobID{Value: tc.id}
		tc.expectedObj.JobID = base.NewOptionalString(tc.id)

		// verify newJobIndexObject
		obj, err := newJobIndexObject(jobID, tc.config, tc.runtime)
		s.NoError(err)
		obj.UpdateTime = time.Time{}
		s.Equal(tc.expectedObj, obj, tc.description)

		err = db.Create(ctx, jobID, tc.config, tc.runtime)
		s.NoError(err)

		obj, err = db.Get(ctx, jobID)
		s.NoError(err)

		if tc.runtime != nil {
			s.WithinDuration(time.Now(), obj.UpdateTime, delta)
		}

		obj.UpdateTime = time.Time{}
		s.Equal(tc.expectedObj, obj, tc.description)

		err = db.Delete(ctx, jobID)
		s.NoError(err)
		_, err = db.Get(ctx, jobID)
		s.Error(err)
		s.True(yarpcerrors.IsNotFound(err))
	}
}

// TestGetAllJobIndex tests getting all JobIndexObjects in DB
func (s *JobIndexObjectTestSuite) TestGetAllJobIndex() {
	db := NewJobIndexOps(testStore)
	ctx := context.Background()

	testcases := []struct {
		description string
		id          string
		expectedObj *JobIndexObject
	}{
		{
			description: "dummy job 1",
			id:          uuid.New(),
		},
		{
			description: "dummy job 2",
			id:          uuid.New(),
		},
	}

	jobId_1 := &peloton.JobID{Value: testcases[0].id}
	jobId_2 := &peloton.JobID{Value: testcases[1].id}

	expectedObj_1 := &job.JobSummary{
		Id:            jobId_1,
		Name:          s.config.Name,
		Owner:         s.config.OwningTeam,
		OwningTeam:    s.config.OwningTeam,
		InstanceCount: s.config.InstanceCount,
		Type:          s.config.Type,
		RespoolID:     s.config.RespoolID,
		Runtime:       s.runtime,
		Labels:        s.config.Labels,
		SLA:           s.config.SLA,
	}

	expectedObj_2 := &job.JobSummary{
		Id:            jobId_2,
		Name:          s.config.Name,
		Owner:         s.config.OwningTeam,
		OwningTeam:    s.config.OwningTeam,
		InstanceCount: s.config.InstanceCount,
		Type:          s.config.Type,
		RespoolID:     s.config.RespoolID,
		Runtime:       s.runtime,
		Labels:        s.config.Labels,
		SLA:           s.config.SLA,
	}

	err := db.Create(ctx, jobId_1, s.config, s.runtime)
	s.NoError(err)
	err = db.Create(ctx, jobId_2, s.config, s.runtime)
	s.NoError(err)

	objs, err := db.GetAll(ctx)
	s.NoError(err)
	s.Len(objs, 2)

	for _, summary := range objs {
		if summary.Id.GetValue() == expectedObj_1.Id.GetValue() {
			s.True(reflect.DeepEqual(expectedObj_1, summary))
		} else if summary.Id.GetValue() == expectedObj_2.Id.GetValue() {
			s.True(reflect.DeepEqual(expectedObj_2, summary))
		} else {
			s.Fail("GetAll doesn't get the correct jobSummary.")
		}
	}
}

// TestGetSummary tests fetching JobSummary for a JobIndexObject
func (s *JobIndexObjectTestSuite) TestGetSummary() {
	db := NewJobIndexOps(testStore)
	ctx := context.Background()
	jobID := &peloton.JobID{Value: uuid.New()}

	err := db.Create(ctx, jobID, s.config, s.runtime)
	s.NoError(err)

	summary, err := db.GetSummary(ctx, jobID)
	s.NoError(err)

	err = db.Create(ctx, jobID, s.config, s.runtime)
	summary, err = db.GetSummary(ctx, jobID)
	s.NoError(err)

	expectedSummary := &job.JobSummary{
		Id:            jobID,
		Name:          s.config.Name,
		Owner:         s.config.OwningTeam,
		OwningTeam:    s.config.OwningTeam,
		InstanceCount: s.config.InstanceCount,
		Type:          s.config.Type,
		RespoolID:     s.config.RespoolID,
		Runtime:       s.runtime,
		Labels:        s.config.Labels,
		SLA:           s.config.SLA,
	}
	s.Equal(expectedSummary, summary)
}

// TestUpdateJobIndex tests updating JobIndexObject in DB
func (s *JobIndexObjectTestSuite) TestUpdateJobIndex() {
	db := NewJobIndexOps(testStore)
	ctx := context.Background()

	testcases := []struct {
		description string
		newConfig   *job.JobConfig
		newRuntime  *job.RuntimeInfo
		expectedObj *JobIndexObject
	}{
		{
			description: "Update config and runtime",
			newConfig:   s.config,
			newRuntime:  s.runtime,
			expectedObj: &JobIndexObject{
				JobType:        1,
				Name:           "my-test-job",
				Owner:          "peloton",
				RespoolID:      "infinite",
				Config:         s.configMarshaled,
				InstanceCount:  8,
				Labels:         s.labelsMarshaled,
				RuntimeInfo:    s.runtimeMarshaled,
				State:          "RUNNING",
				CreationTime:   s.createTime,
				StartTime:      s.startTime,
				CompletionTime: s.completionTime,
				SLA:            s.slaMarshaled,
			},
		},
		{
			description: "Update config only",
			newConfig:   s.config,
			newRuntime:  nil,
			expectedObj: &JobIndexObject{
				JobType:       1,
				Name:          "my-test-job",
				Owner:         "peloton",
				RespoolID:     "infinite",
				Config:        s.configMarshaled,
				InstanceCount: 8,
				Labels:        s.labelsMarshaled,
				SLA:           s.slaMarshaled,
			},
		},
		{
			description: "Update runtime only",
			newConfig:   nil,
			newRuntime:  s.runtime,
			expectedObj: &JobIndexObject{
				RuntimeInfo:    s.runtimeMarshaled,
				State:          "RUNNING",
				CreationTime:   s.createTime,
				StartTime:      s.startTime,
				CompletionTime: s.completionTime,
			},
		},
		{
			description: "Update nothing",
			newConfig:   nil,
			newRuntime:  nil,
			expectedObj: &JobIndexObject{},
		},
	}

	delta, err := time.ParseDuration("1s")
	s.NoError(err)
	for _, tc := range testcases {
		jobID := &peloton.JobID{Value: uuid.New()}
		tc.expectedObj.JobID = base.NewOptionalString(jobID.Value)

		err = db.Create(ctx, jobID, nil, nil)
		s.NoError(err)

		err = db.Update(ctx, jobID, tc.newConfig, tc.newRuntime)
		s.NoError(err)

		obj, err := db.Get(ctx, jobID)
		s.NoError(err)

		if tc.newRuntime != nil {
			s.WithinDuration(time.Now(), obj.UpdateTime, delta)
		}
		obj.UpdateTime = time.Time{}
		s.Equal(tc.expectedObj, obj, tc.description)

		err = db.Delete(ctx, jobID)
		s.NoError(err)
	}
}

// TestJobIndexOpsClientFail tests failure cases due to ORM Client errors
func (s *JobIndexObjectTestSuite) TestJobIndexOpsClientFail() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	mockClient := ormmocks.NewMockClient(ctrl)
	mockStore := &Store{oClient: mockClient, metrics: testStore.metrics}
	indexOps := NewJobIndexOps(mockStore)

	mockClient.EXPECT().Create(gomock.Any(), gomock.Any()).
		Return(errors.New("create failed"))
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("get failed")).Times(2)
	mockClient.EXPECT().GetAll(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("getAll failed"))
	mockClient.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("update failed"))
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).
		Return(errors.New("delete failed"))

	ctx := context.Background()
	jobID := &peloton.JobID{Value: uuid.New()}

	err := indexOps.Create(ctx, jobID, s.config, s.runtime)
	s.Error(err)
	s.Equal("create failed", err.Error())

	_, err = indexOps.Get(ctx, jobID)
	s.Error(err)
	s.Equal("get failed", err.Error())

	_, err = indexOps.GetAll(ctx)
	s.Error(err)
	s.Equal("getAll failed", err.Error())

	_, err = indexOps.GetSummary(ctx, jobID)
	s.Error(err)
	s.Equal("get failed", err.Error())

	err = indexOps.Update(ctx, jobID, s.config, s.runtime)
	s.Error(err)
	s.Equal("update failed", err.Error())

	err = indexOps.Delete(ctx, jobID)
	s.Error(err)
	s.Equal("delete failed", err.Error())
}

// TestToJobSummary tests converting JobIndexObject to JobSummary
func (s *JobIndexObjectTestSuite) TestToJobSummary() {
	jobID := &peloton.JobID{Value: uuid.New()}
	obj, err := newJobIndexObject(jobID, s.config, s.runtime)
	s.NoError(err)

	expectedSummary := &job.JobSummary{
		Id:            jobID,
		Name:          s.config.Name,
		Owner:         s.config.OwningTeam,
		OwningTeam:    s.config.OwningTeam,
		InstanceCount: s.config.InstanceCount,
		Type:          s.config.Type,
		RespoolID:     s.config.RespoolID,
		Runtime:       s.runtime,
		Labels:        s.config.Labels,
		SLA:           s.config.SLA,
	}
	summary, err := obj.ToJobSummary()
	s.NoError(err)
	s.Equal(expectedSummary, summary)

	// bad runtime and labels
	obj.RuntimeInfo = "foo"
	obj.Labels = "bar"
	expectedSummary.Runtime = nil
	expectedSummary.Labels = nil
	summary, err = obj.ToJobSummary()
	s.Error(err)

	// job index with all default value
	defaultJobIndex := &JobIndexObject{JobID: &base.OptionalString{Value: jobID.GetValue()}}
	summary, err = defaultJobIndex.ToJobSummary()
	s.NoError(err)
	s.Empty(summary.Labels)
	s.Empty(summary.Name)
	s.Nil(summary.Runtime)

	// job index with data in bad format intentionally
	jobIndexWithBadData := &JobIndexObject{JobID: &base.OptionalString{Value: jobID.GetValue()}}
	jobIndexWithBadData.Labels = "abc"
	summary, err = jobIndexWithBadData.ToJobSummary()
	s.Error(err)
	s.Nil(summary)
}

// Tests the failure scenarios for new job index object
func (s *JobIndexObjectTestSuite) TestNewJobIndexObject_Errors() {

	jobID := &peloton.JobID{
		Value: uuid.New(),
	}

	testcases := []struct {
		description string
		config      *job.JobConfig
		runtime     *job.RuntimeInfo
		expectedObj *JobIndexObject
	}{
		{
			description: "Bad Runtime: Creation Time",
			runtime: &job.RuntimeInfo{
				CreationTime: "dummy_value",
			},
		},
		{
			description: "Bad Runtime: Start Time",
			runtime: &job.RuntimeInfo{
				StartTime: "dummy_value",
			},
		},
		{
			description: "Bad Runtime: Completion Time",
			runtime: &job.RuntimeInfo{
				CompletionTime: "dummy_value",
			},
		},
	}

	for _, t := range testcases {
		_, err := newJobIndexObject(
			jobID,
			t.config,
			t.runtime,
		)
		s.Error(err)
	}
}

func (s *JobIndexObjectTestSuite) buildConfig() {
	cmd1 := "sleep"
	cmd2 := "echo peloton rules"
	s.configNoInst = &job.JobConfig{
		Name:        "my-test-job",
		Type:        job.JobType_SERVICE,
		Owner:       "me",
		OwningTeam:  "peloton",
		LdapGroups:  []string{"compute", "infra"},
		Description: "simple job",
		Labels: []*peloton.Label{
			{Key: "org", Value: "peloton"},
			{Key: "rack", Value: "top"},
		},
		InstanceCount: 8,
		DefaultConfig: &task.TaskConfig{
			Name: "default",
			Command: &mesos_v1.CommandInfo{
				Value: &cmd1,
			},
		},
		RespoolID: &peloton.ResourcePoolID{Value: "infinite"},
		SLA: &job.SlaConfig{
			Preemptible:                 false,
			Revocable:                   false,
			MaximumUnavailableInstances: uint32(1),
		},
	}
	buf, _ := json.Marshal(s.configNoInst)
	s.configMarshaled = string(buf)
	buf, _ = json.Marshal(s.configNoInst.Labels)
	s.labelsMarshaled = string(buf)
	buf, _ = json.Marshal(s.configNoInst.GetSLA())
	s.slaMarshaled = string(buf)

	s.config = proto.Clone(s.configNoInst).(*job.JobConfig)
	s.config.InstanceConfig = map[uint32]*task.TaskConfig{
		2: {
			Name: "two",
			Command: &mesos_v1.CommandInfo{
				Value: &cmd2,
			},
		},
		5: {Name: "five"},
	}
}

func (s *JobIndexObjectTestSuite) buildRuntime() {
	rt := &job.RuntimeInfo{
		State:                job.JobState_RUNNING,
		GoalState:            job.JobState_SUCCEEDED,
		ConfigurationVersion: 5,
		CreationTime:         "2019-01-25T18:56:05.705Z",
		StartTime:            "2019-01-25T18:56:07.779Z",
		CompletionTime:       "2019-01-25T21:31:52.474Z",
		TaskStats: map[string]uint32{
			"RUNNING":   3,
			"LAUNCHED":  4,
			"SUCCEEDED": 1,
		},
		ResourceUsage: map[string]float64{
			"cpu": 376.4,
			"fd":  80,
		},
	}
	buf, _ := json.Marshal(rt)
	s.runtimeMarshaled = string(buf)
	s.runtime = rt
	s.createTime, _ = time.Parse(time.RFC3339Nano, rt.CreationTime)
	s.startTime, _ = time.Parse(time.RFC3339Nano, rt.StartTime)
	s.completionTime, _ = time.Parse(time.RFC3339Nano, rt.CompletionTime)
}
