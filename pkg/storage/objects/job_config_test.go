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
	"errors"
	"testing"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/private/models"

	ormmocks "github.com/uber/peloton/pkg/storage/orm/mocks"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

type JobConfigObjectTestSuite struct {
	suite.Suite
	jobID       *peloton.JobID
	config      *job.JobConfig
	configAddOn *models.ConfigAddOn
	spec        *stateless.JobSpec
}

func (s *JobConfigObjectTestSuite) SetupTest() {
	setupTestStore()
	s.buildConfig()
}

func TestJobConfigObjectSuite(t *testing.T) {
	suite.Run(t, new(JobConfigObjectTestSuite))
}

// TestCreateDeleteJobConfig tests creating/deleting JobConfigObject in DB
func (s *JobConfigObjectTestSuite) TestCreateGetDeleteJobConfig() {
	jobConfigOps := NewJobConfigOps(testStore)
	ctx := context.Background()

	version := uint64(1)

	err := jobConfigOps.Create(
		ctx,
		s.jobID,
		s.config,
		s.configAddOn,
		s.spec,
		version)
	s.NoError(err)

	config, configAddOn, err := jobConfigOps.Get(ctx, s.jobID, version)
	s.NoError(err)
	s.True(proto.Equal(config, s.config))
	s.True(proto.Equal(configAddOn, s.configAddOn))

	obj, err := jobConfigOps.GetResult(ctx, s.jobID, version)
	s.NoError(err)
	s.True(proto.Equal(obj.JobConfig, s.config))
	s.True(proto.Equal(obj.ConfigAddOn, s.configAddOn))
	s.True(proto.Equal(obj.JobSpec, s.spec))

	err = jobConfigOps.Delete(ctx, s.jobID, version)
	s.NoError(err)

	_, _, err = jobConfigOps.Get(ctx, s.jobID, version)
	s.Error(err)
	s.True(yarpcerrors.IsNotFound(err))
}

// TestGetCurrentVersion tests getting current version of JobConfigObject
func (s *JobConfigObjectTestSuite) TestGetCurrentVersion() {
	jobConfigOps := NewJobConfigOps(testStore)
	jobRuntimeOps := NewJobRuntimeOps(testStore)
	ctx := context.Background()

	runtime := &job.RuntimeInfo{
		State:                job.JobState_INITIALIZED,
		GoalState:            job.JobState_SUCCEEDED,
		ConfigurationVersion: 1,
	}

	err := jobRuntimeOps.Upsert(ctx, s.jobID, runtime)
	s.NoError(err)

	err = jobConfigOps.Create(
		ctx,
		s.jobID,
		s.config,
		s.configAddOn,
		s.spec,
		uint64(1))
	s.NoError(err)

	config, configAddOn, err := jobConfigOps.GetCurrentVersion(ctx, s.jobID)
	s.NoError(err)
	s.True(proto.Equal(config, s.config))
	s.True(proto.Equal(configAddOn, s.configAddOn))

	obj, err := jobConfigOps.GetResultCurrentVersion(ctx, s.jobID)
	s.NoError(err)
	s.True(proto.Equal(obj.JobConfig, s.config))
	s.True(proto.Equal(obj.ConfigAddOn, s.configAddOn))
	s.True(proto.Equal(obj.JobSpec, s.spec))
}

// TestCreateGetDeleteJobConfigFail tests failure cases due to ORM Client errors
func (s *JobConfigObjectTestSuite) TestCreateGetDeleteJobConfigFail() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	mockClient := ormmocks.NewMockClient(ctrl)
	mockStore := &Store{oClient: mockClient, metrics: testStore.metrics}
	configOps := NewJobConfigOps(mockStore)

	mockClient.EXPECT().CreateIfNotExists(gomock.Any(), gomock.Any()).
		Return(errors.New("createifnotexists failed"))
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("get failed")).Times(4)
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).
		Return(errors.New("delete failed"))

	ctx := context.Background()
	version := uint64(1)

	err := configOps.Create(
		ctx,
		s.jobID,
		s.config,
		s.configAddOn,
		s.spec,
		version)
	s.Error(err)
	s.Equal("createifnotexists failed", err.Error())

	_, _, err = configOps.Get(ctx, s.jobID, version)
	s.Error(err)
	s.Equal("get failed", err.Error())

	_, _, err = configOps.GetCurrentVersion(ctx, s.jobID)
	s.Error(err)
	s.Equal("Failed to get Job Runtime: get failed", err.Error())

	_, err = configOps.GetResult(ctx, s.jobID, version)
	s.Error(err)
	s.Equal("get failed", err.Error())

	_, err = configOps.GetResultCurrentVersion(ctx, s.jobID)
	s.Error(err)
	s.Equal("Failed to get Job Runtime: get failed", err.Error())

	err = configOps.Delete(ctx, s.jobID, version)
	s.Error(err)
	s.Equal("delete failed", err.Error())
}

func (s *JobConfigObjectTestSuite) buildConfig() {
	s.jobID = &peloton.JobID{Value: uuid.New()}

	cmd1 := "sleep"
	cmd2 := "echo peloton rules"
	s.config = &job.JobConfig{
		Name:        "my-test-job",
		Type:        job.JobType_SERVICE,
		Owner:       "peloton-eng",
		OwningTeam:  "peloton",
		LdapGroups:  []string{"compute", "infra"},
		Description: "simple job",
		Labels: []*peloton.Label{
			{Key: "org", Value: "peloton"},
			{Key: "rack", Value: "top"},
		},
		InstanceCount: 10,
		DefaultConfig: &task.TaskConfig{
			Name: "default",
			Command: &mesos_v1.CommandInfo{
				Value: &cmd1,
			},
		},
		RespoolID: &peloton.ResourcePoolID{Value: "infinite"},
		InstanceConfig: map[uint32]*task.TaskConfig{
			2: {
				Name: "two",
				Command: &mesos_v1.CommandInfo{
					Value: &cmd2,
				},
			},
			5: {Name: "five"},
		},
	}

	s.spec = &stateless.JobSpec{
		DefaultSpec: &pod.PodSpec{
			Containers: []*pod.ContainerSpec{
				{
					Entrypoint: &pod.CommandSpec{Value: cmd1},
				},
			},
		},
	}

	s.configAddOn = &models.ConfigAddOn{
		SystemLabels: []*peloton.Label{
			{
				Key:   "peloton.job_id",
				Value: s.jobID.GetValue(),
			},
		},
	}
}
