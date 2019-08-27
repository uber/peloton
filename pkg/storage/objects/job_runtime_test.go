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
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	ormmocks "github.com/uber/peloton/pkg/storage/orm/mocks"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

type JobRuntimeObjectTestSuite struct {
	suite.Suite
	jobID   *peloton.JobID
	runtime *job.RuntimeInfo
}

func (s *JobRuntimeObjectTestSuite) SetupTest() {
	setupTestStore()
	s.buildRuntime()
}

func TestJobRuntimeObjectSuite(t *testing.T) {
	suite.Run(t, new(JobRuntimeObjectTestSuite))
}

// TestUpsertGetDeleteJobRuntime tests creating/deleting JobRuntimeObject in DB
func (s *JobRuntimeObjectTestSuite) TestUpsertGetDeleteJobRuntime() {
	jobRuntimeOps := NewJobRuntimeOps(testStore)
	ctx := context.Background()

	err := jobRuntimeOps.Upsert(ctx, s.jobID, s.runtime)
	s.NoError(err)

	runtime, err := jobRuntimeOps.Get(ctx, s.jobID)
	s.NoError(err)

	s.True(proto.Equal(runtime, s.runtime))

	err = jobRuntimeOps.Delete(ctx, s.jobID)
	s.NoError(err)

	_, err = jobRuntimeOps.Get(ctx, s.jobID)
	s.Error(err)
	s.True(yarpcerrors.IsNotFound(err))
}

// TestCreateGetDeleteJobRuntimeFail tests failure cases due to ORM Client errors
func (s *JobRuntimeObjectTestSuite) TestCreateGetDeleteJobRuntimeFail() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	mockClient := ormmocks.NewMockClient(ctrl)
	mockStore := &Store{oClient: mockClient, metrics: testStore.metrics}
	runtimeOps := NewJobRuntimeOps(mockStore)

	mockClient.EXPECT().Create(gomock.Any(), gomock.Any()).
		Return(errors.New("create failed"))
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("get failed"))
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).
		Return(errors.New("delete failed"))

	ctx := context.Background()

	err := runtimeOps.Upsert(ctx, s.jobID, s.runtime)
	s.Error(err)
	s.Equal("create failed", err.Error())

	_, err = runtimeOps.Get(ctx, s.jobID)
	s.Error(err)
	s.Equal("get failed", err.Error())

	err = runtimeOps.Delete(ctx, s.jobID)
	s.Error(err)
	s.Equal("delete failed", err.Error())
}

func (s *JobRuntimeObjectTestSuite) buildRuntime() {
	s.jobID = &peloton.JobID{Value: uuid.New()}
	now := time.Now()

	s.runtime = &job.RuntimeInfo{
		State:        job.JobState_INITIALIZED,
		CreationTime: now.Format(time.RFC3339Nano),
		TaskStats:    make(map[string]uint32),
		GoalState:    job.JobState_SUCCEEDED,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(now.UnixNano()),
			UpdatedAt: uint64(now.UnixNano()),
			Version:   1,
		},
		ConfigurationVersion: 1,
	}
}
