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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	ormmocks "github.com/uber/peloton/pkg/storage/orm/mocks"
)

type ActiveJobsTestSuite struct {
	suite.Suite
	jobId1 *peloton.JobID
	jobId2 *peloton.JobID
}

func TestActiveJobsSuite(t *testing.T) {
	suite.Run(t, new(ActiveJobsTestSuite))
}

func (s *ActiveJobsTestSuite) SetupTest() {
	setupTestStore()
	s.setupTestConfig()
}

// TestCreateGetAllDeleteActiveJobs tests creating and getting all the active jobs.
func (s *ActiveJobsTestSuite) TestCreateGetAllDeleteActiveJobs() {
	testActiveJobsOps := NewActiveJobsOps(testStore)
	ctx := context.Background()

	err := testActiveJobsOps.Create(ctx, s.jobId1)
	s.NoError(err)
	err = testActiveJobsOps.Create(ctx, s.jobId2)
	s.NoError(err)

	// GetAll should return the two created jobs.
	activeIdsResults, err := testActiveJobsOps.GetAll(ctx)
	s.NoError(err)
	s.Len(activeIdsResults, 2)
	valid := map[string]bool{
		activeIdsResults[0].GetValue(): true,
		activeIdsResults[1].GetValue(): true,
	}
	s.True(valid[s.jobId1.GetValue()])
	s.True(valid[s.jobId2.GetValue()])

	// clean up the created jobs.
	err = testActiveJobsOps.Delete(ctx, s.jobId1)
	s.NoError(err)
	err = testActiveJobsOps.Delete(ctx, s.jobId2)
	s.NoError(err)
}

// TestActiveJobsOpsClientFail tests failure cases due to ORM Client errors.
func (s *ActiveJobsTestSuite) TestActiveJobsOpsClientFail() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	mockClient := ormmocks.NewMockClient(ctrl)
	mockStore := &Store{oClient: mockClient, metrics: testStore.metrics}
	activeJobsOps := NewActiveJobsOps(mockStore)

	mockClient.EXPECT().Create(gomock.Any(), gomock.Any()).
		Return(errors.New("create failed"))
	mockClient.EXPECT().GetAll(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("getAll failed"))
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).
		Return(errors.New("delete failed"))

	ctx := context.Background()

	err := activeJobsOps.Create(ctx, s.jobId1)
	s.Error(err)
	s.Equal("create failed", err.Error())

	_, err = activeJobsOps.GetAll(ctx)
	s.Error(err)
	s.Equal("getAll failed", err.Error())

	err = activeJobsOps.Delete(ctx, s.jobId1)
	s.Error(err)
	s.Equal("delete failed", err.Error())
}

func (s *ActiveJobsTestSuite) setupTestConfig() {
	s.jobId1 = &peloton.JobID{Value: uuid.New()}
	s.jobId2 = &peloton.JobID{Value: uuid.New()}
}
