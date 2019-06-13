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

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	ormmocks "github.com/uber/peloton/pkg/storage/orm/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

type JobNameToIDObjectTestSuite struct {
	suite.Suite
}

func (s *JobNameToIDObjectTestSuite) SetupTest() {
	setupTestStore()
}

func TestJobNameToIDObjectSuite(t *testing.T) {
	suite.Run(t, new(JobNameToIDObjectTestSuite))
}

// TestCreateGetAllJobNameToID tests creating/deleting JobNameToIDObject in DB
func (s *JobNameToIDObjectTestSuite) TestCreateGetAllJobNameToID() {
	db := NewJobNameToIDOps(testStore)
	ctx := context.Background()

	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	testcases := []struct {
		name         string
		ids          []string
		expectedObjs []*JobNameToIDObject
	}{
		{
			name: "test-job",
			ids:  []string{id1},
			expectedObjs: []*JobNameToIDObject{
				{
					JobName: "test-job",
					JobID:   id1,
				},
			},
		},
		{
			name: "test-job-multiple-ids",
			ids:  []string{id1, id2, id3},
			expectedObjs: []*JobNameToIDObject{
				{
					JobName: "test-job-multiple-ids",
					JobID:   id3,
				},
				{
					JobName: "test-job-multiple-ids",
					JobID:   id2,
				},
				{
					JobName: "test-job-multiple-ids",
					JobID:   id1,
				},
			},
		},
	}

	for _, tc := range testcases {
		// verify newJobNameToIDObject
		for _, id := range tc.ids {
			err := db.Create(ctx, tc.name, &peloton.JobID{Value: id})
			s.NoError(err)
		}

		objs, err := db.GetAll(ctx, tc.name)
		s.NoError(err)
		s.Equal(len(objs), len(tc.expectedObjs))

		for i, obj := range objs {
			s.Equal(obj.JobID, tc.expectedObjs[i].JobID)
			s.Equal(obj.JobName, tc.expectedObjs[i].JobName)
		}
	}
}

// TestJobIndexOpsClientFail tests failure cases due to ORM Client errors
func (s *JobNameToIDObjectTestSuite) TestJobNameToIDOpsClientFail() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	mockClient := ormmocks.NewMockClient(ctrl)
	mockStore := &Store{oClient: mockClient, metrics: testStore.metrics}
	db := NewJobNameToIDOps(mockStore)

	mockClient.EXPECT().Create(gomock.Any(), gomock.Any()).
		Return(errors.New("create failed"))
	mockClient.EXPECT().GetAll(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("getall failed"))

	ctx := context.Background()

	err := db.Create(ctx, "test", &peloton.JobID{Value: uuid.New()})
	s.Error(err)
	s.Equal("create failed", err.Error())

	_, err = db.GetAll(ctx, "test")
	s.Error(err)
	s.Equal("getall failed", err.Error())
}
