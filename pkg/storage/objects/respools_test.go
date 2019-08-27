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
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/pkg/storage/objects/base"
	ormmocks "github.com/uber/peloton/pkg/storage/orm/mocks"
)

type ResPoolsObjectTestSuite struct {
	suite.Suite
	respoolId1           *peloton.ResourcePoolID
	respoolId2           *peloton.ResourcePoolID
	resPoolConfig        *respool.ResourcePoolConfig
	updatedResPoolConfig *respool.ResourcePoolConfig
}

func TestRespoolObjectSuite(t *testing.T) {
	suite.Run(t, new(ResPoolsObjectTestSuite))
}

func (s *ResPoolsObjectTestSuite) SetupTest() {
	setupTestStore()
	s.buildResPoolConfig()
}

// TestCreateGetAllResourcePools tests creating and getting all
// the resource pools from store.
func (s *ResPoolsObjectTestSuite) TestCreateGetAllResourcePools() {
	testResPoolOps := NewResPoolOps(testStore)
	ctx := context.Background()

	// Create two resource pools.
	err := testResPoolOps.Create(ctx, s.respoolId1, s.resPoolConfig, "peloton")
	s.NoError(err)
	err = testResPoolOps.Create(ctx, s.respoolId2, s.resPoolConfig, "peloton")
	s.NoError(err)

	// Check the created respool matches what's created in the db.
	results, err := testResPoolOps.GetAll(ctx)
	s.NoError(err)
	s.Len(results, 2)
	s.Equal(s.resPoolConfig, results[s.respoolId2.GetValue()])
	s.Equal(s.resPoolConfig, results[s.respoolId1.GetValue()])

	// clean up the created respool
	err = testResPoolOps.Delete(ctx, s.respoolId1)
	s.NoError(err)
	err = testResPoolOps.Delete(ctx, s.respoolId2)
	s.NoError(err)
}

// TestUpdateResourcePool tests updating a resource pool from store.
func (s *ResPoolsObjectTestSuite) TestUpdateResourcePool() {
	testResPoolOps := NewResPoolOps(testStore)
	ctx := context.Background()

	// Test creating and updating the resource pool.
	err := testResPoolOps.Create(ctx, s.respoolId1, s.resPoolConfig, "peloton")
	s.NoError(err)

	resPoolOpsResult, err := testResPoolOps.GetResult(ctx, s.respoolId1.GetValue())
	s.NoError(err)
	s.Equal(s.resPoolConfig, resPoolOpsResult.RespoolConfig)

	// Update the resource pool
	err = testResPoolOps.Update(context.Background(), s.respoolId1, s.updatedResPoolConfig)
	s.NoError(err)

	updatedResPoolOpsResult, err := testResPoolOps.GetResult(ctx, s.respoolId1.GetValue())
	s.NoError(err)
	s.Equal(s.updatedResPoolConfig, updatedResPoolOpsResult.RespoolConfig)

	err = testResPoolOps.Delete(ctx, s.respoolId1)
	s.NoError(err)
}

// TestDeleteResourcePool tests deleting a resource pool from store.
func (s *ResPoolsObjectTestSuite) TestDeleteResourcePool() {
	testResPoolOps := NewResPoolOps(testStore)
	ctx := context.Background()

	err := testResPoolOps.Create(ctx, s.respoolId1, s.resPoolConfig, "peloton")
	s.NoError(err)

	err = testResPoolOps.Delete(ctx, s.respoolId1)
	s.NoError(err)
}

// TestRespoolOpsClientFail tests failure cases due to ORM Client errors.
func (s *ResPoolsObjectTestSuite) TestRespoolOpsClientFail() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	mockClient := ormmocks.NewMockClient(ctrl)
	mockStore := &Store{oClient: mockClient, metrics: testStore.metrics}
	respoolOps := NewResPoolOps(mockStore)

	mockClient.EXPECT().CreateIfNotExists(gomock.Any(), gomock.Any()).
		Return(errors.New("create failed"))
	mockClient.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("update failed")).AnyTimes()
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("get failed"))
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).
		Return(errors.New("delete failed"))

	ctx := context.Background()
	respoolID := &peloton.ResourcePoolID{Value: uuid.New()}

	err := respoolOps.Create(ctx, respoolID, s.resPoolConfig, "test-creator")
	s.Error(err)
	s.Equal("create failed", err.Error())

	err = respoolOps.Update(ctx, respoolID, s.updatedResPoolConfig)
	s.Error(err)
	s.Equal("get failed", err.Error())

	err = respoolOps.Delete(ctx, respoolID)
	s.Error(err)
	s.Equal("delete failed", err.Error())
}

// Tests new respool object can be created.
func (s *ResPoolsObjectTestSuite) TestNewResPoolObjectCreation() {

	respoolID := &peloton.ResourcePoolID{
		Value: uuid.New(),
	}

	testcases := []struct {
		description string
		config      *respool.ResourcePoolConfig
		updateTime  time.Time
	}{
		{
			description: "Simple ResourcePoolConfig: Resources",
			config: &respool.ResourcePoolConfig{
				Name:       "dummy_name",
				OwningTeam: "dummy_OwningTeam",
			},
		},
		{
			description: "ResourceConfig within ResourcePoolConfig is set",
			config: &respool.ResourcePoolConfig{
				Resources: []*respool.ResourceConfig{
					{
						Kind:        "cpu",
						Limit:       1000.0,
						Reservation: 100.0,
						Share:       1.0,
					},
				},
			},
		},
		{
			description: "ResourceConfig has a parent resource pool",
			config: &respool.ResourcePoolConfig{
				Parent: &peloton.ResourcePoolID{
					Value: uuid.New(),
				},
			},
		},
	}

	for _, t := range testcases {
		obj, err := newResPoolObject(
			respoolID,
			t.config,
			time.Now().UTC(),
			time.Now().UTC(),
			"peloton",
		)
		s.NoError(err)
		s.Equal(obj.RespoolID, base.NewOptionalString(respoolID.GetValue()))
	}
}

func (s *ResPoolsObjectTestSuite) buildResPoolConfig() {
	s.respoolId1 = &peloton.ResourcePoolID{Value: uuid.New()}
	s.respoolId2 = &peloton.ResourcePoolID{Value: uuid.New()}

	resources := []*respool.ResourceConfig{
		{
			Kind:        "cpu",
			Limit:       1000.0,
			Reservation: 100.0,
			Share:       1.0,
		},
	}
	updatedResource := []*respool.ResourceConfig{
		{
			Kind:        "gpu",
			Limit:       4.0,
			Reservation: 2.0,
			Share:       1.0,
		},
	}

	s.resPoolConfig = &respool.ResourcePoolConfig{
		Name:        "resource-pool-1",
		OwningTeam:  "peloton",
		LdapGroups:  []string{"compute", "infra"},
		Description: "Simple config for resource pool 1",
		Resources:   resources,
	}

	s.updatedResPoolConfig = &respool.ResourcePoolConfig{
		Name:        "resource-pool-1",
		OwningTeam:  "peloton",
		LdapGroups:  []string{"compute", "infra"},
		Description: "Resource pool is updated.",
		Resources:   updatedResource,
	}
}
