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

package tasks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	resource_mocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"
	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/metrics"
)

func setupService(t *testing.T) (Service, *resource_mocks.MockResourceManagerServiceYARPCClient, *gomock.Controller) {
	ctrl := gomock.NewController(t)
	mockResourceManager := resource_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	metrics := metrics.NewMetrics(tally.NoopScope)
	config := &config.PlacementConfig{
		MaxRounds: config.MaxRoundsConfig{
			Unknown:   1,
			Batch:     1,
			Stateless: 5,
			Daemon:    5,
			Stateful:  0,
		},
		MaxDurations: config.MaxDurationsConfig{
			Unknown:   5 * time.Second,
			Batch:     5 * time.Second,
			Stateless: 10 * time.Second,
			Daemon:    15 * time.Second,
			Stateful:  25 * time.Second,
		},
	}
	return NewService(mockResourceManager, config, metrics), mockResourceManager, ctrl
}

func TestTaskService_Dequeue(t *testing.T) {
	service, mockResourceManager, ctrl := setupService(t)
	defer ctrl.Finish()
	ctx := context.Background()

	request := &resmgrsvc.DequeueGangsRequest{
		Limit:   10,
		Type:    resmgr.TaskType_UNKNOWN,
		Timeout: 100,
	}

	// Placement engine dequeue, resource manager dequeue gangs api failure
	response := &resmgrsvc.DequeueGangsResponse{
		Error: &resmgrsvc.DequeueGangsResponse_Error{},
	}
	mockResourceManager.EXPECT().
		DequeueGangs(
			gomock.Any(),
			request,
		).Return(
		response,
		nil,
	)
	assignments := service.Dequeue(ctx, resmgr.TaskType_UNKNOWN, 10, 100)
	assert.Nil(t, assignments)

	mockResourceManager.EXPECT().
		DequeueGangs(
			gomock.Any(),
			request,
		).Return(
		nil,
		errors.New("dequeue gangs request failed"),
	)
	assignments = service.Dequeue(ctx, resmgr.TaskType_UNKNOWN, 10, 100)
	assert.Nil(t, assignments)

	// Placement engine dequeue gangs with nil task
	gomock.InOrder(
		mockResourceManager.EXPECT().
			DequeueGangs(
				gomock.Any(),
				request,
			).Return(
			&resmgrsvc.DequeueGangsResponse{
				Gangs: []*resmgrsvc.Gang{
					{
						Tasks: nil,
					},
				},
			},
			nil,
		),
	)

	assignments = service.Dequeue(ctx, resmgr.TaskType_UNKNOWN, 10, 100)
	assert.Nil(t, assignments)

	// Placement engine dequeue success call
	gomock.InOrder(
		mockResourceManager.EXPECT().
			DequeueGangs(
				gomock.Any(),
				request,
			).Return(
			&resmgrsvc.DequeueGangsResponse{
				Gangs: []*resmgrsvc.Gang{
					{
						Tasks: []*resmgr.Task{
							{
								Name: "task",
							},
						},
					},
				},
			},
			nil,
		),
	)

	assignments = service.Dequeue(ctx, resmgr.TaskType_UNKNOWN, 10, 100)
	assert.NotNil(t, assignments)
	assert.Equal(t, 1, len(assignments))
}

func TestTaskService_SetPlacements(t *testing.T) {
	service, mockResourceManager, ctrl := setupService(t)
	defer ctrl.Finish()

	ctx := context.Background()
	placements := []*resmgr.Placement{
		{
			Hostname: "hostname",
			Tasks: []*peloton.TaskID{
				{
					Value: "taskid",
				},
			},
		},
	}
	request := &resmgrsvc.SetPlacementsRequest{
		Placements: placements,
	}

	// Placement engine with empty placements
	service.SetPlacements(ctx, nil, nil)

	// Placement engine, resource manager set placements request failed
	mockResourceManager.EXPECT().
		SetPlacements(
			gomock.Any(),
			request,
		).
		Return(
			&resmgrsvc.SetPlacementsResponse{
				Error: &resmgrsvc.SetPlacementsResponse_Error{},
			},
			nil,
		)
	service.SetPlacements(ctx, placements, nil)

	mockResourceManager.EXPECT().
		SetPlacements(
			gomock.Any(),
			request,
		).
		Return(
			&resmgrsvc.SetPlacementsResponse{
				Error: &resmgrsvc.SetPlacementsResponse_Error{
					Failure: &resmgrsvc.SetPlacementsFailure{},
				},
			},
			nil,
		)
	service.SetPlacements(ctx, placements, nil)

	mockResourceManager.EXPECT().
		SetPlacements(
			gomock.Any(),
			request,
		).
		Return(
			nil,
			errors.New("resource manager set placements request failed"),
		)
	service.SetPlacements(ctx, placements, nil)

	gomock.InOrder(
		mockResourceManager.EXPECT().
			SetPlacements(
				gomock.Any(),
				&resmgrsvc.SetPlacementsRequest{
					Placements: placements,
				},
			).
			Return(
				&resmgrsvc.SetPlacementsResponse{},
				nil,
			),
	)
	service.SetPlacements(ctx, placements, nil)
}
