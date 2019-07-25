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

	"github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	resource_mocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/metrics"
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/models/v0"
	"github.com/uber/peloton/pkg/placement/testutil"
)

func setupService(t *testing.T) (*service, *resource_mocks.MockResourceManagerServiceYARPCClient, *gomock.Controller) {
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
	return &service{
		config:          config,
		resourceManager: mockResourceManager,
		metrics:         metrics,
	}, mockResourceManager, ctrl
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
	assignments := []models.Task{
		&models_v0.Assignment{
			Offer: &models_v0.HostOffers{
				Offer: &hostsvc.HostOffer{
					Id:       &peloton.HostOfferID{Value: "pelotonid"},
					Hostname: "hostname",
					AgentId:  &mesos_v1.AgentID{Value: &[]string{"agentid"}[0]},
				},
			},
			Task: &models_v0.TaskV0{
				Task: &resmgr.Task{
					Id:     &peloton.TaskID{Value: "taskid"},
					TaskId: &mesos_v1.TaskID{Value: &[]string{"mesostaskid"}[0]},
				},
			},
		},
	}
	placements := service.createPlacements(assignments)
	request := &resmgrsvc.SetPlacementsRequest{
		Placements:       placements,
		FailedPlacements: make([]*resmgrsvc.SetPlacementsRequest_FailedPlacement, 0),
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
	service.SetPlacements(ctx, assignments, nil)

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
	service.SetPlacements(ctx, assignments, nil)

	mockResourceManager.EXPECT().
		SetPlacements(
			gomock.Any(),
			request,
		).
		Return(
			nil,
			errors.New("resource manager set placements request failed"),
		)
	service.SetPlacements(ctx, assignments, nil)

	gomock.InOrder(
		mockResourceManager.EXPECT().
			SetPlacements(
				gomock.Any(),
				&resmgrsvc.SetPlacementsRequest{
					Placements:       placements,
					FailedPlacements: make([]*resmgrsvc.SetPlacementsRequest_FailedPlacement, 0),
				},
			).
			Return(
				&resmgrsvc.SetPlacementsResponse{},
				nil,
			),
	)
	service.SetPlacements(ctx, assignments, nil)
}

// TestCreatePlacement tests that we can turn assignments into resmgr placement objects
// properly.
func TestCreatePlacement(t *testing.T) {
	service, _, ctrl := setupService(t)
	defer ctrl.Finish()

	now := time.Now()
	deadline := now.Add(30 * time.Second)
	host := testutil.SetupHostOffers()
	assignment1 := testutil.SetupAssignment(deadline, 1)
	assignment1.SetPlacement(host)

	assignment2 := testutil.SetupAssignment(deadline, 1)
	assignments := []models.Task{
		assignment1,
		assignment2,
	}

	placements := service.createPlacements(assignments)
	assert.Equal(t, 1, len(placements))
	assert.Equal(t, host.Hostname(), placements[0].GetHostname())
	assert.Equal(t, host.ID(), placements[0].GetHostOfferID().GetValue())
	assert.Equal(t, host.AgentID(), placements[0].GetAgentId().GetValue())
	assert.Equal(t,
		[]*resmgr.Placement_Task{
			{
				PelotonTaskID: assignment1.GetTask().GetTask().GetId(),
				MesosTaskID:   assignment1.GetTask().GetTask().GetTaskId(),
			},
		}, placements[0].GetTaskIDs())
	assert.Equal(t, 3, len(placements[0].GetPorts()))
}
