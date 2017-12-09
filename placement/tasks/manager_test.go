package tasks

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	resource_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestTaskManager_Dequeue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockResourceManager := resource_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	manager := NewManager(mockResourceManager)

	gomock.InOrder(
		mockResourceManager.EXPECT().
			DequeueGangs(
				gomock.Any(),
				&resmgrsvc.DequeueGangsRequest{
					Limit:   10,
					Type:    resmgr.TaskType_UNKNOWN,
					Timeout: 100,
				},
			).Return(
			&resmgrsvc.DequeueGangsResponse{
				Gangs: []*resmgrsvc.Gang{
					{
						Tasks: []*resmgr.Task{},
					},
				},
			},
			nil,
		),
	)
	ctx := context.Background()
	gangs, err := manager.Dequeue(ctx, resmgr.TaskType_UNKNOWN, 10, 100)

	assert.NoError(t, err)
	assert.NoError(t, err)
	assert.NotNil(t, gangs)
	assert.Equal(t, 1, len(gangs))
}

func TestTaskManager_SetPlacements(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockResourceManager := resource_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	manager := NewManager(mockResourceManager)

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

	err := manager.SetPlacements(ctx, placements)
	assert.NoError(t, err)
}
