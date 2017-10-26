package upgrade

import (
	"context"
	"testing"

	"go.uber.org/yarpc/yarpcerrors"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/upgrade"
	"code.uber.internal/infra/peloton/.gen/peloton/api/upgrade/svc"

	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestCreate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStoreMock := store_mocks.NewMockJobStore(ctrl)
	upgradeStoreMock := store_mocks.NewMockUpgradeStore(ctrl)

	h := &serviceHandler{
		jobStore:     jobStoreMock,
		upgradeStore: upgradeStoreMock,
	}

	jobID := &peloton.JobID{
		Value: "123e4567-e89b-12d3-a456-426655440000",
	}

	// Return error if job was not found.
	jobStoreMock.EXPECT().GetJobRuntime(context.Background(), jobID).
		Return(nil, yarpcerrors.NotFoundErrorf("job not found"))

	res, err := h.Create(context.Background(), &svc.CreateRequest{
		JobId:   jobID,
		Options: &upgrade.Options{},
	})
	assert.Nil(t, res)
	assert.True(t, yarpcerrors.IsNotFound(err))

	// Return error if job is not running.
	jobStoreMock.EXPECT().GetJobRuntime(context.Background(), jobID).
		Return(&job.RuntimeInfo{
			State: job.JobState_SUCCEEDED,
		}, nil)

	res, err = h.Create(context.Background(), &svc.CreateRequest{
		JobId: jobID,
	})
	assert.Nil(t, res)
	assert.True(t, yarpcerrors.IsInvalidArgument(err))

	// CreateUpgrade is called if job is valid.
	jobStoreMock.EXPECT().GetJobRuntime(context.Background(), jobID).
		Return(&job.RuntimeInfo{
			State: job.JobState_RUNNING,
		}, nil)

	id := &peloton.UpgradeID{Value: "8e3e40d2-5149-53f3-8eb6-e7ae5ad1938c"}

	upgradeStoreMock.EXPECT().CreateUpgrade(context.Background(), id, &upgrade.Status{
		Id:    id,
		JobId: jobID,
		State: upgrade.State_ROLLING_FORWARD,
	}, nil, uint64(0), uint64(0)).Return(nil)

	res, err = h.Create(context.Background(), &svc.CreateRequest{
		JobId: jobID,
	})
	assert.Equal(t, &svc.CreateResponse{Id: id}, res)
	assert.NoError(t, err)
}
