package updatesvc

import (
	"context"
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/update/svc"

	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/yarpc/yarpcerrors"
)

func TestCreate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStoreMock := store_mocks.NewMockJobStore(ctrl)
	updateStoreMock := store_mocks.NewMockUpdateStore(ctrl)

	h := &serviceHandler{
		jobStore:    jobStoreMock,
		updateStore: updateStoreMock,
	}

	jobID := &peloton.JobID{
		Value: "123e4567-e89b-12d3-a456-426655440000",
	}

	// Return error if job was not found.
	jobStoreMock.EXPECT().GetJobRuntime(context.Background(), jobID).
		Return(nil, fmt.Errorf("job not found"))

	_, err := h.CreateUpdate(context.Background(), &svc.CreateUpdateRequest{
		JobId: jobID,
	})
	assert.True(t, yarpcerrors.IsNotFound(err))

	// Return error if job is not running.
	jobStoreMock.EXPECT().GetJobRuntime(context.Background(), jobID).
		Return(&job.RuntimeInfo{
			State: job.JobState_SUCCEEDED,
		}, nil)

	_, err = h.CreateUpdate(context.Background(), &svc.CreateUpdateRequest{
		JobId: jobID,
	})
	assert.True(t, yarpcerrors.IsInvalidArgument(err))

	// CreateUpdate is called if job is valid.
	jobStoreMock.EXPECT().GetJobRuntime(context.Background(), jobID).
		Return(&job.RuntimeInfo{
			State: job.JobState_RUNNING,
		}, nil)
	updateStoreMock.EXPECT().CreateUpdate(context.Background(), gomock.Any(), jobID, gomock.Any(), gomock.Any()).
		Return(nil)

	_, err = h.CreateUpdate(context.Background(), &svc.CreateUpdateRequest{
		JobId: jobID,
	})
	assert.NoError(t, err)
}
