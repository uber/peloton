package upgrade

import (
	"context"
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/errors"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/upgrade"

	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestCreate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStoreMock := store_mocks.NewMockJobStore(ctrl)

	h := &serviceHandler{
		jobStore: jobStoreMock,
	}

	id := &peloton.JobID{
		Value: "job-id",
	}

	// Return error if job was not found.
	jobStoreMock.EXPECT().GetJobRuntime(context.Background(), id).
		Return(nil, fmt.Errorf("job not found"))

	res, err := h.Create(context.Background(), &upgrade.CreateRequest{
		Spec: &upgrade.UpgradeSpec{
			JobId: id,
		},
	})
	assert.Equal(t, &upgrade.CreateResponse{
		Response: &upgrade.CreateResponse_NotFound{
			NotFound: &errors.JobNotFound{
				Message: "job not found",
			},
		},
	}, res)
	assert.NoError(t, err)

	// Return error if job is not running.
	jobStoreMock.EXPECT().GetJobRuntime(context.Background(), id).
		Return(&job.RuntimeInfo{
			State: job.JobState_SUCCEEDED,
		}, nil)

	res, err = h.Create(context.Background(), &upgrade.CreateRequest{
		Spec: &upgrade.UpgradeSpec{
			JobId: id,
		},
	})
	assert.Equal(t, &upgrade.CreateResponse{
		Response: &upgrade.CreateResponse_NotFound{
			NotFound: &errors.JobNotFound{
				Message: "cannot upgrade terminated job",
			},
		},
	}, res)
	assert.NoError(t, err)
}
