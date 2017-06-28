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
		Return(nil, fmt.Errorf("job not found"))

	res, err := h.Create(context.Background(), &upgrade.CreateRequest{
		Spec: &upgrade.UpgradeSpec{
			JobId: jobID,
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
	jobStoreMock.EXPECT().GetJobRuntime(context.Background(), jobID).
		Return(&job.RuntimeInfo{
			State: job.JobState_SUCCEEDED,
		}, nil)

	res, err = h.Create(context.Background(), &upgrade.CreateRequest{
		Spec: &upgrade.UpgradeSpec{
			JobId: jobID,
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

	// CreateUpgrade is called if job is valid.
	jobStoreMock.EXPECT().GetJobRuntime(context.Background(), jobID).
		Return(&job.RuntimeInfo{
			State: job.JobState_RUNNING,
		}, nil)
	id := &upgrade.WorkflowID{Value: "8e3e40d2-5149-53f3-8eb6-e7ae5ad1938c"}
	upgradeStoreMock.EXPECT().CreateUpgrade(context.Background(), id, &upgrade.UpgradeSpec{JobId: jobID}).
		Return(nil)

	res, err = h.Create(context.Background(), &upgrade.CreateRequest{
		Spec: &upgrade.UpgradeSpec{
			JobId: jobID,
		},
	})
	assert.Equal(t, &upgrade.CreateResponse{
		Response: &upgrade.CreateResponse_Result{
			Result: id,
		},
	}, res)
	assert.NoError(t, err)
}
