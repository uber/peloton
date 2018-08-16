package cli

import (
	"context"
	"io/ioutil"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	respoolmocks "code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update/svc"
	updatesvcmocks "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update/svc/mocks"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v2"
)

const (
	testJobUpdateConfig = "../example/testjob.yaml"
)

type updateActionsTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	mockUpdate  *updatesvcmocks.MockUpdateServiceYARPCClient
	mockRespool *respoolmocks.MockResourceManagerYARPCClient
	jobID       *peloton.JobID
	updateID    *peloton.UpdateID
	ctx         context.Context
}

func (suite *updateActionsTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockUpdate = updatesvcmocks.NewMockUpdateServiceYARPCClient(suite.mockCtrl)
	suite.mockRespool = respoolmocks.NewMockResourceManagerYARPCClient(suite.mockCtrl)
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.ctx = context.Background()
}

func (suite *updateActionsTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func TestUpdateCLIActions(t *testing.T) {
	suite.Run(t, new(updateActionsTestSuite))
}

// getConfig creates the job configuration from a file
func (suite *updateActionsTestSuite) getConfig() *job.JobConfig {
	var jobConfig job.JobConfig
	buffer, err := ioutil.ReadFile(testJobUpdateConfig)
	suite.NoError(err)
	err = yaml.Unmarshal(buffer, &jobConfig)
	suite.NoError(err)
	return &jobConfig
}

// TestClientUpdateCreate tests creating a new update
func (suite *updateActionsTestSuite) TestClientUpdateCreate() {
	c := Client{
		Debug:        false,
		updateClient: suite.mockUpdate,
		resClient:    suite.mockRespool,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	jobConfig := suite.getConfig()
	batchSize := uint32(2)

	respoolPath := "/DefaultResPool"
	respoolID := &peloton.ResourcePoolID{Value: uuid.NewRandom().String()}
	respoolLookUpResponse := &respool.LookupResponse{
		Id: respoolID,
	}
	jobConfig.RespoolID = respoolID

	resp := &svc.CreateUpdateResponse{
		UpdateID: suite.updateID,
	}

	tt := []struct {
		debug bool
		err   error
	}{
		{
			err: nil,
		},
		{
			debug: true,
			err:   nil,
		},
		{
			err: errors.New("cannot create update"),
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		suite.mockRespool.EXPECT().
			LookupResourcePoolID(context.Background(), gomock.Any()).
			Do(func(_ context.Context, req *respool.LookupRequest) {
				suite.Equal(req.GetPath().GetValue(), respoolPath)
			}).
			Return(respoolLookUpResponse, nil)

		suite.mockUpdate.EXPECT().
			CreateUpdate(context.Background(), gomock.Any()).
			Do(func(_ context.Context, req *svc.CreateUpdateRequest) {
				suite.Equal(suite.jobID.GetValue(), req.JobId.GetValue())
				suite.True(proto.Equal(jobConfig, req.JobConfig))
				suite.Equal(batchSize, req.UpdateConfig.BatchSize)
			}).
			Return(resp, t.err)

		err := c.UpdateCreateAction(
			suite.jobID.GetValue(),
			testJobUpdateConfig,
			batchSize,
			respoolPath,
		)

		if t.err != nil {
			suite.Error(err)
		} else {
			suite.NoError(err)
		}
	}
}

// TestClientUpdateCreate tests successfully creating a new update
// with errors in resource pool lookup
func (suite *updateActionsTestSuite) TestClientUpdateCreateResPoolErrors() {
	c := Client{
		Debug:        false,
		updateClient: suite.mockUpdate,
		resClient:    suite.mockRespool,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	batchSize := uint32(2)

	respoolPath := "/DefaultResPool"

	tt := []struct {
		respoolLookUpResponse *respool.LookupResponse
		err                   error
	}{
		{
			respoolLookUpResponse: &respool.LookupResponse{
				Id: nil,
			},
			err: nil,
		},
		{
			respoolLookUpResponse: nil,
			err: errors.New("cannot lookup resource pool"),
		},
	}

	for _, t := range tt {
		suite.mockRespool.EXPECT().
			LookupResourcePoolID(context.Background(), gomock.Any()).
			Do(func(_ context.Context, req *respool.LookupRequest) {
				suite.Equal(req.GetPath().GetValue(), respoolPath)
			}).
			Return(t.respoolLookUpResponse, t.err)

		err := c.UpdateCreateAction(
			suite.jobID.GetValue(),
			testJobUpdateConfig,
			batchSize,
			respoolPath,
		)
		suite.Error(err)
	}
}

// TestClientUpdateGet tests fetching status of a given update
func (suite *updateActionsTestSuite) TestClientUpdateGet() {
	c := Client{
		Debug:        false,
		updateClient: suite.mockUpdate,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	tt := []struct {
		debug bool
		resp  *svc.GetUpdateResponse
		err   error
	}{
		{
			resp: &svc.GetUpdateResponse{
				UpdateInfo: &update.UpdateInfo{
					UpdateId: suite.updateID,
					JobId:    suite.jobID,
					Status: &update.UpdateStatus{
						State: update.State_ROLLING_FORWARD,
					},
				},
			},
			err: nil,
		},
		{
			debug: true,
			resp: &svc.GetUpdateResponse{
				UpdateInfo: &update.UpdateInfo{
					UpdateId: suite.updateID,
					JobId:    suite.jobID,
					Status: &update.UpdateStatus{
						State: update.State_ROLLING_FORWARD,
					},
				},
			},
			err: nil,
		},
		{
			resp: nil,
			err:  errors.New("did not find the update"),
		},
		{
			resp: &svc.GetUpdateResponse{
				UpdateInfo: nil,
			},
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		suite.mockUpdate.EXPECT().
			GetUpdate(context.Background(), gomock.Any()).
			Do(func(_ context.Context, req *svc.GetUpdateRequest) {
				suite.Equal(suite.updateID.GetValue(), req.GetUpdateId().GetValue())
			}).
			Return(t.resp, t.err)

		if t.err != nil {
			suite.Error(c.UpdateGetAction(suite.updateID.GetValue()))
		} else {
			suite.NoError(c.UpdateGetAction(suite.updateID.GetValue()))
		}
	}
}

// TestClientUpdateList tests fetching update information for
// all updates for a given job
func (suite *updateActionsTestSuite) TestClientUpdateList() {
	c := Client{
		Debug:        false,
		updateClient: suite.mockUpdate,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	updateList := []*update.UpdateInfo{}
	updateInfo := &update.UpdateInfo{
		UpdateId: suite.updateID,
		JobId:    suite.jobID,
		Status: &update.UpdateStatus{
			State: update.State_ROLLING_FORWARD,
		},
	}
	updateList = append(updateList, updateInfo)

	tt := []struct {
		debug bool
		resp  *svc.ListUpdatesResponse
		err   error
	}{
		{
			resp: &svc.ListUpdatesResponse{
				UpdateInfo: updateList,
			},
			err: nil,
		},
		{
			debug: true,
			resp: &svc.ListUpdatesResponse{
				UpdateInfo: updateList,
			},
			err: nil,
		},
		{
			resp: nil,
			err:  errors.New("failed to fetch updates"),
		},
		{
			resp: &svc.ListUpdatesResponse{
				UpdateInfo: []*update.UpdateInfo{},
			},
			err: nil,
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		suite.mockUpdate.EXPECT().
			ListUpdates(context.Background(), gomock.Any()).
			Do(func(_ context.Context, req *svc.ListUpdatesRequest) {
				suite.Equal(suite.jobID.GetValue(), req.GetJobID().GetValue())
			}).
			Return(t.resp, t.err)

		if t.err != nil {
			suite.Error(c.UpdateListAction(suite.jobID.GetValue()))
		} else {
			suite.NoError(c.UpdateListAction(suite.jobID.GetValue()))
		}
	}
}

// TestClientUpdateGetCache tests fetching the update information in the cache
func (suite *updateActionsTestSuite) TestClientUpdateGetCache() {
	c := Client{
		Debug:        false,
		updateClient: suite.mockUpdate,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	resp := &svc.GetUpdateCacheResponse{
		JobId: suite.jobID,
	}

	tt := []struct {
		err error
	}{
		{
			err: nil,
		},
		{
			err: errors.New("did not find update in cache"),
		},
	}

	for _, t := range tt {
		suite.mockUpdate.EXPECT().
			GetUpdateCache(context.Background(), gomock.Any()).
			Do(func(_ context.Context, req *svc.GetUpdateCacheRequest) {
				suite.Equal(suite.updateID.GetValue(), req.GetUpdateId().GetValue())
			}).
			Return(resp, t.err)

		if t.err != nil {
			suite.Error(c.UpdateGetCacheAction(suite.updateID.GetValue()))
		} else {
			suite.NoError(c.UpdateGetCacheAction(suite.updateID.GetValue()))
		}
	}
}

// TestClientUpdateAbort tests aborting a job update
func (suite *updateActionsTestSuite) TestClientUpdateAbort() {
	c := Client{
		Debug:        false,
		updateClient: suite.mockUpdate,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	resp := &svc.AbortUpdateResponse{}
	tt := []struct {
		err error
	}{
		{
			err: nil,
		},
		{
			err: errors.New("update in terminal state"),
		},
	}

	for _, t := range tt {
		suite.mockUpdate.EXPECT().
			AbortUpdate(context.Background(), gomock.Any()).
			Do(func(_ context.Context, req *svc.AbortUpdateRequest) {
				suite.Equal(suite.updateID.GetValue(), req.GetUpdateId().GetValue())
			}).
			Return(resp, t.err)

		if t.err != nil {
			suite.Error(c.UpdateAbortAction(suite.updateID.GetValue()))
		} else {
			suite.NoError(c.UpdateAbortAction(suite.updateID.GetValue()))
		}
	}
}

// TestClientUpdatePause tests pausing a job update
func (suite *updateActionsTestSuite) TestClientUpdatePause() {
	c := Client{
		Debug:        false,
		updateClient: suite.mockUpdate,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	resp := &svc.PauseUpdateResponse{}
	tt := []struct {
		err error
	}{
		{
			err: nil,
		},
		{
			err: errors.New("update cannot be paused"),
		},
	}

	for _, t := range tt {
		suite.mockUpdate.EXPECT().
			PauseUpdate(context.Background(), gomock.Any()).
			Do(func(_ context.Context, req *svc.PauseUpdateRequest) {
				suite.Equal(suite.updateID.GetValue(), req.GetUpdateId().GetValue())
			}).
			Return(resp, t.err)

		if t.err != nil {
			suite.Error(c.UpdatePauseAction(suite.updateID.GetValue()))
		} else {
			suite.NoError(c.UpdatePauseAction(suite.updateID.GetValue()))
		}
	}
}
