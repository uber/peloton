package cli

import (
	"context"
	"io/ioutil"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update/svc"
	updatesvcmocks "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update/svc/mocks"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v2"
)

const (
	testJobUpdateConfig = "../example/testjob.yaml"
)

type updateActionsTestSuite struct {
	suite.Suite
	mockCtrl   *gomock.Controller
	mockUpdate *updatesvcmocks.MockUpdateServiceYARPCClient
	jobID      *peloton.JobID
	updateID   *peloton.UpdateID
	ctx        context.Context
}

func (suite *updateActionsTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockUpdate = updatesvcmocks.NewMockUpdateServiceYARPCClient(suite.mockCtrl)
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

// TestClientUpdateCreate tests successfully creating a new update
func (suite *updateActionsTestSuite) TestClientUpdateCreate() {
	c := Client{
		Debug:        false,
		updateClient: suite.mockUpdate,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	jobConfig := suite.getConfig()
	batchSize := uint32(2)

	resp := &svc.CreateUpdateResponse{
		UpdateID: suite.updateID,
	}

	suite.mockUpdate.EXPECT().
		CreateUpdate(context.Background(), gomock.Any()).
		Do(func(_ context.Context, req *svc.CreateUpdateRequest) {
			suite.Equal(suite.jobID.GetValue(), req.JobId.GetValue())
			suite.True(proto.Equal(jobConfig, req.JobConfig))
			suite.Equal(batchSize, req.UpdateConfig.BatchSize)
		}).
		Return(resp, nil)

	err := c.UpdateCreateAction(suite.jobID.GetValue(), testJobUpdateConfig, batchSize)
	suite.NoError(err)
}

// TestClientUpdateGet tests fetching status of a given update
func (suite *updateActionsTestSuite) TestClientUpdateGet() {
	c := Client{
		Debug:        false,
		updateClient: suite.mockUpdate,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	resp := &svc.GetUpdateResponse{
		UpdateInfo: &update.UpdateInfo{
			UpdateId: suite.updateID,
			JobId:    suite.jobID,
			Status: &update.UpdateStatus{
				State: update.State_ROLLING_FORWARD,
			},
		},
	}

	suite.mockUpdate.EXPECT().
		GetUpdate(context.Background(), gomock.Any()).
		Do(func(_ context.Context, req *svc.GetUpdateRequest) {
			suite.Equal(suite.updateID.GetValue(), req.GetUpdateId().GetValue())
		}).
		Return(resp, nil)

	err := c.UpdateGetAction(suite.updateID.GetValue())
	suite.NoError(err)
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

	resp := &svc.ListUpdatesResponse{
		UpdateInfo: updateList,
	}

	suite.mockUpdate.EXPECT().
		ListUpdates(context.Background(), gomock.Any()).
		Do(func(_ context.Context, req *svc.ListUpdatesRequest) {
			suite.Equal(suite.jobID.GetValue(), req.GetJobID().GetValue())
		}).
		Return(resp, nil)

	err := c.UpdateListAction(suite.jobID.GetValue())
	suite.NoError(err)
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

	suite.mockUpdate.EXPECT().
		GetUpdateCache(context.Background(), gomock.Any()).
		Do(func(_ context.Context, req *svc.GetUpdateCacheRequest) {
			suite.Equal(suite.updateID.GetValue(), req.GetUpdateId().GetValue())
		}).
		Return(resp, nil)

	err := c.UpdateGetCacheAction(suite.updateID.GetValue())
	suite.NoError(err)
}
