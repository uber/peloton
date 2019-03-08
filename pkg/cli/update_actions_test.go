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

package cli

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	jobmocks "github.com/uber/peloton/.gen/peloton/api/v0/job/mocks"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	respoolmocks "github.com/uber/peloton/.gen/peloton/api/v0/respool/mocks"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v0/update/svc"
	updatesvcmocks "github.com/uber/peloton/.gen/peloton/api/v0/update/svc/mocks"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
	"gopkg.in/yaml.v2"
)

const (
	testJobUpdateConfig = "../../example/testjob.yaml"
)

type updateActionsTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	mockUpdate  *updatesvcmocks.MockUpdateServiceYARPCClient
	mockRespool *respoolmocks.MockResourceManagerYARPCClient
	mockJob     *jobmocks.MockJobManagerYARPCClient
	jobID       *peloton.JobID
	updateID    *peloton.UpdateID
	ctx         context.Context
}

func (suite *updateActionsTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockUpdate = updatesvcmocks.NewMockUpdateServiceYARPCClient(suite.mockCtrl)
	suite.mockRespool = respoolmocks.NewMockResourceManagerYARPCClient(suite.mockCtrl)
	suite.mockJob = jobmocks.NewMockJobManagerYARPCClient(suite.mockCtrl)
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
		jobClient:    suite.mockJob,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	jobConfig := suite.getConfig()
	batchSize := uint32(2)
	version := uint64(3)
	maxInstanceRetries := uint32(3)
	maxFailureInstances := uint32(0)

	respoolPath := "/DefaultResPool"
	respoolID := &peloton.ResourcePoolID{Value: uuid.NewRandom().String()}
	respoolLookUpResponse := &respool.LookupResponse{
		Id: respoolID,
	}
	jobConfig.RespoolID = respoolID
	jobConfig.ChangeLog = &peloton.ChangeLog{
		Version: version,
	}

	jobGetResponse := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				ConfigurationVersion: version,
			},
		},
	}

	jobGetResponseWithUpdate := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				UpdateID: &peloton.UpdateID{
					Value: "abc",
				},
				ConfigurationVersion: version,
			},
		},
	}

	updateGetResponse := &svc.GetUpdateResponse{
		UpdateInfo: &update.UpdateInfo{
			Status: &update.UpdateStatus{
				State: update.State_SUCCEEDED,
			},
		},
	}

	resp := &svc.CreateUpdateResponse{
		UpdateID: suite.updateID,
	}

	tt := []struct {
		debug    bool
		override bool
		err      error
		resp     *svc.GetUpdateResponse
	}{
		{
			err: nil,
		},
		{
			override: true,
			err:      nil,
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

		if t.override == false {
			suite.mockJob.EXPECT().
				Get(gomock.Any(), gomock.Any()).
				Do(func(_ context.Context, req *job.GetRequest) {
					suite.Equal(suite.jobID.GetValue(), req.GetId().GetValue())
				}).
				Return(jobGetResponse, nil)
		} else {
			suite.mockJob.EXPECT().
				Get(gomock.Any(), gomock.Any()).
				Do(func(_ context.Context, req *job.GetRequest) {
					suite.Equal(suite.jobID.GetValue(), req.GetId().GetValue())
				}).
				Return(jobGetResponseWithUpdate, nil)
			suite.mockUpdate.EXPECT().
				GetUpdate(gomock.Any(), gomock.Any()).
				Return(updateGetResponse, nil)
		}

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
			uint64(0),
			t.override,
			maxInstanceRetries,
			maxFailureInstances,
			false,
			false,
			"",
			false,
		)

		if t.err != nil {
			suite.Error(err)
		} else {
			suite.NoError(err)
		}
	}
}

// TestClientUpdateCreate tests failing to create a new update
// due to errors in resource pool lookup
func (suite *updateActionsTestSuite) TestClientUpdateCreateResPoolErrors() {
	c := Client{
		Debug:        false,
		updateClient: suite.mockUpdate,
		resClient:    suite.mockRespool,
		jobClient:    suite.mockJob,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	batchSize := uint32(2)
	maxInstanceRetries := uint32(3)
	maxFailureInstances := uint32(0)

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
			err:                   errors.New("cannot lookup resource pool"),
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
			uint64(0),
			false,
			maxInstanceRetries,
			maxFailureInstances,
			false,
			false,
			"",
			false,
		)
		suite.Error(err)
	}
}

// TestClientUpdateCreateJobGetErrors tests failing to create a new update
// due to errors in job get or existing update
func (suite *updateActionsTestSuite) TestClientUpdateCreateJobGetErrors() {
	c := Client{
		Debug:        false,
		updateClient: suite.mockUpdate,
		resClient:    suite.mockRespool,
		jobClient:    suite.mockJob,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	batchSize := uint32(2)
	maxInstanceRetries := uint32(3)
	maxFailureInstances := uint32(0)
	version := uint64(3)

	respoolPath := "/DefaultResPool"
	respoolID := &peloton.ResourcePoolID{Value: uuid.NewRandom().String()}
	respoolLookUpResponse := &respool.LookupResponse{
		Id: respoolID,
	}

	jobGetResponse := &job.GetResponse{
		JobInfo: &job.JobInfo{},
	}

	jobGetResponseWithUpdate := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				UpdateID: &peloton.UpdateID{
					Value: "abcd",
				},
				ConfigurationVersion: version,
			},
		},
	}

	updateGetResponse := &svc.GetUpdateResponse{
		UpdateInfo: &update.UpdateInfo{
			Status: &update.UpdateStatus{
				State: update.State_ROLLING_FORWARD,
			},
		},
	}

	tt := []struct {
		resp          *job.GetResponse
		configVersion uint64
		err           error
	}{
		{
			resp: jobGetResponseWithUpdate,
			err:  nil,
		},
		{
			resp:          jobGetResponseWithUpdate,
			configVersion: version - 1,
			err:           nil,
		},
		{
			resp: nil,
			err:  errors.New("cannot get job"),
		},
		{
			resp: jobGetResponse,
			err:  nil,
		},
	}

	for _, t := range tt {
		suite.mockRespool.EXPECT().
			LookupResourcePoolID(context.Background(), gomock.Any()).
			Do(func(_ context.Context, req *respool.LookupRequest) {
				suite.Equal(req.GetPath().GetValue(), respoolPath)
			}).
			Return(respoolLookUpResponse, nil)

		suite.mockJob.EXPECT().
			Get(gomock.Any(), gomock.Any()).
			Do(func(_ context.Context, req *job.GetRequest) {
				suite.Equal(suite.jobID.GetValue(), req.GetId().GetValue())
			}).
			Return(t.resp, t.err)

		if t.resp.GetJobInfo().GetRuntime().GetUpdateID() != nil &&
			t.err == nil && t.configVersion == 0 {
			suite.mockUpdate.EXPECT().
				GetUpdate(gomock.Any(), gomock.Any()).
				Return(updateGetResponse, nil)
		}

		err := c.UpdateCreateAction(
			suite.jobID.GetValue(),
			testJobUpdateConfig,
			batchSize,
			respoolPath,
			t.configVersion,
			false,
			maxInstanceRetries,
			maxFailureInstances,
			false,
			false,
			"",
			false,
		)
		suite.Error(err)
	}
}

// TestClientUpdateCreate tests creating a new update
// with retry due to invalid version
func (suite *updateActionsTestSuite) TestClientUpdateCreateRetry() {
	c := Client{
		Debug:        false,
		updateClient: suite.mockUpdate,
		resClient:    suite.mockRespool,
		jobClient:    suite.mockJob,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	jobConfig := suite.getConfig()
	batchSize := uint32(2)
	maxInstanceRetries := uint32(3)
	maxFailureInstances := uint32(0)
	version := uint64(3)

	respoolPath := "/DefaultResPool"
	respoolID := &peloton.ResourcePoolID{Value: uuid.NewRandom().String()}
	respoolLookUpResponse := &respool.LookupResponse{
		Id: respoolID,
	}
	jobConfig.RespoolID = respoolID
	jobConfig.ChangeLog = &peloton.ChangeLog{
		Version: version,
	}

	jobGetResponseBadVersion := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				ConfigurationVersion: version - 1,
			},
		},
	}

	jobGetResponse := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				ConfigurationVersion: version,
			},
		},
	}

	resp := &svc.CreateUpdateResponse{
		UpdateID: suite.updateID,
	}

	gomock.InOrder(
		suite.mockRespool.EXPECT().
			LookupResourcePoolID(context.Background(), gomock.Any()).
			Do(func(_ context.Context, req *respool.LookupRequest) {
				suite.Equal(req.GetPath().GetValue(), respoolPath)
			}).
			Return(respoolLookUpResponse, nil),

		suite.mockJob.EXPECT().
			Get(gomock.Any(), gomock.Any()).
			Do(func(_ context.Context, req *job.GetRequest) {
				suite.Equal(suite.jobID.GetValue(), req.GetId().GetValue())
			}).
			Return(jobGetResponseBadVersion, nil),

		suite.mockUpdate.EXPECT().
			CreateUpdate(context.Background(), gomock.Any()).
			Do(func(_ context.Context, req *svc.CreateUpdateRequest) {
				suite.Equal(suite.jobID.GetValue(), req.JobId.GetValue())
				suite.Equal(batchSize, req.UpdateConfig.BatchSize)
			}).
			Return(nil, yarpcerrors.InvalidArgumentErrorf(
				"invalid job configuration version")),

		suite.mockJob.EXPECT().
			Get(gomock.Any(), gomock.Any()).
			Do(func(_ context.Context, req *job.GetRequest) {
				suite.Equal(suite.jobID.GetValue(), req.GetId().GetValue())
			}).
			Return(jobGetResponse, nil),

		suite.mockUpdate.EXPECT().
			CreateUpdate(context.Background(), gomock.Any()).
			Do(func(_ context.Context, req *svc.CreateUpdateRequest) {
				suite.Equal(suite.jobID.GetValue(), req.JobId.GetValue())
				suite.True(proto.Equal(jobConfig, req.JobConfig))
				suite.Equal(batchSize, req.UpdateConfig.BatchSize)
			}).
			Return(resp, nil),
	)

	err := c.UpdateCreateAction(
		suite.jobID.GetValue(),
		testJobUpdateConfig,
		batchSize,
		respoolPath,
		uint64(0),
		false,
		maxInstanceRetries,
		maxFailureInstances,
		false,
		false,
		"",
		false,
	)
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
			suite.Error(c.UpdateAbortAction(suite.updateID.GetValue(), ""))
		} else {
			suite.NoError(c.UpdateAbortAction(suite.updateID.GetValue(), ""))
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
			suite.Error(c.UpdatePauseAction(suite.updateID.GetValue(), ""))
		} else {
			suite.NoError(c.UpdatePauseAction(suite.updateID.GetValue(), ""))
		}
	}
}

// TestClientUpdateResume tests resuming a job update
func (suite *updateActionsTestSuite) TestClientUpdateResume() {
	c := Client{
		Debug:        false,
		updateClient: suite.mockUpdate,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	resp := &svc.ResumeUpdateResponse{}
	tt := []struct {
		err error
	}{
		{
			err: nil,
		},
		{
			err: errors.New("update cannot be resumed"),
		},
	}

	for _, t := range tt {
		suite.mockUpdate.EXPECT().
			ResumeUpdate(context.Background(), gomock.Any()).
			Do(func(_ context.Context, req *svc.ResumeUpdateRequest) {
				suite.Equal(suite.updateID.GetValue(), req.GetUpdateId().GetValue())
			}).
			Return(resp, t.err)

		if t.err != nil {
			suite.Error(c.UpdateResumeAction(suite.updateID.GetValue(), ""))
		} else {
			suite.NoError(c.UpdateResumeAction(suite.updateID.GetValue(), ""))
		}
	}
}
