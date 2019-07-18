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

// +build !unit

package cassandra

import (
	"context"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/storage"
	datastore "github.com/uber/peloton/pkg/storage/cassandra/api"
	datastoremocks "github.com/uber/peloton/pkg/storage/cassandra/api/mocks"
	datastoreimpl "github.com/uber/peloton/pkg/storage/cassandra/impl"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"
	qb "github.com/uber/peloton/pkg/storage/querybuilder"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

const (
	testJob      = "941ff353-ba82-49fe-8f80-fb5bc649b04d"
	testUpdateID = "141ff353-ba82-49fe-8f80-fb5bc649b042"
)

type MockDatastoreTestSuite struct {
	suite.Suite
	testJobID *peloton.JobID

	ctrl            *gomock.Controller
	mockedDataStore *datastoremocks.MockDataStore
	store           *Store
	jobConfigOps    *objectmocks.MockJobConfigOps
	jobRuntimeOps   *objectmocks.MockJobRuntimeOps
}

func (suite *MockDatastoreTestSuite) SetupTest() {
	var result datastore.ResultSet

	suite.testJobID = &peloton.JobID{Value: testJob}

	suite.ctrl = gomock.NewController(suite.T())
	suite.mockedDataStore = datastoremocks.NewMockDataStore(suite.ctrl)
	suite.jobConfigOps = objectmocks.NewMockJobConfigOps(suite.ctrl)
	suite.jobRuntimeOps = objectmocks.NewMockJobRuntimeOps(suite.ctrl)

	suite.store = &Store{
		DataStore:     suite.mockedDataStore,
		jobConfigOps:  suite.jobConfigOps,
		jobRuntimeOps: suite.jobRuntimeOps,
		metrics:       storage.NewMetrics(testScope.SubScope("storage")),
		Conf:          &Config{},
		retryPolicy:   nil,
	}

	queryBuilder := &datastoreimpl.QueryBuilder{}
	// Mock datastore execute to fail
	suite.mockedDataStore.EXPECT().Execute(gomock.Any(), gomock.Any()).
		Return(result, errors.New("my-error")).AnyTimes()
	suite.mockedDataStore.EXPECT().NewQuery().Return(queryBuilder).AnyTimes()
}

func TestMockDatastoreTestSuite(t *testing.T) {
	suite.Run(t, new(MockDatastoreTestSuite))
}

// TestDataStoreDeleteJob test delete job
func (suite *MockDatastoreTestSuite) TestDataStoreDeleteJob() {
	ctx, cancelFunc := context.WithTimeout(
		context.Background(),
		time.Second)
	defer cancelFunc()
	jobID := &peloton.JobID{
		Value: uuid.New(),
	}

	// Failure test for get job_config
	suite.jobConfigOps.EXPECT().GetCurrentVersion(ctx, gomock.Any()).
		Return(nil, nil, errors.New("my-error"))
	suite.Error(suite.store.DeleteJob(ctx, jobID.GetValue()))
}

// TestGetFrameworkID tests the fetch for framework ID
func (suite *MockDatastoreTestSuite) TestGetFrameworkID() {
	_, err := suite.store.GetFrameworkID(context.Background(), common.PelotonRole)
	suite.Error(err)
}

// TestGetStreamID test the fetch for stream ID
func (suite *MockDatastoreTestSuite) TestGetStreamID() {
	_, err := suite.store.GetMesosStreamID(context.Background(), common.PelotonRole)
	suite.Error(err)
}

// TestDataStoreFailureGetJobSummary tests datastore failures in getting
// job summary
func (suite *MockDatastoreTestSuite) TestDataStoreFailureGetJobSummary() {
	_, err := suite.store.getJobSummaryFromIndex(
		context.Background(), suite.testJobID)
	suite.Error(err)

	result := make(map[string]interface{})
	jobID, err := qb.ParseUUID(suite.testJobID.GetValue())
	suite.NoError(err)
	result["job_id"] = jobID

	// if name is set to "", we will try to get the summary by querying the
	// job_config table. In case there is a Data Store error in this query, it
	// should not prevent the rest of the jobs to be returned. So it should only
	// log the error and move on to the next entry.
	result["name"] = ""
	results := []map[string]interface{}{result}
	_, err = suite.store.getJobSummaryFromResultMap(
		context.Background(), results)
	suite.NoError(err)
}

// TestDataStoreFailureGetJob tests datastore failures in getting job
func (suite *MockDatastoreTestSuite) TestDataStoreFailureGetJob() {
	_, err := suite.store.GetMaxJobConfigVersion(
		context.Background(), suite.testJobID.GetValue())
	suite.Error(err)
}

// TestDataStoreFailureGetTasks tests datastore failures in getting tasks
func (suite *MockDatastoreTestSuite) TestDataStoreFailureGetTasks() {
	_, err := suite.store.GetTasksForJobAndStates(
		context.Background(), suite.testJobID, []task.TaskState{
			task.TaskState(task.TaskState_PENDING)})
	suite.Error(err)

	_, err = suite.store.GetTasksForJobResultSet(
		context.Background(), suite.testJobID)
	suite.Error(err)

	_, err = suite.store.GetTasksForJob(
		context.Background(), suite.testJobID)
	suite.Error(err)

	_, err = suite.store.GetTaskForJob(
		context.Background(), suite.testJobID.GetValue(), 0)
	suite.Error(err)

	_, err = suite.store.getTask(context.Background(), testJob, 0)
	suite.Error(err)
}

// TestDataStoreFailureGetTaskConfig tests datastore failures in getting task cfg
func (suite *MockDatastoreTestSuite) TestDataStoreFailureGetTaskConfig() {
	_, _, err := suite.store.GetTaskConfigs(
		context.Background(), suite.testJobID, []uint32{0}, 0)
	suite.Error(err)
}

// TestDataStoreFailureGetTaskRuntime tests datastore failures in getting
// task runtime
func (suite *MockDatastoreTestSuite) TestDataStoreFailureGetTaskRuntime() {
	_, err := suite.store.GetTaskRuntimesForJobByRange(
		context.Background(), suite.testJobID, &task.InstanceRange{
			From: uint32(0),
			To:   uint32(3),
		})
	suite.Error(err)

	_, err = suite.store.GetTaskRuntime(
		context.Background(), suite.testJobID, 0)
	suite.Error(err)

	_, err = suite.store.getTaskRuntimeRecord(context.Background(), testJob, 0)
	suite.Error(err)
}

// TestDataStoreFailureJobQuery tests datastore failures in job query
func (suite *MockDatastoreTestSuite) TestDataStoreFailureJobQuery() {
	_, _, _, err := suite.store.QueryJobs(
		context.Background(), nil, &job.QuerySpec{}, false)
	suite.Error(err)
}

// TestDataStoreFailureTaskQuery tests datastore failures in task query
func (suite *MockDatastoreTestSuite) TestDataStoreFailureTaskQuery() {
	_, _, err := suite.store.QueryTasks(
		context.Background(), suite.testJobID, &task.QuerySpec{})
	suite.Error(err)
}

// TestDataStoreFailureFramework tests datastore failures in get frameworks
func (suite *MockDatastoreTestSuite) TestDataStoreFailureFramework() {
	_, err := suite.store.getFrameworkInfo(context.Background(), "framwork-id")
	suite.Error(err)
}

// TestDataStoreFailureGetPersistentVolume tests datastore failures in get
// persistent volume
func (suite *MockDatastoreTestSuite) TestDataStoreFailureGetPersistentVolume() {
	_, err := suite.store.GetPersistentVolume(
		context.Background(), &peloton.VolumeID{Value: "test"})
	suite.Error(err)
}

// TestDataStoreFailureGetUpdate tests datastore failures in get update
func (suite *MockDatastoreTestSuite) TestDataStoreFailureGetUpdate() {
	_, err := suite.store.GetUpdate(
		context.Background(), &peloton.UpdateID{Value: "test"})
	suite.Error(err)

	_, err = suite.store.GetUpdateProgress(
		context.Background(), &peloton.UpdateID{Value: "test"})
	suite.Error(err)

	_, err = suite.store.GetUpdatesForJob(
		context.Background(),
		suite.testJobID.GetValue())
	suite.Error(err)
}

// TestDataStoreFailureDeleteJobCfgVersion tests datastore failures in delete
// job config version
func (suite *MockDatastoreTestSuite) TestDataStoreFailureDeleteJobCfgVersion() {
	ctx := context.Background()
	var result datastore.ResultSet

	// Setup mocks for this context

	// Simulate failure to delete task config
	suite.mockedDataStore.EXPECT().Execute(ctx, gomock.Any()).
		Return(result, errors.New("my-error"))

	err := suite.store.deleteJobConfigVersion(ctx, suite.testJobID, 0)
	suite.Error(err)

	// Simulate success to to delete task cfg and failure to delete job cfg
	suite.mockedDataStore.EXPECT().Execute(ctx, gomock.Any()).
		Return(result, nil)
	suite.mockedDataStore.EXPECT().Execute(ctx, gomock.Any()).
		Return(result, errors.New("my-error"))

	err = suite.store.deleteJobConfigVersion(ctx, suite.testJobID, 0)
	suite.Error(err)
}

// TestWorkflowEventsFailures tests failure scenarios for workflow events
func (suite *MockDatastoreTestSuite) TestWorkflowEventsFailures() {
	updateID := &peloton.UpdateID{
		Value: testUpdateID,
	}

	err := suite.store.AddWorkflowEvent(
		context.Background(),
		updateID,
		0,
		models.WorkflowType_UPDATE,
		update.State_ROLLING_FORWARD)
	suite.Error(err)

	err = suite.store.deleteWorkflowEvents(context.Background(), updateID, 0)
	suite.Error(err)

	_, err = suite.store.GetWorkflowEvents(context.Background(), updateID, 0, 0)
	suite.Error(err)
}

// TestDataStoreFailureGetTaskConfigs tests datastore failures in get task
// config from legacy/v2 tables
func (suite *MockDatastoreTestSuite) TestDataStoreFailureGetTaskConfigs() {
	ctx := context.Background()
	var result datastore.ResultSet

	suite.mockedDataStore.EXPECT().Execute(ctx, gomock.Any()).
		Return(result, nil)
	suite.mockedDataStore.EXPECT().Execute(ctx, gomock.Any()).
		Return(result, errors.New("my-error"))
	_, _, err := suite.store.GetTaskConfigs(ctx, suite.testJobID, []uint32{0}, 0)
	suite.Error(err)
}
