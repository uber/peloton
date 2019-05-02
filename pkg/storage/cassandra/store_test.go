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
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/query"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v0/volume"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/backoff"
	"github.com/uber/peloton/pkg/common/taskconfig"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/storage"
	qb "github.com/uber/peloton/pkg/storage/querybuilder"

	"github.com/gocql/gocql"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	_resPoolOwner = "teamPeloton"
)

type CassandraStoreTestSuite struct {
	suite.Suite
	store *Store
}

// NOTE(gabe): using this method of setup is definitely less elegant
// than a SetupTest() and TearDownTest() helper functions of suite,
// but has the unfortunate sideeffect of making test runs on my MBP go
// from ~3m wall to 10m wall. For now, keep using init() until a fix
// for this is found

var store *Store
var testScope = tally.NewTestScope("", map[string]string{})

// This test vector borrowed from gzip test suite to simulate a checksum
// error during ucompressing data
var badCheckSumData = []byte{
	0x1f, 0x8b, 0x08, 0x08, 0xc8, 0x58, 0x13, 0x4a,
	0x00, 0x03, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x2e,
	0x74, 0x78, 0x74, 0x00, 0xcb, 0x48, 0xcd, 0xc9,
	0xc9, 0x57, 0x28, 0xcf, 0x2f, 0xca, 0x49, 0xe1,
	0x02, 0x00, 0x2d, 0x3b, 0x08, 0xaf, 0xff, 0x00,
	0x00, 0x00,
}

func init() {
	conf := MigrateForTest()
	var err error
	store, err = NewStore(conf, testScope)
	if err != nil {
		log.Fatal(err)
	}
}

func TestCassandraStore(t *testing.T) {
	suite.Run(t, new(CassandraStoreTestSuite))
	assert.True(t, testScope.Snapshot().Counters()["execute.execute+result=success,store=peloton_test"].Value() > 0)
}

func (suite *CassandraStoreTestSuite) TestMigrateString() {
	conf := MigrateForTest()
	conf.CassandraConn.Username = "user"
	conf.CassandraConn.Password = "password"
	expectedStr := fmt.Sprintf("cassandra://%v:%v@%v:%v/%v",
		conf.CassandraConn.Username,
		conf.CassandraConn.Password,
		conf.CassandraConn.ContactPoints[0],
		conf.CassandraConn.Port,
		conf.StoreName)
	expectedStr = strings.Replace(expectedStr, " ", "", -1)
	connStr := conf.MigrateString()
	suite.Equal(connStr, expectedStr)
}

// TestGetTaskIDsForJobAndState tests reading task IDs for given job and state
func (suite *CassandraStoreTestSuite) TestGetTaskIDsForJobAndState() {
	var taskStore storage.TaskStore
	taskStore = store
	var jobID = peloton.JobID{Value: uuid.New()}
	runtimes := make(map[uint32]*task.RuntimeInfo)
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = 10
	jobConfig.InstanceConfig = map[uint32]*task.TaskConfig{}

	for i := 0; i < 10; i++ {
		taskInfo := createTaskInfo(jobConfig, &jobID, 0)
		taskInfo.Config = &task.TaskConfig{Name: fmt.Sprintf("task_%d", i)}
		jobConfig.InstanceConfig[uint32(i)] = taskInfo.Config
		taskInfo.Runtime.State = task.TaskState_RUNNING
		if i%2 == 0 {
			taskInfo.Runtime.State = task.TaskState_PENDING
		}
		runtimes[uint32(i)] = taskInfo.Runtime
	}

	err := suite.createJob(context.Background(), &jobID, jobConfig, &models.ConfigAddOn{}, "user1")
	suite.Nil(err)

	for i := 0; i < 10; i++ {
		runtimes[uint32(i)].ConfigVersion = jobConfig.GetChangeLog().
			GetVersion()
		err = taskStore.CreateTaskRuntime(context.Background(),
			&jobID, uint32(i), runtimes[uint32(i)], "test", jobConfig.GetType())
		suite.NoError(err)
	}

	// get the task configs
	ids, err := store.GetTaskIDsForJobAndState(
		context.Background(), &jobID, task.TaskState_PENDING.String())
	suite.NoError(err)
	suite.Equal(5, len(ids))

	ids, err = store.GetTaskIDsForJobAndState(
		context.Background(), &jobID, task.TaskState_RUNNING.String())
	suite.NoError(err)
	suite.Equal(5, len(ids))
}

func (suite *CassandraStoreTestSuite) createJob(
	ctx context.Context,
	id *peloton.JobID,
	jobConfig *job.JobConfig,
	configAddOn *models.ConfigAddOn,
	owner string) error {
	now := time.Now()
	jobConfig.ChangeLog = &peloton.ChangeLog{
		CreatedAt: uint64(now.UnixNano()),
		UpdatedAt: uint64(now.UnixNano()),
		Version:   1,
	}
	if err := store.CreateJobConfig(ctx, id, jobConfig, configAddOn, jobConfig.GetChangeLog().GetVersion(), owner); err != nil {
		return err
	}

	var goalState job.JobState
	switch jobConfig.Type {
	case job.JobType_BATCH:
		goalState = job.JobState_SUCCEEDED
	default:
		goalState = job.JobState_RUNNING
	}

	initialJobRuntime := job.RuntimeInfo{
		State:        job.JobState_INITIALIZED,
		CreationTime: now.Format(time.RFC3339Nano),
		TaskStats:    make(map[string]uint32),
		GoalState:    goalState,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(now.UnixNano()),
			UpdatedAt: uint64(now.UnixNano()),
			Version:   1,
		},
		ConfigurationVersion: jobConfig.GetChangeLog().GetVersion(),
	}
	// Init the task stats to reflect that all tasks are in initialized state
	initialJobRuntime.TaskStats[task.TaskState_INITIALIZED.String()] = jobConfig.InstanceCount

	// Create the initial job runtime record
	err := store.CreateJobRuntime(ctx, id, &initialJobRuntime)
	if err != nil {
		return err
	}
	err = updateJobIndex(ctx, id, jobConfig, &initialJobRuntime)
	if err != nil {
		return err
	}
	return createTaskConfigs(ctx, id, jobConfig, configAddOn)
}

// CreateTaskConfigs from the job config.
func createTaskConfigs(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig, configAddOn *models.ConfigAddOn) error {
	version := jobConfig.GetChangeLog().GetVersion()
	if jobConfig.GetDefaultConfig() != nil {
		if err := store.CreateTaskConfig(
			ctx,
			id,
			common.DefaultTaskConfigID,
			jobConfig.GetDefaultConfig(),
			configAddOn,
			version,
		); err != nil {
			return err
		}
	}

	for instanceID, cfg := range jobConfig.GetInstanceConfig() {
		merged := taskconfig.Merge(jobConfig.GetDefaultConfig(), cfg)
		// TODO set correct version
		if err := store.CreateTaskConfig(
			ctx,
			id,
			int64(instanceID),
			merged,
			configAddOn,
			version,
		); err != nil {
			return err
		}
	}

	return nil
}

// createTaskConfigsLegacy creates task configs in task_config table, and not
// task_config_v2 table. This will help in testing legacy jobs which don't have
// entries in the task_config table.
func createTaskConfigsLegacy(
	ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig) error {
	version := jobConfig.GetChangeLog().GetVersion()

	configBuffer, err := proto.Marshal(jobConfig.GetDefaultConfig())
	if err != nil {
		return err
	}
	queryBuilder := store.DataStore.NewQuery()
	stmt := queryBuilder.Insert(taskConfigTable).
		Columns("job_id", "version", "instance_id", "creation_time", "config").
		Values(id.GetValue(), version, common.DefaultTaskConfigID,
			time.Now().UTC(), configBuffer)
	if err = store.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		return err
	}
	for instanceID, cfg := range jobConfig.GetInstanceConfig() {
		merged := taskconfig.Merge(jobConfig.GetDefaultConfig(), cfg)
		mergedBuffer, err := proto.Marshal(merged)
		if err != nil {
			return err
		}
		stmt := queryBuilder.Insert(taskConfigTable).
			Columns("job_id", "version", "instance_id",
				"creation_time", "config").
			Values(id.GetValue(), version, int64(instanceID),
				time.Now().UTC(), mergedBuffer)
		if err = store.applyStatement(ctx, stmt, id.GetValue()); err != nil {
			return err
		}
	}
	return nil
}

// updateJobIndex creates/updates job_index row for a job. This method
// is provided for tests related to QueryJobs(), and should not be used
// for anything else.
// TODO Remove when QueryJobs() is moved to ORM.
func updateJobIndex(
	ctx context.Context,
	id *peloton.JobID,
	config *job.JobConfig,
	runtime *job.RuntimeInfo,
) error {
	if runtime == nil && config == nil {
		return nil
	}

	queryBuilder := store.DataStore.NewQuery()
	stmt := queryBuilder.Update(jobIndexTable).
		Where(qb.Eq{"job_id": id.GetValue()})

	if runtime != nil {
		runtimeBuffer, err := json.Marshal(runtime)
		if err != nil {
			return err
		}

		completeTime := time.Time{}
		if runtime.GetCompletionTime() != "" {
			completeTime, _ = time.Parse(
				time.RFC3339Nano,
				runtime.GetCompletionTime())
		}

		stmt = stmt.Set("runtime_info", runtimeBuffer).
			Set("state", runtime.GetState().String()).
			Set("creation_time", parseTime(runtime.GetCreationTime())).
			Set("completion_time", completeTime).
			Set("update_time", time.Now())
	}

	if config != nil {
		// Do not save the instance config with the job
		// configuration in the job_index table.
		instanceConfig := config.GetInstanceConfig()
		config.InstanceConfig = nil
		configBuffer, err := json.Marshal(config)
		config.InstanceConfig = instanceConfig
		if err != nil {
			return err
		}

		labelBuffer, err := json.Marshal(config.Labels)
		if err != nil {
			return err
		}

		stmt = stmt.Set("config", configBuffer).
			Set("respool_id", config.GetRespoolID().GetValue()).
			Set("owner", config.GetOwningTeam()).
			Set("name", config.GetName()).
			Set("job_type", uint32(config.GetType())).
			Set("instance_count", uint32(config.GetInstanceCount())).
			Set("labels", labelBuffer)
	}

	err := store.applyStatement(ctx, stmt, id.GetValue())
	if err != nil {
		return err
	}
	return nil
}

// updateJobIndex deletes job_index row for a job. This method
// is provided for tests related to QueryJobs(), and should not be used
// for anything else.
// TODO Remove when QueryJobs() is moved to ORM.
func deleteJobIndex(ctx context.Context, id *peloton.JobID) error {
	queryBuilder := store.DataStore.NewQuery()
	stmt := queryBuilder.Delete(jobIndexTable).
		Where(qb.Eq{"job_id": id.GetValue()})
	return store.applyStatement(ctx, stmt, id.GetValue())
}

// Run the following query to trigger lucene index refresh
func (suite *CassandraStoreTestSuite) refreshLuceneIndex() {
	queryBuilder := store.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(jobIndexTable).Where("expr(job_index_lucene_v2, '{refresh:true}')")
	_, err := store.DataStore.Execute(context.Background(), stmt)
	suite.NoError(err)
}

// Test compress and uncompress functionality
func (suite *CassandraStoreTestSuite) TestCompression() {
	buf := []byte("Test data for compression")
	compressedBuf, err := compress(buf)
	suite.NoError(err)

	// success case for compression
	uncompressedBuf, err := uncompress(compressedBuf)
	suite.NoError(err)
	suite.Equal(buf, uncompressedBuf)

	// simulate uncompression failures due to checksum
	uncompressedBuf, err = uncompress(badCheckSumData)
	suite.Equal(err, gzip.ErrChecksum)

	// simulate uncompressing data that was never compressed. this should not
	// throw error but just return the source buffer.
	uncompressedBuf, err = uncompress(buf)
	suite.NoError(err)
	suite.Equal(buf, uncompressedBuf)
}

func (suite *CassandraStoreTestSuite) TestQueryJobPaging() {
	var jobStore storage.JobStore
	jobStore = store

	var originalJobs []*job.JobConfig
	var jobIDs []*peloton.JobID
	var records = 300
	respool := &peloton.ResourcePoolID{Value: uuid.New()}
	configAddOn := &models.ConfigAddOn{}

	var keys0 = []string{"test0", "test1", "test2", "test3"}
	var vals0 = []string{"testValue0", "testValue1", `"testValue2"`, "testValue3"}

	for i := 0; i < records; i++ {
		var jobID = peloton.JobID{Value: uuid.New()}
		jobIDs = append(jobIDs, &jobID)
		var sla = job.SlaConfig{
			Priority:                22,
			MaximumRunningInstances: 3,
			Preemptible:             false,
		}
		var taskConfig = task.TaskConfig{
			Resource: &task.ResourceConfig{
				CpuLimit:    0.8,
				MemLimitMb:  800,
				DiskLimitMb: 1500,
				FdLimit:     1000 + uint32(i),
			},
		}
		var labels = []*peloton.Label{
			{Key: keys0[i%len(keys0)], Value: vals0[i%len(keys0)]},
		}
		var jobConfig = job.JobConfig{
			Name:          fmt.Sprintf("TestJob_%d", i),
			OwningTeam:    fmt.Sprintf("owner_%d", 1000+i),
			LdapGroups:    []string{"TestQueryJobPaging", "team6", "gign"},
			SLA:           &sla,
			DefaultConfig: &taskConfig,
			Labels:        labels,
			InstanceCount: 10,
			Type:          job.JobType_BATCH,
			Description:   fmt.Sprintf("A test job with awesome keyword%v keytest%v", i, i),
			RespoolID:     respool,
		}
		originalJobs = append(originalJobs, &jobConfig)
		err := suite.createJob(context.Background(), &jobID, &jobConfig, configAddOn, "uber")
		suite.NoError(err)

		// Update job runtime to different values
		runtime, err := jobStore.GetJobRuntime(context.Background(), jobID.GetValue())
		suite.NoError(err)

		runtime.State = job.JobState(i + 1)
		err = jobStore.UpdateJobRuntime(context.Background(), &jobID, runtime)
		suite.NoError(err)
	}

	suite.refreshLuceneIndex()

	spec := &job.QuerySpec{
		Keywords: []string{"TestQueryJobPaging", "test", "awesome"},
		Pagination: &query.PaginationSpec{
			Offset: 10,
			Limit:  25,
		},
	}
	_, _ = suite.queryJobs(spec, 25, int(_defaultQueryMaxLimit))

	var owner = query.PropertyPath{Value: "owner"}
	var orderByOwner = query.OrderBy{
		Property: &owner,
		Order:    query.OrderBy_ASC,
	}
	spec = &job.QuerySpec{
		Keywords: []string{"TestQueryJobPaging", "test", "awesome"},
		Pagination: &query.PaginationSpec{
			Offset: 10,
			Limit:  25,
			OrderBy: []*query.OrderBy{
				&orderByOwner,
			},
		},
	}
	result1, summary := suite.queryJobs(spec, 25, int(_defaultQueryMaxLimit))
	for i, c := range result1 {
		suite.Equal(fmt.Sprintf("owner_%d", 1010+i), c.Config.GetOwningTeam())
	}
	for i, c := range summary {
		suite.Equal(fmt.Sprintf("owner_%d", 1010+i), c.GetOwningTeam())
	}

	// Pagination with limit not set. Limit should default to _defaultQueryLimit
	spec = &job.QuerySpec{
		Keywords: []string{"TestQueryJobPaging", "test", "awesome"},
		Pagination: &query.PaginationSpec{
			OrderBy: []*query.OrderBy{
				&orderByOwner,
			},
		},
	}
	_, _ = suite.queryJobs(spec, int(_defaultQueryLimit), int(_defaultQueryMaxLimit))

	orderByOwner.Order = query.OrderBy_DESC
	spec = &job.QuerySpec{
		Keywords: []string{"TestQueryJobPaging", "test", "awesome"},
		Pagination: &query.PaginationSpec{
			Offset: 10,
			Limit:  25,
			OrderBy: []*query.OrderBy{
				&orderByOwner,
			},
		},
	}
	result1, summary = suite.queryJobs(spec, 25, int(_defaultQueryMaxLimit))
	for i, c := range result1 {
		suite.Equal(fmt.Sprintf("owner_%d", 1289-i), c.Config.GetOwningTeam())
	}
	for i, c := range summary {
		suite.Equal(fmt.Sprintf("owner_%d", 1289-i), c.GetOwningTeam())
	}

	_, _ = suite.queryJobs(&job.QuerySpec{}, int(_defaultQueryLimit), int(_defaultQueryMaxLimit))

	result1, summary, total, err := jobStore.QueryJobs(context.Background(), &peloton.ResourcePoolID{Value: uuid.New()}, &job.QuerySpec{}, false)
	suite.NoError(err)
	suite.Equal(0, len(result1))
	suite.Equal(0, len(summary))
	suite.Equal(0, int(total))

	spec = &job.QuerySpec{
		Pagination: &query.PaginationSpec{
			Limit: 1000,
		},
	}
	_, _ = suite.queryJobs(spec, int(_defaultQueryMaxLimit), int(_defaultQueryMaxLimit))

	for _, jobID := range jobIDs {
		suite.NoError(jobStore.DeleteJob(context.Background(), jobID.GetValue()))
		suite.NoError(deleteJobIndex(context.Background(), jobID))
	}
}

func (suite *CassandraStoreTestSuite) queryJobs(
	spec *job.QuerySpec, expectedEntriesPerPage int, expectedTotalEntries int) (
	[]*job.JobInfo, []*job.JobSummary) {
	var jobStore storage.JobStore
	jobStore = store
	result, summary, total, err := jobStore.QueryJobs(context.Background(), nil, spec, false)
	suite.NoError(err)
	suite.Equal(expectedEntriesPerPage, len(result))
	suite.Equal(expectedEntriesPerPage, len(summary))
	suite.Equal(expectedTotalEntries, int(total))
	return result, summary
}

func (suite *CassandraStoreTestSuite) TestJobQueryStaleLuceneIndex() {
	var jobStore storage.JobStore
	jobStore = store
	jobID := peloton.JobID{Value: uuid.New()}
	configAddOn := &models.ConfigAddOn{}
	var jobConfig = job.JobConfig{
		Name:        "StaleLuceneIndex",
		OwningTeam:  "owner",
		Type:        job.JobType_BATCH,
		Description: fmt.Sprintf("get jobs summary"),
	}
	err := suite.createJob(context.Background(), &jobID, &jobConfig, configAddOn, "uber")
	suite.NoError(err)

	runtime, err := jobStore.GetJobRuntime(context.Background(), jobID.GetValue())
	suite.NoError(err)

	// set job creation time to two days ago
	creationTime := time.Now().AddDate(0, 0, -5).UTC().Format(time.RFC3339Nano)
	runtime.CreationTime = creationTime
	err = jobStore.UpdateJobRuntime(context.Background(), &jobID, runtime)
	suite.NoError(err)
	err = updateJobIndex(context.Background(), &jobID, nil, runtime)
	suite.NoError(err)
	suite.refreshLuceneIndex()

	jobStates := []job.JobState{
		job.JobState_PENDING, job.JobState_RUNNING, job.JobState_INITIALIZED}
	spec := &job.QuerySpec{
		Name:      "StaleLuceneIndex",
		JobStates: jobStates,
	}
	_, summary := suite.queryJobs(spec, 1, 1)
	suite.Equal(creationTime, summary[0].GetRuntime().GetCreationTime())

	// Set runtime state to succeeded and update the job_index
	runtime.State = job.JobState_SUCCEEDED
	err = jobStore.UpdateJobRuntime(context.Background(), &jobID, runtime)
	suite.NoError(err)
	err = updateJobIndex(context.Background(), &jobID, nil, runtime)
	suite.NoError(err)

	// Now we have query from lucnene index showing the job as PENDING for 5days
	// but the job_index table has been updated to SUCCEEDED. This should be
	// reconciled and the final summary should not contain this job.
	newSummary, err := store.reconcileStaleBatchJobsFromJobSummaryList(
		context.Background(), summary, false)
	suite.NoError(err)
	suite.Equal(0, len(newSummary))

	// Run reconciliation but for terminal job query. So the new summary list
	// should contain the job but with its new terminal state.
	newSummary, err = store.reconcileStaleBatchJobsFromJobSummaryList(
		context.Background(), summary, true)
	suite.NoError(err)
	suite.Equal(1, len(newSummary))
	suite.Equal(job.JobState_SUCCEEDED, newSummary[0].GetRuntime().GetState())
}

func (suite *CassandraStoreTestSuite) TestGetJobSummaryByTimeRange() {
	var jobStore storage.JobStore
	jobStore = store
	jobID := peloton.JobID{Value: uuid.New()}
	configAddOn := &models.ConfigAddOn{}
	var labels = []*peloton.Label{
		{Key: "test0", Value: "test-val0"},
		{Key: "test1", Value: "test-val1"},
	}
	var jobConfig = job.JobConfig{
		Name:          "GetJobSummaryByTimeRange",
		OwningTeam:    "owner",
		LdapGroups:    []string{"job", "summary"},
		Labels:        labels,
		InstanceCount: 10,
		Type:          job.JobType_BATCH,
		Description:   fmt.Sprintf("get jobs summary"),
	}
	err := suite.createJob(context.Background(), &jobID, &jobConfig, configAddOn, "uber")
	suite.NoError(err)

	where := fmt.Sprintf(`expr(job_index_lucene_v2,` +
		`'{refresh:true, filter: [` +
		`{type: "match", field:"name", value:"GetJobSummary"}` +
		`]}')`)
	queryBuilder := store.DataStore.NewQuery()
	stmt := queryBuilder.Select("job_id",
		"name",
		"owner",
		"job_type",
		"respool_id",
		"instance_count",
		"labels",
		"runtime_info").
		From(jobIndexTable)
	stmt = stmt.Where(where)
	_, err = store.executeRead(context.Background(), stmt)
	suite.NoError(err)

	spec := &job.QuerySpec{
		Name: "GetJobSummaryByTimeRange",
	}
	// Modify creation_time to now - 8 days
	updateStmt := queryBuilder.Update(jobIndexTable).
		Set("creation_time", time.Now().AddDate(0, 0, -8).UTC()).
		Where(qb.Eq{"job_id": jobID.GetValue()})
	_, err = store.executeWrite(context.Background(), updateStmt)
	suite.NoError(err)

	suite.refreshLuceneIndex()

	_, _ = suite.queryJobs(spec, 1, 1)

	// Modify state to SUCCEEDED. Now this job should not show up.
	updateStmt = queryBuilder.Update(jobIndexTable).
		Set("state", "SUCCEEDED").
		Where(qb.Eq{"job_id": jobID.GetValue()})
	_, err = store.executeWrite(context.Background(), updateStmt)
	suite.NoError(err)

	suite.refreshLuceneIndex()

	// Any query for terminal states should not display jobs that are older than 7 days
	jobStates := []job.JobState{job.JobState_KILLED, job.JobState_FAILED, job.JobState_SUCCEEDED}
	spec = &job.QuerySpec{
		Name:      "GetJobSummaryByTimeRange",
		JobStates: jobStates,
	}
	_, _ = suite.queryJobs(spec, 0, 0)

	updateStmt = queryBuilder.Update(jobIndexTable).
		Set("state", "KILLED").
		Where(qb.Eq{"job_id": jobID.GetValue()})
	_, err = store.executeWrite(context.Background(), updateStmt)
	suite.NoError(err)

	suite.refreshLuceneIndex()

	_, _ = suite.queryJobs(spec, 0, 0)

	updateStmt = queryBuilder.Update(jobIndexTable).
		Set("state", "FAILED").
		Where(qb.Eq{"job_id": jobID.GetValue()})
	_, err = store.executeWrite(context.Background(), updateStmt)
	suite.NoError(err)

	suite.refreshLuceneIndex()

	_, _ = suite.queryJobs(spec, 0, 0)

	updateStmt = queryBuilder.Update(jobIndexTable).
		Set("state", "RUNNING").
		Where(qb.Eq{"job_id": jobID.GetValue()})
	_, err = store.executeWrite(context.Background(), updateStmt)
	suite.NoError(err)

	suite.refreshLuceneIndex()

	// Any query for active states should display jobs that are older than 7 days
	jobStates = []job.JobState{job.JobState_PENDING, job.JobState_RUNNING, job.JobState_INITIALIZED}
	spec = &job.QuerySpec{
		Name:      "GetJobSummaryByTimeRange",
		JobStates: jobStates,
	}

	// Even if job is created 8 days ago, display it because it is in active state.
	_, _ = suite.queryJobs(spec, 1, 1)

	jobStates = []job.JobState{job.JobState_RUNNING, job.JobState_SUCCEEDED}
	spec = &job.QuerySpec{
		Name:      "GetJobSummaryByTimeRange",
		JobStates: jobStates,
	}
	// When searching for ACTIVE + TERMINAL states, we will again default to 7 days time range.
	_, _ = suite.queryJobs(spec, 0, 0)

	// Modify creation_time to now - 3 days
	updateStmt = queryBuilder.Update(jobIndexTable).
		Set("creation_time", time.Now().AddDate(0, 0, -3).UTC()).
		Where(qb.Eq{"job_id": jobID.GetValue()})
	_, err = store.executeWrite(context.Background(), updateStmt)
	suite.NoError(err)

	suite.refreshLuceneIndex()

	_, _ = suite.queryJobs(spec, 1, 1)

	// query by creation time ranges with last 5 days
	now := time.Now().UTC()
	max, _ := ptypes.TimestampProto(now)
	min, _ := ptypes.TimestampProto(now.AddDate(0, 0, -5))
	spec = &job.QuerySpec{
		Name:              "GetJobSummaryByTimeRange",
		CreationTimeRange: &peloton.TimeRange{Min: min, Max: max},
	}

	// The 3 day old job here will show up in this query
	_, _ = suite.queryJobs(spec, 1, 1)

	// query by creation time ranges with last 2 days
	now = time.Now().UTC()
	max, _ = ptypes.TimestampProto(now)
	min, _ = ptypes.TimestampProto(now.AddDate(0, 0, -2))
	spec = &job.QuerySpec{
		Name:              "GetJobSummaryByTimeRange",
		CreationTimeRange: &peloton.TimeRange{Min: min, Max: max},
	}
	// The 3 day old job here will NOT show up in this query
	_, _ = suite.queryJobs(spec, 0, 0)

	// Modify creation_time to now - 1 hour
	updateStmt = queryBuilder.Update(jobIndexTable).
		Set("creation_time", time.Now().Add(-1*time.Hour).UTC()).
		Where(qb.Eq{"job_id": jobID.GetValue()})
	_, err = store.executeWrite(context.Background(), updateStmt)
	suite.NoError(err)

	suite.refreshLuceneIndex()

	// query by creation time ranges with last 2 hours
	now = time.Now().UTC()
	max, _ = ptypes.TimestampProto(now)
	min, _ = ptypes.TimestampProto(now.Add(-2 * time.Hour))
	spec = &job.QuerySpec{
		Name:              "GetJobSummaryByTimeRange",
		CreationTimeRange: &peloton.TimeRange{Min: min, Max: max},
	}
	// The 1 hour old job here will show up in this query
	_, _ = suite.queryJobs(spec, 1, 1)

	// query by creation time range where max < min. This should error out
	spec = &job.QuerySpec{
		Name:              "GetJobSummaryByTimeRange",
		CreationTimeRange: &peloton.TimeRange{Min: max, Max: min},
	}
	_, _, _, err = jobStore.QueryJobs(context.Background(), nil, spec, true)
	suite.Error(err)

	// query by completion time ranges with last 2 hours
	spec = &job.QuerySpec{
		Name:                "GetJobSummaryByTimeRange",
		CompletionTimeRange: &peloton.TimeRange{Min: min, Max: max},
	}

	// The 1 hour old job here will NOT show up in this query because its completion_time
	// does not match the query spec
	_, _ = suite.queryJobs(spec, 0, 0)

	// query by both creation and completion time ranges with last 2 hours
	spec = &job.QuerySpec{
		Name:                "GetJobSummaryByTimeRange",
		CreationTimeRange:   &peloton.TimeRange{Min: min, Max: max},
		CompletionTimeRange: &peloton.TimeRange{Min: min, Max: max},
	}

	// The 1 hour old job here will NOT show up in this query because its completion_time
	// does not match the query spec (even if it matches the creation_time spec)
	// This is an AND operation
	_, _ = suite.queryJobs(spec, 0, 0)

	// update creation time and completion time to t - 1h
	updateStmt = queryBuilder.Update(jobIndexTable).
		Set("state", "SUCCEEDED").
		Set("creation_time", time.Now().Add(-1*time.Hour).UTC()).
		Set("completion_time", time.Now().Add(-1*time.Hour).UTC()).
		Where(qb.Eq{"job_id": jobID.GetValue()})
	_, err = store.executeWrite(context.Background(), updateStmt)
	suite.NoError(err)

	suite.refreshLuceneIndex()

	// again query by both creation and completion time ranges with last 2 hours
	// The 1 hour old job here will show up in this query
	_, _ = suite.queryJobs(spec, 1, 1)

	// now query by just completion time ranges with last 2 hours
	spec = &job.QuerySpec{
		Name:                "GetJobSummaryByTimeRange",
		CompletionTimeRange: &peloton.TimeRange{Min: min, Max: max},
	}
	// The 1 hour old job here will show up in this query
	_, _ = suite.queryJobs(spec, 1, 1)

	suite.NoError(jobStore.DeleteJob(context.Background(), jobID.GetValue()))
	suite.NoError(deleteJobIndex(context.Background(), &jobID))
}

func (suite *CassandraStoreTestSuite) TestGetJobSummary() {
	var jobStore storage.JobStore
	jobStore = store
	jobID := peloton.JobID{Value: uuid.New()}
	configAddOn := &models.ConfigAddOn{}
	var labels = []*peloton.Label{
		{Key: "test0", Value: "test-val0"},
		{Key: "test1", Value: "test-val1"},
	}
	var jobConfig = job.JobConfig{
		Name:          "GetJobSummary",
		OwningTeam:    "owner",
		LdapGroups:    []string{"job", "summary"},
		Labels:        labels,
		InstanceCount: 10,
		Type:          job.JobType_BATCH,
		Description:   fmt.Sprintf("get jobs summary"),
	}

	err := suite.createJob(context.Background(), &jobID, &jobConfig, configAddOn, "uber")
	suite.NoError(err)

	where := fmt.Sprintf(`expr(job_index_lucene_v2,` +
		`'{refresh:true, filter: [` +
		`{type: "match", field:"name", value:"GetJobSummary"}` +
		`]}')`)
	queryBuilder := store.DataStore.NewQuery()
	stmt := queryBuilder.Select("job_id",
		"name",
		"owner",
		"job_type",
		"respool_id",
		"instance_count",
		"labels",
		"runtime_info").
		From(jobIndexTable)
	stmt = stmt.Where(where)
	allResults, err := store.executeRead(context.Background(), stmt)
	suite.NoError(err)
	suite.Equal(1, len(allResults))

	summaryResultFromLucene, err := store.getJobSummaryFromResultMap(
		context.Background(), allResults)
	suite.NoError(err)

	suite.Equal(1, len(summaryResultFromLucene))
	suite.Equal("GetJobSummary", summaryResultFromLucene[0].GetName())
	suite.Equal("owner", summaryResultFromLucene[0].GetOwningTeam())
	suite.Equal("owner", summaryResultFromLucene[0].GetOwner())

	// tamper allResults to make job_id not UUID and look for error.
	allResults[0]["job_id"] = "junk"
	_, err = store.getJobSummaryFromResultMap(
		context.Background(), allResults)
	suite.Error(err)

	// QueryJobs with summaryOnly = true. result1 should be nil
	spec := &job.QuerySpec{
		Name: "GetJobSummary",
	}
	result1, summary, total, err := jobStore.QueryJobs(context.Background(), nil, spec, true)
	suite.NoError(err)
	suite.Equal(0, len(result1))
	suite.Equal(1, len(summary))
	suite.Equal(1, int(total))
	suite.Equal("GetJobSummary", summary[0].GetName())

	// query with spec = nil should not result in error, it should result in 0 entries.
	_, summary, total, err = jobStore.QueryJobs(context.Background(), nil, nil, true)
	suite.NoError(err)
	suite.Equal(0, len(summary))
	suite.Equal(0, int(total))

	suite.NoError(jobStore.DeleteJob(context.Background(), jobID.GetValue()))
	suite.NoError(deleteJobIndex(context.Background(), &jobID))
}

func (suite *CassandraStoreTestSuite) TestQueryJob() {
	var jobStore storage.JobStore
	jobStore = store

	var originalJobs []*job.JobConfig
	var jobIDs []*peloton.JobID
	var records = 5

	var keys0 = []string{"test0", "test1", "test2", "test3", "test4", "test5"}
	var vals0 = []string{"testValue0", "testValue1", `"testValue2"`, "testValue3", "testValue4", "testValue5"}

	var keys1 = []string{"key0", "key1", "key2", "key3", "key4", "key5"}
	var vals1 = []string{"valX0", "valX1", "valX2", "valX3", "valX4", "valX5"}
	keyCommon := "keyX"
	valCommon := "valX"
	// Create 5 jobs with different labels and a common label
	for i := 0; i < records; i++ {
		var jobID = peloton.JobID{Value: uuid.New()} // fmt.Sprintf("TestQueryJob%d", i)}
		jobIDs = append(jobIDs, &jobID)
		configAddOn := &models.ConfigAddOn{}
		var sla = job.SlaConfig{
			Priority:                22,
			MaximumRunningInstances: 3,
			Preemptible:             false,
		}
		var taskConfig = task.TaskConfig{
			Resource: &task.ResourceConfig{
				CpuLimit:    0.8,
				MemLimitMb:  800,
				DiskLimitMb: 1500,
				FdLimit:     1000 + uint32(i),
			},
		}
		instanceConfig := make(map[uint32]*task.TaskConfig)
		instanceConfig[0] = &taskConfig
		var labels = []*peloton.Label{
			{Key: keys0[i], Value: vals0[i]},
			{Key: keys1[i], Value: vals1[i]},
			{Key: keyCommon, Value: valCommon},
		}
		var jobConfig = job.JobConfig{
			Name:           fmt.Sprintf("TestQueryJob_%d", i),
			OwningTeam:     fmt.Sprintf("query_owner_%d", i),
			LdapGroups:     []string{"money", "team6", "gign"},
			SLA:            &sla,
			DefaultConfig:  &taskConfig,
			Labels:         labels,
			InstanceCount:  10,
			Type:           job.JobType_BATCH,
			Description:    fmt.Sprintf("A test job with awesome keyword%v keytest%v", i, i),
			InstanceConfig: instanceConfig,
		}
		originalJobs = append(originalJobs, &jobConfig)
		err := suite.createJob(context.Background(), &jobID, &jobConfig, configAddOn, "uber")
		suite.NoError(err)

		// Update job runtime to different values
		runtime, err := jobStore.GetJobRuntime(context.Background(), jobID.GetValue())
		suite.NoError(err)

		runtime.State = job.JobState(i + 1)
		err = jobStore.UpdateJobRuntime(context.Background(), &jobID, runtime)
		suite.NoError(err)

		err = updateJobIndex(context.Background(), &jobID, nil, runtime)
		suite.NoError(err)
	}

	suite.refreshLuceneIndex()

	// query by common label should return all jobs
	spec := &job.QuerySpec{
		Labels: []*peloton.Label{
			{Key: keyCommon, Value: valCommon},
		},
	}
	result1, summary := suite.queryJobs(spec, records, records)
	asMap := map[string]*job.JobInfo{}
	for _, r := range result1 {
		asMap[r.Id.Value] = r
	}

	for i := 0; i < records; i++ {
		suite.Equal(fmt.Sprintf("TestQueryJob_%d", i), asMap[jobIDs[i].Value].Config.Name)
	}

	// query by specific state returns one job
	for i := 0; i < records; i++ {
		spec = &job.QuerySpec{
			Labels: []*peloton.Label{
				{Key: keyCommon, Value: valCommon},
			},
			JobStates: []job.JobState{job.JobState(i + 1)},
		}
		result1, summary := suite.queryJobs(spec, 1, 1)
		suite.Equal(i+1, int(result1[0].Runtime.State))
		suite.Nil(result1[0].GetConfig().GetInstanceConfig())
		suite.Equal(fmt.Sprintf("TestQueryJob_%d", i), asMap[jobIDs[i].Value].Config.Name)

		suite.Equal(i+1, int(summary[0].GetRuntime().GetState()))
		suite.Equal(fmt.Sprintf("TestQueryJob_%d", i), summary[0].GetName())
	}

	for i := 0; i < records; i++ {
		spec = &job.QuerySpec{
			Owner: fmt.Sprintf("query_owner_%d", i),
		}
		_, summary = suite.queryJobs(spec, 1, 1)
		suite.Equal(fmt.Sprintf("query_owner_%d", i), summary[0].GetOwningTeam())
	}
	// query by Owner_0 returns 0 jobs
	spec = &job.QuerySpec{
		Owner: "Query_Owner_0",
	}
	_, _ = suite.queryJobs(spec, 0, 0)

	// query by Owner returns 0 jobs
	spec = &job.QuerySpec{
		Owner: "Owner",
	}
	_, _ = suite.queryJobs(spec, 0, 0)

	// query by Name
	for i := 0; i < records; i++ {
		spec = &job.QuerySpec{
			Name: fmt.Sprintf("TestQueryJob_%d", i),
		}
		_, summary = suite.queryJobs(spec, 1, 1)
		suite.Equal(fmt.Sprintf("TestQueryJob_%d", i), summary[0].GetName())
	}
	// query by wrong name returns 0 jobs
	spec = &job.QuerySpec{
		Name: "TestQueryJob_wrong_name",
	}
	_, _ = suite.queryJobs(spec, 0, 0)
	// query by partial name returns 5 jobs
	spec = &job.QuerySpec{
		Name: "TestQueryJob",
	}
	_, _ = suite.queryJobs(spec, records, records)

	// Test query with partial keyword
	spec = &job.QuerySpec{
		Keywords: []string{"stQueryJob"},
	}
	_, _ = suite.queryJobs(spec, records, records)

	// Test query with partial keyword that should match one job
	spec = &job.QuerySpec{
		Keywords: []string{"stQueryJob_2"},
	}
	_, summary = suite.queryJobs(spec, 1, 1)
	suite.Equal("TestQueryJob_2", summary[0].GetName())

	// test sort by name in ascending order
	orderByName := query.PropertyPath{Value: "name"}
	orderByOwner := query.PropertyPath{Value: "owner"}
	orderBy := []*query.OrderBy{
		{
			Order:    query.OrderBy_ASC,
			Property: &orderByName,
		},
	}
	spec = &job.QuerySpec{
		Keywords: []string{"TestQueryJob"},
		Pagination: &query.PaginationSpec{
			OrderBy: orderBy,
		},
	}
	// expect that first entry is TestQueryJob_0
	_, summary = suite.queryJobs(spec, records, records)
	suite.Equal("TestQueryJob_0", summary[0].GetName())

	// test sort by name in descending order
	orderBy = []*query.OrderBy{
		{
			Order:    query.OrderBy_DESC,
			Property: &orderByName,
		},
	}
	spec = &job.QuerySpec{
		Keywords: []string{"TestQueryJob"},
		Pagination: &query.PaginationSpec{
			OrderBy: orderBy,
		},
	}
	// expect that first entry is TestQueryJob_4
	_, summary = suite.queryJobs(spec, records, records)
	suite.Equal("TestQueryJob_4", summary[0].GetName())

	// test sort by owner in ascending order
	orderBy = []*query.OrderBy{
		{
			Order:    query.OrderBy_ASC,
			Property: &orderByOwner,
		},
	}
	spec = &job.QuerySpec{
		Keywords: []string{"query_owner"},
		Pagination: &query.PaginationSpec{
			OrderBy: orderBy,
		},
	}
	// expect that first entry is owner_0
	_, summary = suite.queryJobs(spec, records, records)
	suite.Equal("query_owner_0", summary[0].GetOwningTeam())

	// test sort by owner in descending order
	orderBy = []*query.OrderBy{
		{
			Order:    query.OrderBy_DESC,
			Property: &orderByOwner,
		},
	}
	spec = &job.QuerySpec{
		Keywords: []string{"query_owner"},
		Pagination: &query.PaginationSpec{
			OrderBy: orderBy,
		},
	}
	// expect that first entry is owner_4
	_, summary = suite.queryJobs(spec, records, records)
	suite.Equal("query_owner_4", summary[0].GetOwningTeam())

	// Test query with partial keyword and owner that should match one job
	spec = &job.QuerySpec{
		Keywords: []string{"stQueryJob_2"},
		Owner:    "query_owner_2",
	}
	_, _ = suite.queryJobs(spec, 1, 1)

	// Update tasks to different states, and query by state
	for i := 0; i < records; i++ {
		spec = &job.QuerySpec{
			Labels: []*peloton.Label{
				{Key: keys0[i], Value: vals0[i]},
				{Key: keys1[i], Value: vals1[i]},
			},
		}
		_, _ = suite.queryJobs(spec, 1, 1)
		suite.Equal(fmt.Sprintf("TestQueryJob_%d", i), asMap[jobIDs[i].Value].Config.Name)
	}

	// query for non-exist label return nothing
	var other = "other"
	spec = &job.QuerySpec{
		Labels: []*peloton.Label{
			{Key: keys0[0], Value: other},
			{Key: keys1[1], Value: vals1[0]},
		},
	}
	_, _ = suite.queryJobs(spec, 0, 0)

	// Test query with keyword
	spec = &job.QuerySpec{
		Keywords: []string{"team6", "test", "awesome"},
	}
	_, _ = suite.queryJobs(spec, records, records)

	spec = &job.QuerySpec{
		Keywords: []string{"team6", "test", "awesome", "keytest1"},
	}
	_, _ = suite.queryJobs(spec, 1, 1)

	spec = &job.QuerySpec{
		Keywords: []string{"team6", "test", "awesome", "nonexistkeyword"},
	}
	_, _ = suite.queryJobs(spec, 0, 0)

	// Query with both labels and keyword
	spec = &job.QuerySpec{
		Labels: []*peloton.Label{
			{Key: keys0[0], Value: vals0[0]},
		},
		Keywords: []string{"team6", "test", "awesome"},
	}
	_, _ = suite.queryJobs(spec, 1, 1)

	spec = &job.QuerySpec{
		Keywords: []string{"team6", "test", "awesome"},
		Pagination: &query.PaginationSpec{
			Offset: 1,
		},
	}
	_, _ = suite.queryJobs(spec, records-1, records)

	spec = &job.QuerySpec{
		Keywords: []string{"team6", "test", "awesome"},
		Pagination: &query.PaginationSpec{
			Offset: 0,
			Limit:  1,
		},
	}
	_, _ = suite.queryJobs(spec, 1, records)

	// Test max limit should cap total returned.
	spec = &job.QuerySpec{
		Keywords: []string{"team6", "test", "awesome"},
		Pagination: &query.PaginationSpec{
			Offset:   0,
			Limit:    1,
			MaxLimit: 2,
		},
	}
	// expected total should be 2, same as MaxLimit, instead of 5.
	_, _ = suite.queryJobs(spec, 1, 2)

	spec = &job.QuerySpec{
		Labels: []*peloton.Label{{
			Key:   keys0[2],
			Value: vals0[2],
		}},
	}
	_, _ = suite.queryJobs(spec, 1, 1)

	// Query for multiple states
	for i := 0; i < records; i++ {
		runtime, err := jobStore.GetJobRuntime(context.Background(), jobIDs[i].GetValue())
		suite.NoError(err)
		runtime.State = job.JobState(i)
		store.UpdateJobRuntime(context.Background(), jobIDs[i], runtime)
	}

	suite.refreshLuceneIndex()

	jobStates := []job.JobState{job.JobState_PENDING, job.JobState_RUNNING, job.JobState_SUCCEEDED}
	spec = &job.QuerySpec{
		Labels: []*peloton.Label{
			{Key: keyCommon, Value: valCommon},
		},
		JobStates: jobStates,
	}
	_, _ = suite.queryJobs(spec, len(jobStates), len(jobStates))

	jobStates = []job.JobState{job.JobState_PENDING, job.JobState_INITIALIZED, job.JobState_RUNNING, job.JobState_SUCCEEDED}
	spec = &job.QuerySpec{
		Labels: []*peloton.Label{
			{Key: keyCommon, Value: valCommon},
		},
		JobStates: jobStates,
	}
	_, _ = suite.queryJobs(spec, len(jobStates), len(jobStates))
	for _, jobID := range jobIDs {
		suite.NoError(jobStore.DeleteJob(context.Background(), jobID.GetValue()))
		suite.NoError(deleteJobIndex(context.Background(), jobID))
	}
}

func (suite *CassandraStoreTestSuite) TestCreateGetJobConfig() {
	var jobStore storage.JobStore
	jobStore = store
	var originalJobs []*job.JobConfig
	var records = 1
	var keys = []string{"testKey0", "testKey1", "testKey2", "key0"}
	var vals = []string{"testVal0", "testVal1", "testVal2", "val0"}
	// Wait for 5 * 50ms for DB to be cleaned up
	var maxAttempts = 5
	for i := 0; i < records; i++ {
		var jobID = peloton.JobID{Value: uuid.New()}
		configAddOn := &models.ConfigAddOn{
			SystemLabels: []*peloton.Label{
				{
					Key:   "peloton.job_id",
					Value: jobID.GetValue(),
				},
			},
		}
		var sla = job.SlaConfig{
			Priority:                22,
			MaximumRunningInstances: 3,
			Preemptible:             false,
		}
		var taskConfig = task.TaskConfig{
			Resource: &task.ResourceConfig{
				CpuLimit:    0.8,
				MemLimitMb:  800,
				DiskLimitMb: 1500,
				FdLimit:     1000 + uint32(i),
			},
		}
		var labels = []*peloton.Label{
			{Key: keys[0], Value: vals[0]},
			{Key: keys[1], Value: vals[1]},
			{Key: keys[2], Value: vals[2]},
		}
		// Add some special label to job0 and job1
		if i < 2 {
			labels = append(labels, &peloton.Label{Key: keys[3], Value: vals[3]})
		}

		// Add owner to job0 and job1
		var owner = "team6"
		if i < 2 {
			owner = "money"
		}
		instanceConfig := make(map[uint32]*task.TaskConfig)
		instanceConfig[0] = &taskConfig
		instanceConfig[1] = &taskConfig
		instanceConfig[2] = &taskConfig
		var jobconfig = job.JobConfig{
			Name:           jobID.GetValue(),
			OwningTeam:     owner,
			LdapGroups:     []string{"money", "team6", "otto"},
			SLA:            &sla,
			InstanceConfig: instanceConfig,
			InstanceCount:  3,
			Labels:         labels,
		}
		originalJobs = append(originalJobs, &jobconfig)
		err := suite.createJob(context.Background(), &jobID, &jobconfig, configAddOn, "uber")
		suite.NoError(err)

		// Create job with same job id would be no op
		jobconfig.Labels = nil
		jobconfig.Name = "random"
		err = suite.createJob(context.Background(), &jobID, &jobconfig, configAddOn, "uber2")
		suite.Error(err)

		// refresh lucene
		suite.refreshLuceneIndex()
		spec := &job.QuerySpec{
			Name: jobID.GetValue(),
		}
		// query jobs should result in 1 entry after delete
		_, _ = suite.queryJobs(spec, 1, 1)

		var jobconf *job.JobConfig
		jobconf, _, err = jobStore.GetJobConfig(context.Background(), jobID.GetValue())
		suite.NoError(err)
		suite.Equal(jobconf.Name, jobID.GetValue())
		suite.Equal(len(jobconf.Labels), 4)
		taskConfigs, addOn, noErr := store.GetTaskConfigs(context.Background(), &jobID, []uint32{0, 1, 2}, 1)
		suite.Equal(len(taskConfigs), 3)
		suite.NoError(noErr)
		suite.Len(addOn.SystemLabels, len(configAddOn.SystemLabels))
		for i := 0; i < len(addOn.SystemLabels); i++ {
			suite.Equal(configAddOn.SystemLabels[i].Key, addOn.SystemLabels[i].Key)
			suite.Equal(configAddOn.SystemLabels[i].Value, addOn.SystemLabels[i].Value)
		}

		suite.NoError(jobStore.DeleteJob(context.Background(), jobID.GetValue()))
		suite.NoError(deleteJobIndex(context.Background(), &jobID))

		for i := 0; i < maxAttempts; i++ {
			jobconf, addOn, err = jobStore.GetJobConfig(context.Background(), jobID.GetValue())
			if err != nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		suite.True(yarpcerrors.IsNotFound(err))
		suite.Nil(jobconf)

		var jobRuntime *job.RuntimeInfo
		for i = 0; i < maxAttempts; i++ {
			jobRuntime, err = jobStore.GetJobRuntime(context.Background(), jobID.GetValue())
			if err != nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		suite.True(yarpcerrors.IsNotFound(err))
		suite.Nil(jobRuntime)

		tasks, err := store.GetTasksForJob(context.Background(), &jobID)
		suite.Len(tasks, 0)
		suite.NoError(err)

		// refresh lucene
		suite.refreshLuceneIndex()
		// query jobs should result in 0 entries after delete
		_, _ = suite.queryJobs(spec, 0, 0)
	}
}

func (suite *CassandraStoreTestSuite) TestFrameworkInfo() {
	var frameworkStore storage.FrameworkInfoStore
	frameworkStore = store
	err := frameworkStore.SetMesosFrameworkID(context.Background(), "framework1", "12345")
	suite.NoError(err)
	var frameworkID string
	frameworkID, err = frameworkStore.GetFrameworkID(context.Background(), "framework1")
	suite.NoError(err)
	suite.Equal(frameworkID, "12345")

	frameworkID, err = frameworkStore.GetFrameworkID(context.Background(), "framework2")
	suite.Error(err)

	err = frameworkStore.SetMesosStreamID(context.Background(), "framework1", "s-12345")
	suite.NoError(err)

	frameworkID, err = frameworkStore.GetFrameworkID(context.Background(), "framework1")
	suite.NoError(err)
	suite.Equal(frameworkID, "12345")

	frameworkID, err = frameworkStore.GetMesosStreamID(context.Background(), "framework1")
	suite.NoError(err)
	suite.Equal(frameworkID, "s-12345")
}

func (suite *CassandraStoreTestSuite) TestAddTasks() {
	var taskStore storage.TaskStore
	taskStore = store
	var nJobs = 3
	var nTasks = uint32(3)
	var jobIDs []*peloton.JobID
	var jobs []*job.JobConfig
	configAddOn := &models.ConfigAddOn{}
	for i := 0; i < nJobs; i++ {
		var jobID = peloton.JobID{Value: uuid.New()}
		jobIDs = append(jobIDs, &jobID)
		jobConfig := createJobConfig()
		jobConfig.Name = fmt.Sprintf("TestAddTasks_%d", i)
		jobs = append(jobs, jobConfig)
		err := suite.createJob(context.Background(), &jobID, jobConfig, configAddOn, "uber")
		suite.NoError(err)

		// For each job, create 3 tasks
		for j := uint32(0); j < nTasks; j++ {
			taskInfo := createTaskInfo(jobConfig, &jobID, j)
			taskInfo.Runtime.State = task.TaskState(j)
			taskInfo.Runtime.ConfigVersion = jobConfig.GetChangeLog().GetVersion()
			err = taskStore.CreateTaskRuntime(
				context.Background(),
				&jobID,
				j,
				taskInfo.Runtime,
				"test",
				jobConfig.GetType())
			suite.NoError(err)
			err = taskStore.UpdateTaskRuntime(
				context.Background(),
				&jobID,
				j,
				taskInfo.Runtime,
				jobConfig.GetType())
			suite.NoError(err)
		}
	}
	// List all tasks by job
	for i := 0; i < nJobs; i++ {
		tasks, err := taskStore.GetTasksForJob(context.Background(), jobIDs[i])
		suite.NoError(err)
		suite.Equal(len(tasks), 3)
		for _, task := range tasks {
			suite.Equal(task.JobId.Value, jobIDs[i].Value)
		}
	}

	// List tasks for a job in certain state
	// TODO: change the task.runtime.State to string type

	// Update task
	// List all tasks by job
	for i := 0; i < nJobs; i++ {
		tasks, err := taskStore.GetTasksForJob(context.Background(), jobIDs[i])
		suite.NoError(err)
		suite.Equal(len(tasks), 3)
		for _, task := range tasks {
			task.Runtime.Host = fmt.Sprintf("compute_%d", i)
			err = taskStore.UpdateTaskRuntime(
				context.Background(),
				task.JobId,
				task.InstanceId,
				task.Runtime,
				job.JobType_BATCH)
			suite.NoError(err)
		}
	}
	for i := 0; i < nJobs; i++ {
		tasks, err := taskStore.GetTasksForJob(context.Background(), jobIDs[i])
		suite.NoError(err)
		suite.Equal(len(tasks), 3)
		for _, task := range tasks {
			suite.Equal(fmt.Sprintf("compute_%d", i), task.Runtime.Host)
		}
	}

	// Test JobID doesnt exist
	_, err := taskStore.GetTaskForJob(
		context.Background(),
		"dummy_jobID",
		uint32(0))
	suite.Error(err)

	for i := 0; i < nJobs; i++ {
		for j := 0; j < int(nTasks); j++ {
			taskID := fmt.Sprintf("%s-%d", jobIDs[i].Value, j)
			taskInfo, err := taskStore.GetTaskByID(context.Background(), taskID)
			suite.NoError(err)
			suite.Equal(taskInfo.JobId.GetValue(), jobIDs[i].GetValue())
			suite.Equal(taskInfo.InstanceId, uint32(j))

			var taskMap map[uint32]*task.TaskInfo
			taskMap, err = taskStore.GetTaskForJob(context.Background(), jobIDs[i].GetValue(), uint32(j))
			suite.NoError(err)
			taskInfo = taskMap[uint32(j)]
			suite.Equal(taskInfo.JobId.Value, jobIDs[i].Value)
			suite.Equal(taskInfo.InstanceId, uint32(j))
		}
		// TaskID does not exist
	}
	task, err := taskStore.GetTaskByID(context.Background(), "taskdoesnotexist")
	suite.Error(err)
	suite.Nil(task)
}

/* Disable the test as we need to temporary disable LWT for C* writes. See T1176379

func (suite *CassandraStoreTestSuite) TestUpdateTaskConcurrently() {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	before := time.Now()

	suite.NoError(store.createTaskConfig(context.Background(), jobID, 0, &task.TaskConfig{}, 0))
	suite.NoError(store.CreateTaskRuntime(context.Background(), jobID, 0, &task.RuntimeInfo{}, ""))

	c := 25

	var wg sync.WaitGroup
	wg.Add(c)
	// Let `c` number of go-routines race around updating the jobID, and see that
	// eventually exactly `c` writes was done.
	for i := 0; i < c; i++ {
		go func() {
			for {
				info, err := store.getTask(context.Background(), jobID.GetValue(), 0)
				suite.NoError(err)
				if err := store.UpdateTaskRuntime(context.Background(), jobID, 0, info.Runtime); err == nil {
					wg.Done()
					return
				} else if !yarpcerrors.IsAlreadyExists(err) {
					suite.Fail(err.Error())
				}
			}
		}()
	}

	wg.Wait()

	info, err := store.getTask(context.Background(), jobID.GetValue(), 0)
	suite.NoError(err)

	suite.Equal(uint64(c), info.GetRuntime().GetRevision().GetVersion())
	suite.True(info.GetRuntime().GetRevision().UpdatedAt >= uint64(before.UnixNano()))
}
*/

func (suite *CassandraStoreTestSuite) TestTaskVersionMigration() {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	configAddOn := &models.ConfigAddOn{}

	// Create legacy task with missing version field.
	suite.NoError(
		store.CreateTaskConfig(
			context.Background(),
			jobID,
			0,
			&task.TaskConfig{},
			configAddOn,
			0,
		),
	)
	queryBuilder := store.DataStore.NewQuery()
	stmt := queryBuilder.Insert(taskRuntimeTable).
		Columns(
			"job_id",
			"instance_id").
		Values(
			jobID.GetValue(),
			0).
		IfNotExist()
	suite.NoError(store.applyStatement(context.Background(), stmt, ""))

	// Test invalid jobID error
	_, err := store.getTask(context.Background(), "invalid_jobID", 0)
	suite.Error(err)

	info, err := store.getTask(context.Background(), jobID.GetValue(), 0)
	suite.NoError(err)
	suite.Equal(uint64(0), info.GetRuntime().GetRevision().GetVersion())
	suite.Equal(uint64(0), info.GetRuntime().GetRevision().GetCreatedAt())
	suite.Equal(uint64(0), info.GetRuntime().GetRevision().GetUpdatedAt())

	before := time.Now()
	info.Runtime.Revision = &peloton.ChangeLog{
		Version:   1,
		UpdatedAt: uint64(time.Now().UnixNano()),
	}
	suite.NoError(store.UpdateTaskRuntime(
		context.Background(),
		jobID,
		0,
		info.Runtime,
		job.JobType_BATCH))

	info, err = store.getTask(context.Background(), jobID.GetValue(), 0)
	suite.NoError(err)
	suite.Equal(uint64(1), info.GetRuntime().GetRevision().GetVersion())
	suite.True(info.GetRuntime().GetRevision().UpdatedAt >= uint64(before.UnixNano()))
}

// TestGetTaskConfigs tests reading task configs(overridden and default)
func (suite *CassandraStoreTestSuite) TestGetTaskConfigs() {
	var instanceIDs []uint32
	jobID := &peloton.JobID{Value: "dummy_value"}

	// Test invalid JobID error
	_, _, err := store.GetTaskConfigs(
		context.Background(), jobID, instanceIDs, uint64(0))
	suite.Error(err)

	jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	configAddOn := &models.ConfigAddOn{
		SystemLabels: []*peloton.Label{
			{
				Key:   "peloton.job_id",
				Value: jobID.GetValue(),
			},
		},
	}

	// create default task config
	store.CreateTaskConfig(context.Background(), jobID,
		common.DefaultTaskConfigID,
		&task.TaskConfig{
			Name: "default",
		}, configAddOn,
		0)

	// create 5 tasks with versions
	for i := int64(0); i < 5; i++ {
		suite.NoError(store.CreateTaskConfig(context.Background(), jobID,
			i,
			&task.TaskConfig{
				Name: fmt.Sprintf("task-%d", i),
			}, configAddOn,
			0))
		instanceIDs = append(instanceIDs, uint32(i))
	}

	// Add new instance ID 6 which should have the detault task config
	instanceIDs = append(instanceIDs, uint32(6))

	// get the task configs
	configs, addOn, err := store.GetTaskConfigs(context.Background(), jobID, instanceIDs, uint64(0))
	suite.NoError(err)
	suite.Equal(6, len(configs))
	suite.Len(addOn.SystemLabels, len(configAddOn.SystemLabels))
	for i := 0; i < len(addOn.SystemLabels); i++ {
		suite.Equal(configAddOn.SystemLabels[i].Key, addOn.SystemLabels[i].Key)
		suite.Equal(configAddOn.SystemLabels[i].Value, addOn.SystemLabels[i].Value)
	}

	for instance, config := range configs {
		expectedName := fmt.Sprintf("task-%d", instance)
		if instance == 6 {
			// for instance ID 6 we expect the default config
			expectedName = "default"
		}
		suite.Equal(expectedName, config.Name)
	}
}

func (suite *CassandraStoreTestSuite) TestGetTaskStateSummary() {
	var taskStore storage.TaskStore
	taskStore = store
	var jobID = peloton.JobID{Value: uuid.New()}
	configAddOn := &models.ConfigAddOn{}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(2 * len(task.TaskState_name))
	err := suite.createJob(context.Background(), &jobID, jobConfig, configAddOn, "user1")
	suite.Nil(err)

	for i := uint32(0); i < uint32(2*len(task.TaskState_name)); i++ {
		taskInfo := createTaskInfo(jobConfig, &jobID, i)
		err := taskStore.CreateTaskRuntime(
			context.Background(),
			&jobID,
			i,
			taskInfo.Runtime,
			"user1",
			jobConfig.GetType())
		suite.Nil(err)
		taskInfo.Runtime.State = task.TaskState(i / 2)
		err = taskStore.UpdateTaskRuntime(
			context.Background(),
			&jobID,
			i,
			taskInfo.Runtime,
			jobConfig.GetType())
		suite.Nil(err)
	}

	taskStateSummary, err := store.GetTaskStateSummaryForJob(context.Background(), &jobID)
	suite.Nil(err)
	suite.Equal(len(taskStateSummary), len(task.TaskState_name))
	for _, state := range task.TaskState_name {
		suite.Equal(taskStateSummary[state], uint32(2))
	}
}

func (suite *CassandraStoreTestSuite) TestGetTaskByRange() {
	var taskStore storage.TaskStore
	taskStore = store
	var jobID = peloton.JobID{Value: uuid.New()}
	configAddOn := &models.ConfigAddOn{}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(100)
	err := suite.createJob(context.Background(), &jobID, jobConfig, configAddOn, "user1")
	suite.Nil(err)

	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		taskInfo := createTaskInfo(jobConfig, &jobID, i)
		err := taskStore.CreateTaskRuntime(
			context.Background(),
			&jobID,
			i,
			taskInfo.Runtime,
			"user1",
			jobConfig.GetType())
		suite.Nil(err)
	}
	suite.validateRange(&jobID, 0, 30)
	suite.validateRange(&jobID, 30, 65)
	suite.validateRange(&jobID, 60, 83)
	suite.validateRange(&jobID, 70, 97)
	suite.validateRange(&jobID, 70, 120)
}

// TestActiveJobsForRecovery tests add/get/delete jobs to active jobs table
func (suite *CassandraStoreTestSuite) TestActiveJobsForRecovery() {
	var jobStore storage.JobStore
	jobStore = store
	ctx := context.Background()
	jobIDs := []string{}
	for i := 0; i < 10; i++ {
		jobID := peloton.JobID{Value: uuid.New()}
		err := jobStore.AddActiveJob(ctx, &jobID)
		suite.NoError(err)
		jobIDs = append(jobIDs, jobID.GetValue())
	}

	actualJobIDs, err := jobStore.GetActiveJobs(ctx)
	suite.NoError(err)
	suite.Len(actualJobIDs, 10)

	for _, actualID := range actualJobIDs {
		suite.True(util.Contains(jobIDs, actualID.GetValue()))
	}

	for _, id := range jobIDs {
		err := jobStore.DeleteActiveJob(
			context.Background(), &peloton.JobID{Value: id})
		suite.NoError(err)
	}

	actualJobIDs, err = jobStore.GetActiveJobs(ctx)
	suite.NoError(err)
	suite.Len(actualJobIDs, 0)

	// Delete job id which is not present in active_jobs table
	err = jobStore.DeleteActiveJob(
		context.Background(), &peloton.JobID{Value: uuid.New()})
	suite.NoError(err)
}

func (suite *CassandraStoreTestSuite) validateRange(jobID *peloton.JobID, from, to int) {
	var taskStore storage.TaskStore
	taskStore = store
	jobConfig, _, err := store.GetJobConfig(context.Background(), jobID.GetValue())
	suite.NoError(err)

	if to > int(jobConfig.InstanceCount) {
		to = int(jobConfig.InstanceCount - 1)
	}
	r := &task.InstanceRange{
		From: uint32(from),
		To:   uint32(to),
	}
	var taskInRange map[uint32]*task.TaskInfo
	taskInRange, err = taskStore.GetTasksForJobByRange(context.Background(), jobID, r)
	suite.NoError(err)

	suite.Equal(to-from, len(taskInRange))
	for i := from; i < to; i++ {
		tID := fmt.Sprintf("%s-%d-%d", jobID.GetValue(), i, 1)
		suite.Equal(tID, *(taskInRange[uint32(i)].Runtime.MesosTaskId.Value))
	}

	var tasks []*task.TaskInfo
	tasks, n, err := taskStore.QueryTasks(context.Background(), jobID, &task.QuerySpec{
		Pagination: &query.PaginationSpec{
			Offset: uint32(from),
			Limit:  uint32(to - from + 1),
		},
	})
	suite.NoError(err)
	suite.Equal(n, jobConfig.InstanceCount)

	for i := from; i < to; i++ {
		tID := fmt.Sprintf("%s-%d-%d", jobID.GetValue(), i, 1)
		suite.Equal(tID, *(tasks[uint32(i-from)].Runtime.MesosTaskId.Value))
	}

	tasks, n, err = taskStore.QueryTasks(context.Background(), jobID, &task.QuerySpec{})
	suite.NoError(err)
	suite.Equal(jobConfig.InstanceCount, n)
	suite.Equal(int(_defaultQueryLimit), len(tasks))

	for i, t := range tasks {
		tID := fmt.Sprintf("%s-%d-%d", jobID.GetValue(), i, 1)
		suite.Equal(tID, *(t.Runtime.MesosTaskId.Value))
	}
}

// TestGetTaskRuntimesForJobByRange tests getting task runtimes for job by
// instance range
func (suite *CassandraStoreTestSuite) TestGetTaskRuntimesForJobByRange() {
	var jobID = peloton.JobID{Value: uuid.New()}
	configAddOn := &models.ConfigAddOn{}
	runtimes := make(map[uint32]*task.RuntimeInfo)
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = 5
	jobConfig.InstanceConfig = map[uint32]*task.TaskConfig{}

	for i := 0; i < 5; i++ {
		taskInfo := createTaskInfo(jobConfig, &jobID, 0)
		runtimes[uint32(i)] = taskInfo.Runtime
	}

	err := suite.createJob(
		context.Background(),
		&jobID,
		jobConfig,
		configAddOn,
		"user1",
	)
	suite.Nil(err)

	for i := 0; i < 5; i++ {
		runtimes[uint32(i)].ConfigVersion = jobConfig.GetChangeLog().
			GetVersion()
		err = store.CreateTaskRuntime(context.Background(),
			&jobID, uint32(i), runtimes[uint32(i)], "test", jobConfig.GetType())
		suite.NoError(err)
	}

	r := &task.InstanceRange{
		From: uint32(0),
		To:   uint32(3),
	}
	runtime, err := store.GetTaskRuntimesForJobByRange(
		context.Background(), &jobID, r)
	suite.NoError(err)
	suite.Equal(3, len(runtime))

	r.From = uint32(5)
	r.To = uint32(6)
	runtime, err = store.GetTaskRuntimesForJobByRange(
		context.Background(), &jobID, r)
	suite.NoError(err)
	suite.Equal(0, len(runtime))
}

func (suite *CassandraStoreTestSuite) TestCreateGetResourcePoolConfig() {
	var resourcePoolStore storage.ResourcePoolStore
	resourcePoolStore = store
	testCases := []struct {
		resourcePoolID string
		owner          string
		config         *respool.ResourcePoolConfig
		expectedErr    error
		msg            string
	}{
		{
			resourcePoolID: "first",
			owner:          "team1",
			config:         createResourcePoolConfig(),
			expectedErr:    nil,
			msg:            "testcase: create resource pool",
		},
		{
			resourcePoolID: "second",
			owner:          "",
			config:         createResourcePoolConfig(),
			expectedErr:    nil,
			msg:            "testcase: create resource pool, no owner",
		},
		{
			resourcePoolID: "",
			owner:          "team2",
			config:         createResourcePoolConfig(),
			expectedErr:    errors.New("Key may not be empty"),
			msg:            "testcase: create resource pool, no resource ID",
		},
		{
			resourcePoolID: "first",
			owner:          "team1",
			config:         createResourcePoolConfig(),
			expectedErr:    errors.New("code:already-exists message:first is not applied, item could exist already"),
			msg:            "testcase: create resource pool, duplicate ID",
		},
	}

	for _, tc := range testCases {
		actualErr := resourcePoolStore.CreateResourcePool(context.Background(),
			&peloton.ResourcePoolID{Value: tc.resourcePoolID},
			tc.config, tc.owner)
		if tc.expectedErr == nil {
			suite.Nil(actualErr, tc.msg)
		} else {
			suite.EqualError(actualErr, tc.expectedErr.Error(), tc.msg)
		}
	}

	// cleanup respools
	for _, tc := range testCases {
		if tc.expectedErr == nil {
			err := resourcePoolStore.DeleteResourcePool(context.Background(),
				&peloton.ResourcePoolID{Value: tc.resourcePoolID})
			suite.NoError(err)
		}
	}
}

// TestGetAllResourcePools tests getting all resource pools from store
func (suite *CassandraStoreTestSuite) TestGetAllResourcePools() {
	var resourcePoolStore storage.ResourcePoolStore
	resourcePoolStore = store
	nResourcePools := 2

	initResourcePools, err := resourcePoolStore.GetAllResourcePools(
		context.Background())
	suite.NoError(err)

	// todo move to setup once ^^^ issue resolves
	for i := 0; i < nResourcePools; i++ {
		resourcePoolID := &peloton.ResourcePoolID{
			Value: fmt.Sprintf("%s%d", _resPoolOwner, i)}
		resourcePoolConfig := createResourcePoolConfig()
		resourcePoolConfig.Name = resourcePoolID.Value
		err := resourcePoolStore.CreateResourcePool(context.Background(),
			resourcePoolID, resourcePoolConfig, _resPoolOwner)
		suite.NoError(err)
	}

	resourcePools, err := resourcePoolStore.GetAllResourcePools(
		context.Background())
	suite.NoError(err)
	suite.Len(resourcePools, len(initResourcePools)+nResourcePools)

	// cleanup created resource pools
	for i := 0; i < nResourcePools; i++ {
		resourcePoolID := &peloton.ResourcePoolID{
			Value: fmt.Sprintf("%s%d", _resPoolOwner, i)}
		err := resourcePoolStore.DeleteResourcePool(
			context.Background(), resourcePoolID)
		suite.NoError(err)
	}
}

// TestGetAllResourcePoolsEmptyResourcePool tests getting empty resource pool
func (suite *CassandraStoreTestSuite) TestGetAllResourcePoolsEmptyResourcePool() {
	var resourcePoolStore storage.ResourcePoolStore
	resourcePoolStore = store
	resourcePools, err := resourcePoolStore.GetAllResourcePools(context.Background())
	suite.NoError(err)
	suite.Len(resourcePools, 0)
}

func (suite *CassandraStoreTestSuite) TestUpdateResourcePool() {
	var resourcePoolStore storage.ResourcePoolStore
	resourcePoolStore = store
	resourcePoolID := &peloton.ResourcePoolID{Value: fmt.Sprintf("%s%d",
		"UpdateRespool", 1)}
	resourcePoolConfig := createResourcePoolConfig()

	resourcePoolConfig.Name = resourcePoolID.Value
	err := resourcePoolStore.CreateResourcePool(context.Background(),
		resourcePoolID, resourcePoolConfig, "Update")
	suite.NoError(err)

	resourcePoolConfig.Description = "Updated description"
	err = resourcePoolStore.UpdateResourcePool(context.Background(),
		resourcePoolID, resourcePoolConfig)
	suite.NoError(err)

	// GetAllResourcePools
	resourcePools, err := resourcePoolStore.GetAllResourcePools(context.Background())
	suite.NoError(err)
	suite.Equal("Updated description", resourcePools[resourcePoolID.Value].Description)

	// cleanup resource pool
	err = resourcePoolStore.DeleteResourcePool(
		context.Background(), resourcePoolID)
	suite.NoError(err)
}

func (suite *CassandraStoreTestSuite) TestDeleteResourcePool() {
	var resourcePoolStore storage.ResourcePoolStore
	resourcePoolStore = store
	resourcePoolID := &peloton.ResourcePoolID{Value: fmt.Sprintf("%s%d", "DeleteRespool", 1)}
	resourcePoolConfig := createResourcePoolConfig()
	resourcePoolConfig.Name = resourcePoolID.Value
	err := resourcePoolStore.CreateResourcePool(context.Background(), resourcePoolID, resourcePoolConfig, "Delete")
	suite.Nil(err)
	err = resourcePoolStore.DeleteResourcePool(context.Background(), resourcePoolID)
	suite.Nil(err)
}

func (suite *CassandraStoreTestSuite) TestJobRuntime() {
	var jobStore = store
	nTasks := 20

	// Test invalid jobID error
	_, err := jobStore.GetJobRuntime(
		context.Background(),
		"dummy_jobID")
	suite.Error(err)

	// CreateJob should create the default job runtime
	var jobID = peloton.JobID{Value: uuid.New()}
	configAddOn := &models.ConfigAddOn{}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(nTasks)
	err = suite.createJob(context.Background(), &jobID, jobConfig, configAddOn, "uber")
	suite.NoError(err)

	runtime, err := jobStore.GetJobRuntime(context.Background(), jobID.GetValue())
	suite.NoError(err)
	suite.Equal(job.JobState_INITIALIZED, runtime.State)
	suite.Equal(1, len(runtime.TaskStats))
	suite.Equal(jobConfig.InstanceCount, runtime.TaskStats[task.TaskState_INITIALIZED.String()])

	// update job runtime
	runtime.State = job.JobState_RUNNING
	runtime.TaskStats[task.TaskState_PENDING.String()] = 5
	runtime.TaskStats[task.TaskState_PLACED.String()] = 5
	runtime.TaskStats[task.TaskState_RUNNING.String()] = 5
	runtime.TaskStats[task.TaskState_SUCCEEDED.String()] = 5

	err = jobStore.UpdateJobRuntime(context.Background(), &jobID, runtime)
	suite.NoError(err)

	runtime, err = jobStore.GetJobRuntime(context.Background(), jobID.GetValue())
	suite.NoError(err)
	suite.Equal(job.JobState_RUNNING, runtime.State)
	suite.Equal(5, len(runtime.TaskStats))

	jobIds, err := store.GetJobsByStates(context.Background(), []job.JobState{job.JobState_RUNNING})
	suite.NoError(err)
	idFound := false
	for _, id := range jobIds {
		if id == jobID {
			idFound = true
		}
	}
	suite.True(idFound)

	jobIds, err = store.GetJobsByStates(context.Background(), []job.JobState{120})
	suite.NoError(err)
	suite.Equal(0, len(jobIds))

	// job not found test
	_, err = store.getTaskRuntimeRecord(
		context.Background(), uuid.New(), 0)
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))
}

func (suite *CassandraStoreTestSuite) TestJobConfig() {
	var jobStore = store
	oldInstanceCount := 20

	// CreateJob should create the default job runtime
	var jobID = peloton.JobID{Value: uuid.New()}
	configAddOn := &models.ConfigAddOn{
		SystemLabels: []*peloton.Label{
			{
				Key:   "peloton.job_id",
				Value: jobID.GetValue(),
			},
		},
	}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(oldInstanceCount)
	err := suite.createJob(context.Background(), &jobID, jobConfig, configAddOn, "uber")
	suite.NoError(err)

	jobRuntime, err := jobStore.GetJobRuntime(context.Background(), jobID.GetValue())
	suite.NoError(err)
	suite.Equal(jobConfig.InstanceCount, jobRuntime.TaskStats[task.TaskState_INITIALIZED.String()])

	jobConfig, addOn, err := jobStore.GetJobConfig(context.Background(), jobID.GetValue())
	suite.NoError(err)
	suite.Equal(uint32(oldInstanceCount), jobConfig.InstanceCount)
	suite.Len(addOn.SystemLabels, len(configAddOn.SystemLabels))
	for i := 0; i < len(configAddOn.SystemLabels); i++ {
		suite.Equal(configAddOn.SystemLabels[i].Key, addOn.SystemLabels[i].Key)
		suite.Equal(configAddOn.SystemLabels[i].Value, addOn.SystemLabels[i].Value)
	}

	// test getting all jobs in the index
	jobs, err := jobStore.GetAllJobsInJobIndex(context.Background())
	suite.NoError(err)
	var found bool
	for _, job := range jobs {
		if job.GetId().GetValue() == jobID.GetValue() {
			found = true
			break
		}
	}
	suite.Equal(true, found)

	// in production, cachedJob would take care of job runtime update
	jobRuntime.Revision.Version = uint64(2)
	jobRuntime.ConfigurationVersion = uint64(2)
	err = jobStore.UpdateJobRuntime(context.Background(), &jobID, jobRuntime)
	suite.NoError(err)

}

func (suite *CassandraStoreTestSuite) TestGetJobConfigOfDifferentVersions() {
	var jobStore = store
	oldInstanceCount := 20
	newInstanceCount := 50

	// CreateJob should create the default job runtime
	var jobID = peloton.JobID{Value: uuid.New()}
	configAddOn := &models.ConfigAddOn{
		SystemLabels: []*peloton.Label{
			{
				Key:   "peloton.job_id",
				Value: jobID.GetValue(),
			},
		},
	}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(oldInstanceCount)
	jobConfig.ChangeLog = &peloton.ChangeLog{
		Version: 0,
	}
	err := jobStore.CreateJobConfig(
		context.Background(),
		&jobID,
		jobConfig,
		configAddOn,
		jobConfig.GetChangeLog().GetVersion(),
		"test-owner")
	suite.NoError(err)

	jobConfig.ChangeLog.Version = 1
	jobConfig.InstanceCount = uint32(newInstanceCount)
	err = jobStore.CreateJobConfig(
		context.Background(),
		&jobID,
		jobConfig,
		configAddOn,
		jobConfig.GetChangeLog().GetVersion(),
		"test-owner")
	suite.NoError(err)

	jobConfig, addOn, err := jobStore.GetJobConfigWithVersion(context.Background(), jobID.GetValue(), 0)
	suite.NoError(err)
	suite.Equal(uint32(oldInstanceCount), jobConfig.InstanceCount)
	suite.Len(addOn.SystemLabels, len(configAddOn.SystemLabels))
	for i := 0; i < len(configAddOn.SystemLabels); i++ {
		suite.Equal(configAddOn.SystemLabels[i].Key, addOn.SystemLabels[i].Key)
		suite.Equal(configAddOn.SystemLabels[i].Value, addOn.SystemLabels[i].Value)
	}

	jobConfig, addOn, err = jobStore.GetJobConfigWithVersion(context.Background(), jobID.GetValue(), 1)
	suite.NoError(err)
	suite.Equal(uint32(newInstanceCount), jobConfig.InstanceCount)
	suite.Len(addOn.SystemLabels, len(configAddOn.SystemLabels))
	for i := 0; i < len(configAddOn.SystemLabels); i++ {
		suite.Equal(configAddOn.SystemLabels[i].Key, addOn.SystemLabels[i].Key)
		suite.Equal(configAddOn.SystemLabels[i].Value, addOn.SystemLabels[i].Value)
	}
}

func (suite *CassandraStoreTestSuite) TestPersistentVolumeInfo() {
	var volumeStore storage.PersistentVolumeStore
	volumeStore = store
	pv := &volume.PersistentVolumeInfo{
		Id: &peloton.VolumeID{
			Value: "volume1",
		},
		State:     volume.VolumeState_INITIALIZED,
		GoalState: volume.VolumeState_CREATED,
		JobId: &peloton.JobID{
			Value: "job",
		},
		Hostname:      "host",
		InstanceId:    uint32(0),
		SizeMB:        uint32(10),
		ContainerPath: "testpath",
	}
	err := volumeStore.CreatePersistentVolume(context.Background(), pv)
	suite.NoError(err)

	volumeID1 := &peloton.VolumeID{
		Value: "volume1",
	}
	rpv, err := volumeStore.GetPersistentVolume(context.Background(), volumeID1)
	suite.NoError(err)
	suite.Equal(rpv.Id.Value, "volume1")
	suite.Equal(rpv.State.String(), "INITIALIZED")
	suite.Equal(rpv.GoalState.String(), "CREATED")
	suite.Equal(rpv.JobId.Value, "job")
	suite.Equal(rpv.InstanceId, uint32(0))
	suite.Equal(rpv.Hostname, "host")
	suite.Equal(rpv.SizeMB, uint32(10))
	suite.Equal(rpv.ContainerPath, "testpath")

	// Verify get non-existent volume returns error.
	volumeID2 := &peloton.VolumeID{
		Value: "volume2",
	}
	_, err = volumeStore.GetPersistentVolume(context.Background(), volumeID2)
	suite.Error(err)

	rpv.State = volume.VolumeState_CREATED
	err = volumeStore.UpdatePersistentVolume(context.Background(), rpv)
	suite.NoError(err)

	// Verfy updated persistent volume info.
	rpv, err = volumeStore.GetPersistentVolume(context.Background(), volumeID1)
	suite.NoError(err)
	suite.Equal(rpv.Id.Value, "volume1")
	suite.Equal(rpv.State.String(), "CREATED")
	suite.Equal(rpv.GoalState.String(), "CREATED")
	suite.Equal(rpv.JobId.Value, "job")
	suite.Equal(rpv.InstanceId, uint32(0))
	suite.Equal(rpv.Hostname, "host")
	suite.Equal(rpv.SizeMB, uint32(10))
	suite.Equal(rpv.ContainerPath, "testpath")
}

// TestUpdate tests all job update related APIs by writing and reading
// from actual Cassandra instance. Since the state needs to be
// created in Cassandra and DB calls are not mocked, one test will be used
// test all cases.
func (suite *CassandraStoreTestSuite) TestUpdate() {
	// the job identifier
	jobID := &peloton.JobID{
		Value: uuid.New(),
	}

	// the update identifier
	updateID := &peloton.UpdateID{
		Value: uuid.New(),
	}

	// the update configuration
	updateConfig := &update.UpdateConfig{
		BatchSize: 5,
	}

	// job config versions
	jobVersion := uint64(5)
	jobPrevVersion := uint64(4)

	// opaque data
	opaque := "test"

	// update state
	state := update.State_INITIALIZED
	instancesTotal := uint32(60)
	numOfInstancesAdded := 30
	instancesAdded := make([]uint32, 0)
	instancesUpdated := make([]uint32, 0)
	for i := 0; i < int(instancesTotal); i++ {
		if i < numOfInstancesAdded {
			instancesAdded = append(instancesAdded, uint32(i))
		} else {
			instancesUpdated = append(instancesUpdated, uint32(i))
		}
	}

	// get a non-existent update
	_, err := store.GetUpdate(
		context.Background(),
		updateID,
	)
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))

	// get progress of a non-existent update
	_, err = store.GetUpdateProgress(
		context.Background(),
		updateID,
	)
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))

	// make sure job has no updates
	updateList, err := store.GetUpdatesForJob(context.Background(), jobID.GetValue())
	suite.NoError(err)
	suite.Equal(len(updateList), 0)

	// no workflow events present for an update
	workflowEvents, err := store.GetWorkflowEvents(
		context.Background(),
		updateID,
		0,
		0,
	)
	suite.NoError(err)
	suite.Equal(0, len(workflowEvents))

	// create a new update
	suite.NoError(store.CreateUpdate(
		context.Background(),
		&models.UpdateModel{
			UpdateID:             updateID,
			JobID:                jobID,
			UpdateConfig:         updateConfig,
			JobConfigVersion:     jobVersion,
			PrevJobConfigVersion: jobPrevVersion,
			State:                state,
			InstancesTotal:       instancesTotal,
			InstancesUpdated:     instancesUpdated,
			InstancesAdded:       instancesAdded,
			Type:                 models.WorkflowType_UPDATE,
			OpaqueData:           &peloton.OpaqueData{Data: opaque},
		},
	))

	// add workflow event for instance 0 as initialized state
	suite.NoError(store.AddWorkflowEvent(
		context.Background(),
		updateID,
		0,
		models.WorkflowType_UPDATE,
		state,
	))

	// create an update with bad updateConfig
	suite.Error(store.CreateUpdate(
		context.Background(),
		&models.UpdateModel{
			UpdateID:             updateID,
			JobID:                jobID,
			UpdateConfig:         nil,
			JobConfigVersion:     jobVersion,
			PrevJobConfigVersion: jobPrevVersion,
			State:                state,
			InstancesTotal:       instancesTotal,
			InstancesUpdated:     instancesUpdated,
			InstancesAdded:       instancesAdded,
			Type:                 models.WorkflowType_UPDATE,
			OpaqueData:           &peloton.OpaqueData{Data: opaque},
		},
	))

	// creating same update again should fail
	err = store.CreateUpdate(
		context.Background(),
		&models.UpdateModel{
			UpdateID:             updateID,
			JobID:                jobID,
			UpdateConfig:         updateConfig,
			JobConfigVersion:     jobVersion,
			PrevJobConfigVersion: jobPrevVersion,
			State:                state,
			InstancesTotal:       instancesTotal,
			InstancesUpdated:     instancesUpdated,
			InstancesAdded:       instancesAdded,
			Type:                 models.WorkflowType_UPDATE,
			OpaqueData:           &peloton.OpaqueData{Data: opaque},
		},
	)
	suite.Error(err)
	suite.True(yarpcerrors.IsAlreadyExists(err))

	// get the same update
	updateInfo, err := store.GetUpdate(
		context.Background(),
		updateID,
	)
	suite.NoError(err)
	suite.Equal(updateInfo.GetJobID().GetValue(), jobID.GetValue())
	suite.Equal(updateInfo.GetUpdateConfig().GetBatchSize(), updateConfig.GetBatchSize())
	suite.Equal(updateInfo.GetJobConfigVersion(), jobVersion)
	suite.Equal(updateInfo.GetPrevJobConfigVersion(), jobPrevVersion)
	suite.Equal(updateInfo.GetState(), state)
	suite.Equal(updateInfo.GetInstancesTotal(), instancesTotal)
	suite.Equal(updateInfo.GetInstancesDone(), uint32(0))
	suite.Equal(updateInfo.GetInstancesFailed(), uint32(0))
	suite.Equal(len(updateInfo.GetInstancesCurrent()), 0)
	suite.Equal(updateInfo.GetType(), models.WorkflowType_UPDATE)
	suite.Equal(updateInfo.GetInstancesUpdated(), instancesUpdated)
	suite.Equal(updateInfo.GetInstancesAdded(), instancesAdded)
	suite.Equal(updateInfo.GetOpaqueData().GetData(), opaque)

	// get the progress
	updateInfo, err = store.GetUpdateProgress(
		context.Background(),
		updateID,
	)
	suite.NoError(err)
	suite.Equal(updateInfo.GetState(), state)
	suite.Equal(updateInfo.GetInstancesTotal(), instancesTotal)
	suite.Equal(updateInfo.GetInstancesDone(), uint32(0))
	suite.Equal(len(updateInfo.GetInstancesCurrent()), 0)

	// TODO: Add validation for workflow event and state
	// Once update models are converged from v0 -> v1alpha
	workflowEvents, err = store.GetWorkflowEvents(
		context.Background(),
		updateID,
		0,
		0,
	)
	suite.NoError(err)
	suite.Equal(1, len(workflowEvents))

	// write new progress
	prevState := update.State_INITIALIZED
	state = update.State_ROLLING_FORWARD
	opaqueNew := "new_test"
	instancesDone := uint32(5)
	instancesFailed := uint32(6)
	instanceCurrent := []uint32{5, 6, 7, 8}
	err = store.WriteUpdateProgress(
		context.Background(),
		&models.UpdateModel{
			UpdateID:         updateID,
			PrevState:        prevState,
			State:            state,
			InstancesDone:    instancesDone,
			InstancesFailed:  instancesFailed,
			InstancesCurrent: instanceCurrent,
			OpaqueData:       &peloton.OpaqueData{Data: opaqueNew},
		},
	)
	suite.NoError(err)

	// for instance 0 add workflow operation event
	suite.NoError(store.AddWorkflowEvent(
		context.Background(),
		updateID,
		0,
		models.WorkflowType_UPDATE,
		update.State_ROLLING_FORWARD))

	// get the update
	updateInfo, err = store.GetUpdate(
		context.Background(),
		updateID,
	)
	suite.NoError(err)
	suite.Equal(updateInfo.GetJobID().GetValue(), jobID.GetValue())
	suite.Equal(updateInfo.GetUpdateConfig().GetBatchSize(), updateConfig.GetBatchSize())
	suite.Equal(updateInfo.GetJobConfigVersion(), jobVersion)
	suite.Equal(updateInfo.GetPrevJobConfigVersion(), jobPrevVersion)
	suite.Equal(updateInfo.GetState(), state)
	suite.Equal(updateInfo.GetPrevState(), prevState)
	suite.Equal(updateInfo.GetInstancesTotal(), instancesTotal)
	suite.Equal(updateInfo.GetInstancesDone(), instancesDone)
	suite.Equal(updateInfo.GetInstancesFailed(), instancesFailed)
	suite.Equal(updateInfo.GetInstancesCurrent(), instanceCurrent)
	suite.Equal(updateInfo.GetOpaqueData().GetData(), opaqueNew)

	// get the progress
	updateInfo, err = store.GetUpdateProgress(
		context.Background(),
		updateID,
	)
	suite.NoError(err)
	suite.Equal(updateInfo.GetState(), state)
	suite.Equal(updateInfo.GetPrevState(), prevState)
	suite.Equal(updateInfo.GetInstancesTotal(), instancesTotal)
	suite.Equal(updateInfo.GetInstancesDone(), instancesDone)
	suite.Equal(updateInfo.GetInstancesFailed(), instancesFailed)
	suite.Equal(updateInfo.GetInstancesCurrent(), instanceCurrent)

	workflowEvents, err = store.GetWorkflowEvents(
		context.Background(),
		updateID,
		0,
		0,
	)
	suite.NoError(err)
	suite.Equal(2, len(workflowEvents))

	// get only one workflow event
	workflowEvents, err = store.GetWorkflowEvents(
		context.Background(),
		updateID,
		0,
		1,
	)
	suite.NoError(err)
	suite.Equal(1, len(workflowEvents))

	suite.NoError(store.deleteWorkflowEvents(context.Background(), updateID, 0))

	workflowEvents, err = store.GetWorkflowEvents(
		context.Background(),
		updateID,
		0,
		0,
	)
	suite.NoError(err)
	suite.Equal(0, len(workflowEvents))

	// fetch update for job
	updateList, err = store.GetUpdatesForJob(context.Background(), jobID.GetValue())
	suite.NoError(err)
	suite.Equal(len(updateList), 1)
	suite.Equal(updateList[0].GetValue(), updateID.GetValue())

	// create 15 updates
	count := 15
	for i := 0; i < count; i++ {
		id := &peloton.UpdateID{
			Value: uuid.New(),
		}
		suite.NoError(store.CreateUpdate(
			context.Background(),
			&models.UpdateModel{
				UpdateID:             id,
				JobID:                jobID,
				UpdateConfig:         updateConfig,
				JobConfigVersion:     jobVersion,
				PrevJobConfigVersion: jobPrevVersion,
				State:                state,
				InstancesTotal:       instancesTotal,
			},
		))
	}

	// Create update to increase #instances 60 -> 120
	state = update.State_INITIALIZED
	instancesTotal = uint32(120)
	numOfInstancesAdded = 60
	instancesAdded = make([]uint32, 0)
	for i := 60; i < int(instancesTotal); i++ {
		instancesAdded = append(instancesAdded, uint32(i))
	}

	updateID = &peloton.UpdateID{
		Value: uuid.New(),
	}
	suite.NoError(store.CreateUpdate(
		context.Background(),
		&models.UpdateModel{
			UpdateID:             updateID,
			JobID:                jobID,
			UpdateConfig:         updateConfig,
			JobConfigVersion:     jobVersion,
			PrevJobConfigVersion: jobPrevVersion,
			State:                state,
			InstancesTotal:       instancesTotal,
			InstancesAdded:       instancesAdded,
			Type:                 models.WorkflowType_UPDATE,
		},
	))

	updateInfo, err = store.GetUpdate(
		context.Background(),
		updateID,
	)
	suite.NoError(err)
	suite.Equal(updateInfo.GetJobID().GetValue(), jobID.GetValue())
	suite.Equal(updateInfo.GetUpdateConfig().GetBatchSize(), updateConfig.GetBatchSize())
	suite.Equal(updateInfo.GetJobConfigVersion(), jobVersion)
	suite.Equal(updateInfo.GetPrevJobConfigVersion(), jobPrevVersion)
	suite.Equal(updateInfo.GetState(), state)
	suite.Equal(updateInfo.GetInstancesTotal(), instancesTotal)
	suite.Equal(updateInfo.GetInstancesAdded(), instancesAdded)

	// Create update to decrease #instances 120 -> 60
	state = update.State_INITIALIZED
	prevInstancesTotal := instancesTotal
	instancesTotal = uint32(60)
	instancesRemoved := make([]uint32, 0)
	for i := instancesTotal; i < prevInstancesTotal; i++ {
		instancesRemoved = append(instancesRemoved, uint32(i))
	}
	updateID = &peloton.UpdateID{
		Value: uuid.New(),
	}

	suite.NoError(store.CreateUpdate(
		context.Background(),
		&models.UpdateModel{
			UpdateID:             updateID,
			JobID:                jobID,
			UpdateConfig:         updateConfig,
			JobConfigVersion:     jobVersion,
			PrevJobConfigVersion: jobPrevVersion,
			State:                state,
			InstancesTotal:       instancesTotal,
			InstancesRemoved:     instancesRemoved,
			Type:                 models.WorkflowType_UPDATE,
		},
	))

	updateInfo, err = store.GetUpdate(
		context.Background(),
		updateID,
	)
	suite.NoError(err)
	suite.Equal(updateInfo.GetJobID().GetValue(), jobID.GetValue())
	suite.Equal(updateInfo.GetUpdateConfig().GetBatchSize(), updateConfig.GetBatchSize())
	suite.Equal(updateInfo.GetJobConfigVersion(), jobVersion)
	suite.Equal(updateInfo.GetPrevJobConfigVersion(), jobPrevVersion)
	suite.Equal(updateInfo.GetState(), state)
	suite.Equal(updateInfo.GetInstancesTotal(), instancesTotal)
	suite.Equal(updateInfo.GetInstancesRemoved(), instancesRemoved)

	// delete the first update
	err = store.DeleteUpdate(context.Background(), updateID, jobID, jobVersion)
	suite.NoError(err)

	// update is deleted with its workflow events
	workflowEvents, err = store.GetWorkflowEvents(
		context.Background(),
		updateID,
		0,
		0,
	)
	suite.NoError(err)
	suite.Equal(0, len(workflowEvents))

	// delete the job
	store.DeleteJob(context.Background(), jobID.GetValue())
	suite.NoError(deleteJobIndex(context.Background(), jobID))

	// make sure update is not found
	_, err = store.GetUpdate(
		context.Background(),
		updateID,
	)
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))

	_, err = store.GetUpdateProgress(
		context.Background(),
		updateID,
	)
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))
}

// TestModifyUpdate tests ModifyUpdate call
func (suite *CassandraStoreTestSuite) TestModifyUpdate() {
	// the job identifier
	jobID := &peloton.JobID{
		Value: uuid.New(),
	}

	// the update identifier
	updateID := &peloton.UpdateID{
		Value: uuid.New(),
	}

	// the update configuration
	updateConfig := &update.UpdateConfig{
		BatchSize: 5,
	}

	// job config versions
	jobVersion := uint64(5)
	jobPrevVersion := uint64(4)

	// update state
	state := update.State_INITIALIZED
	instancesTotal := uint32(60)
	numOfInstancesAdded := 30
	instancesAdded := make([]uint32, 0)
	instancesUpdated := make([]uint32, 0)
	for i := 0; i < int(instancesTotal); i++ {
		if i < numOfInstancesAdded {
			instancesAdded = append(instancesAdded, uint32(i))
		} else {
			instancesUpdated = append(instancesUpdated, uint32(i))
		}
	}

	// add INITIALIZED update state to job update events
	suite.NoError(store.AddJobUpdateEvent(
		context.Background(),
		updateID,
		models.WorkflowType_UPDATE,
		state,
	))

	// create a new update
	suite.NoError(store.CreateUpdate(
		context.Background(),
		&models.UpdateModel{
			UpdateID:             updateID,
			JobID:                jobID,
			UpdateConfig:         updateConfig,
			JobConfigVersion:     jobVersion,
			PrevJobConfigVersion: jobPrevVersion,
			State:                state,
			InstancesTotal:       instancesTotal,
			InstancesUpdated:     instancesUpdated,
			InstancesAdded:       instancesAdded,
			Type:                 models.WorkflowType_UPDATE,
		},
	))

	// add ROLLING_BACKWARD update state to job update events
	suite.NoError(store.AddJobUpdateEvent(
		context.Background(),
		updateID,
		models.WorkflowType_UPDATE,
		update.State_ROLLING_BACKWARD,
	))

	suite.NoError(store.ModifyUpdate(
		context.Background(),
		&models.UpdateModel{
			UpdateID:             updateID,
			JobID:                jobID,
			JobConfigVersion:     jobVersion + 1,
			PrevJobConfigVersion: jobVersion,
			InstancesDone:        0,
			InstancesFailed:      0,
			InstancesCurrent:     []uint32{},
			InstancesAdded:       []uint32{},
			InstancesRemoved:     instancesAdded,
			State:                update.State_ROLLING_BACKWARD,
			PrevState:            state,
		},
	),
	)

	updateResult, err := store.GetUpdate(context.Background(), updateID)
	suite.NoError(err)
	suite.Empty(updateResult.GetInstancesCurrent())
	suite.Empty(updateResult.GetInstancesAdded())
	suite.NotEmpty(updateResult.GetInstancesRemoved())
	suite.Equal(updateResult.GetState(), update.State_ROLLING_BACKWARD)
	suite.Equal(updateResult.GetPrevState(), state)
	suite.Equal(updateResult.GetJobConfigVersion(), jobVersion+1)
	suite.Equal(updateResult.GetPrevJobConfigVersion(), jobVersion)

	// Get job update events, events are in descending create timestamp order
	jobUpdateEvents, err := store.GetJobUpdateEvents(
		context.Background(),
		updateID)
	suite.NoError(err)
	suite.Equal(2, len(jobUpdateEvents))
	suite.Equal(stateless.WorkflowState_WORKFLOW_STATE_ROLLING_BACKWARD,
		jobUpdateEvents[0].GetState())
	suite.Equal(stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED,
		jobUpdateEvents[1].GetState())

	// delete update
	suite.NoError(store.DeleteUpdate(
		context.Background(),
		updateID,
		jobID,
		jobVersion+1,
	))

	// delete the job
	store.DeleteJob(context.Background(), jobID.GetValue())
	suite.NoError(deleteJobIndex(context.Background(), jobID))

	updateResult, err = store.GetUpdate(context.Background(), updateID)
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))

	// job update events are deleted as well with update
	jobUpdateEvents, err = store.GetJobUpdateEvents(
		context.Background(),
		updateID)
	suite.NoError(err)
	suite.Equal(0, len(jobUpdateEvents))
}

func createJobConfig() *job.JobConfig {
	var sla = job.SlaConfig{
		Priority:                22,
		MaximumRunningInstances: 6,
		Preemptible:             false,
	}
	var jobConfig = job.JobConfig{
		Name:          "uber",
		OwningTeam:    "uber",
		LdapGroups:    []string{"money", "team6", "otto"},
		SLA:           &sla,
		InstanceCount: uint32(6),
		DefaultConfig: &task.TaskConfig{},
	}
	return &jobConfig
}

func createTaskInfo(
	jobConfig *job.JobConfig, jobID *peloton.JobID, i uint32) *task.TaskInfo {

	var tID = fmt.Sprintf("%s-%d-%d", jobID.GetValue(), i, 1)
	var taskInfo = task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			PrevMesosTaskId:    nil,
			MesosTaskId:        &mesos.TaskID{Value: &tID},
			DesiredMesosTaskId: &mesos.TaskID{Value: &tID},
			State:              task.TaskState_INITIALIZED,
			Revision: &peloton.ChangeLog{
				Version: 1,
			},
		},
		Config:     jobConfig.GetDefaultConfig(),
		InstanceId: uint32(i),
		JobId:      jobID,
	}
	return &taskInfo
}

// Returns mock resource pool config
func createResourcePoolConfig() *respool.ResourcePoolConfig {
	return &respool.ResourcePoolConfig{
		Name:        "TestResourcePool_1",
		ChangeLog:   nil,
		Description: "test resource pool",
		LdapGroups:  []string{"l1", "l2"},
		OwningTeam:  "team1",
		Parent:      nil,
		Policy:      1,
		Resources:   createResourceConfigs(),
	}
}

// Returns mock list of resource configs
func createResourceConfigs() []*respool.ResourceConfig {
	return []*respool.ResourceConfig{
		{
			Kind:        "cpu",
			Limit:       1000.0,
			Reservation: 100.0,
			Share:       1.0,
		},
		{
			Kind:        "gpu",
			Limit:       4.0,
			Reservation: 2.0,
			Share:       1.0,
		},
	}
}

func (suite *CassandraStoreTestSuite) TestGetTaskRuntime() {
	//Test invalid JobID error
	_, err := store.GetTaskRuntime(
		context.Background(),
		&peloton.JobID{Value: "dummy_jobID"},
		0)
	suite.Error(err)

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	configAddOn := &models.ConfigAddOn{}
	var tID = fmt.Sprintf("%s-%d-%d", jobID.GetValue(), 0, 1)
	suite.NoError(store.CreateTaskConfig(context.Background(), jobID, 0, &task.TaskConfig{}, configAddOn, 0))
	suite.NoError(store.CreateTaskRuntime(
		context.Background(),
		jobID,
		0,
		&task.RuntimeInfo{
			PrevMesosTaskId:    nil,
			MesosTaskId:        &mesos.TaskID{Value: &tID},
			DesiredMesosTaskId: &mesos.TaskID{Value: &tID},
		},
		"",
		job.JobType_BATCH))

	info, err := store.getTask(context.Background(), jobID.GetValue(), 0)
	suite.NoError(err)

	runtime, err := store.GetTaskRuntime(context.Background(), jobID, 0)
	suite.NoError(err)

	suite.Equal(info.Runtime, runtime)
}

func (suite *CassandraStoreTestSuite) TestTaskQueryFilter() {
	var taskStore storage.TaskStore
	taskStore = store
	var jobID = peloton.JobID{Value: uuid.New()}
	configAddOn := &models.ConfigAddOn{}
	runtimes := make(map[uint32]*task.RuntimeInfo)
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = 100
	jobConfig.InstanceConfig = map[uint32]*task.TaskConfig{}

	hosts := []string{"host0", "host1", "host2", "host3"}

	for i := 0; i < 100; i++ {
		taskInfo := createTaskInfo(jobConfig, &jobID, 0)
		taskInfo.Config = &task.TaskConfig{Name: fmt.Sprintf("task_%d", i)}
		jobConfig.InstanceConfig[uint32(i)] = taskInfo.Config
		taskInfo.Runtime.State = task.TaskState(i % 16)
		taskInfo.Runtime.Host = hosts[i%4]
		runtimes[uint32(i)] = taskInfo.Runtime
	}

	err := suite.createJob(context.Background(), &jobID, jobConfig, configAddOn, "user1")
	suite.Nil(err)

	for i := 0; i < 100; i++ {
		runtimes[uint32(i)].ConfigVersion = jobConfig.GetChangeLog().GetVersion()
		err = taskStore.CreateTaskRuntime(context.Background(),
			&jobID, uint32(i), runtimes[uint32(i)], "test", jobConfig.GetType())
		suite.NoError(err)
	}

	// Test invalid jobID error
	_, _, err = taskStore.QueryTasks(
		context.Background(),
		&peloton.JobID{Value: "dummy_jobID"},
		&task.QuerySpec{
			TaskStates: []task.TaskState{task.TaskState(task.TaskState_PENDING)},
		})
	suite.Error(err)

	// testing filtering on state
	tasks, _, err := taskStore.QueryTasks(context.Background(), &jobID, &task.QuerySpec{
		TaskStates: []task.TaskState{task.TaskState(task.TaskState_PENDING)},
	})
	suite.Nil(err)
	suite.Equal(len(tasks), 7)

	// testing filtering on state
	tasks, _, err = taskStore.QueryTasks(context.Background(), &jobID, &task.QuerySpec{
		TaskStates: []task.TaskState{task.TaskState(task.TaskState_RUNNING)},
	})
	suite.Nil(err)
	suite.Equal(len(tasks), 6)

	// testing filtering on name
	tasks, _, err = taskStore.QueryTasks(context.Background(), &jobID, &task.QuerySpec{
		Names: []string{"task_1"},
	})
	suite.Nil(err)
	suite.Equal(1, len(tasks))
	suite.Equal(uint32(1), tasks[0].InstanceId)

	// testing filtering on name and host
	tasks, _, err = taskStore.QueryTasks(context.Background(), &jobID, &task.QuerySpec{
		Names: []string{"task_2"},
		Hosts: []string{"host2"},
	})
	suite.Nil(err)
	suite.Equal(1, len(tasks))
	suite.Equal(uint32(2), tasks[0].InstanceId)

	tasks, _, err = taskStore.QueryTasks(context.Background(), &jobID, &task.QuerySpec{
		Names: []string{"task_1"},
		Hosts: []string{"Host2"},
	})
	suite.Nil(err)
	suite.Equal(0, len(tasks))

	// testing filtering state and name
	tasks, _, err = taskStore.QueryTasks(context.Background(), &jobID, &task.QuerySpec{
		TaskStates: []task.TaskState{task.TaskState(task.TaskState_PLACED)},
		Names:      []string{"task_5"},
	})
	suite.Nil(err)
	suite.Equal(1, len(tasks))
	suite.Equal(uint32(5), tasks[0].InstanceId)

	// testing filtering state and host
	tasks, _, err = taskStore.QueryTasks(context.Background(), &jobID, &task.QuerySpec{
		TaskStates: []task.TaskState{task.TaskState(task.TaskState_LOST)},
		Hosts:      []string{"host3"},
	})
	suite.Nil(err)
	suite.Equal(6, len(tasks))

}

func (suite *CassandraStoreTestSuite) TestQueryTasks() {
	var taskStore storage.TaskStore
	taskStore = store
	var jobID = peloton.JobID{Value: uuid.New()}
	configAddOn := &models.ConfigAddOn{}
	runtimes := make(map[uint32]*task.RuntimeInfo)
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(len(task.TaskState_name))
	err := suite.createJob(context.Background(), &jobID, jobConfig, configAddOn, "user1")
	suite.Nil(err)

	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		taskInfo := createTaskInfo(jobConfig, &jobID, i)
		taskInfo.Runtime.ConfigVersion = jobConfig.GetChangeLog().GetVersion()
		err := taskStore.CreateTaskRuntime(
			context.Background(),
			&jobID,
			i,
			taskInfo.Runtime,
			"user1",
			jobConfig.GetType())
		suite.Nil(err)

		taskInfo.Runtime.State = task.TaskState(i)
		taskInfo.Runtime.StartTime = time.Now().Add(time.Duration(i) * time.Minute).Format(time.RFC3339)
		runtimes[i] = taskInfo.Runtime
		err = taskStore.UpdateTaskRuntime(
			context.Background(), &jobID, i,
			runtimes[i], jobConfig.GetType())
		suite.NoError(err)
	}

	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		tasks, _, err := taskStore.QueryTasks(context.Background(), &jobID, &task.QuerySpec{
			TaskStates: []task.TaskState{task.TaskState(i)},
		})
		suite.Nil(err)

		suite.Equal(1, len(tasks))
		suite.Equal(task.TaskState(i), tasks[0].GetRuntime().GetState())
	}

	for i := uint32(0); i < jobConfig.InstanceCount-1; i += 2 {
		tasks, _, err := taskStore.QueryTasks(context.Background(), &jobID, &task.QuerySpec{
			TaskStates: []task.TaskState{task.TaskState(i), task.TaskState(i + 1)},
			Pagination: &query.PaginationSpec{
				Offset: 0,
				Limit:  2,
				OrderBy: []*query.OrderBy{
					{
						Order: query.OrderBy_DESC,
						Property: &query.PropertyPath{
							Value: "state",
						},
					},
				},
			},
		})
		suite.Nil(err)

		suite.Equal(2, len(tasks))
		suite.Equal(task.TaskState(i), tasks[1].GetRuntime().GetState())
		suite.Equal(task.TaskState(i+1), tasks[0].GetRuntime().GetState())
	}

	// testing invalid sorting field
	tasks, _, err := taskStore.QueryTasks(context.Background(), &jobID, &task.QuerySpec{
		Pagination: &query.PaginationSpec{
			Offset: 0,
			Limit:  2,
			OrderBy: []*query.OrderBy{
				{
					Order: query.OrderBy_DESC,
					Property: &query.PropertyPath{
						Value: "dummy_field",
					},
				},
			},
		},
	})
	suite.Nil(tasks)
	suite.NotNil(err)

	// testing sorting by state
	tasks, _, err = taskStore.QueryTasks(context.Background(), &jobID, &task.QuerySpec{
		Pagination: &query.PaginationSpec{
			Limit: 17,
			OrderBy: []*query.OrderBy{
				{
					Order: query.OrderBy_DESC,
					Property: &query.PropertyPath{
						Value: stateField,
					},
				},
			},
		},
	})

	for i := 1; i < len(tasks); i++ {
		suite.Equal(tasks[i-1].Runtime.State > tasks[i].Runtime.State, true)
	}

	// testing sorting by time
	tasks, _, err = taskStore.QueryTasks(context.Background(), &jobID, &task.QuerySpec{
		Pagination: &query.PaginationSpec{
			Limit: 17,
			OrderBy: []*query.OrderBy{
				{
					Order: query.OrderBy_DESC,
					Property: &query.PropertyPath{
						Value: creationTimeField,
					},
				},
			},
		},
	})

	for i := 0; i < len(tasks); i++ {
		suite.Equal(uint32(len(tasks)-i-1), tasks[i].InstanceId)
	}

	suite.NoError(taskStore.DeleteTaskRuntime(context.Background(), &jobID, uint32(0)))
}

// Test task query with sort by host
func (suite *CassandraStoreTestSuite) TestQueryTasksSortByHost() {
	var jobID = peloton.JobID{Value: uuid.New()}
	var taskStore = suite.createTasksForSortBy(jobID)
	tasks, _, err := taskStore.QueryTasks(context.Background(), &jobID, suite.prepareQuerySpec([]string{hostField}))
	suite.Nil(err)

	for i := 1; i < 100; i++ {
		suite.Equal(tasks[i-1].GetRuntime().GetHost() > tasks[i].GetRuntime().GetHost(), true)
	}
	suite.NoError(taskStore.DeleteTaskRuntime(context.Background(), &jobID, uint32(0)))
}

// Test task query with sort by InstanceID
func (suite *CassandraStoreTestSuite) TestQueryTasksSortByInstanceID() {
	var jobID = peloton.JobID{Value: uuid.New()}
	var taskStore = suite.createTasksForSortBy(jobID)
	tasks, _, err := taskStore.QueryTasks(context.Background(), &jobID, suite.prepareQuerySpec([]string{instanceIDField}))
	suite.Nil(err)

	for i := 1; i < 100; i++ {
		suite.Equal(tasks[i-1].GetInstanceId() > tasks[i].GetInstanceId(), true)
	}
	suite.NoError(taskStore.DeleteTaskRuntime(context.Background(), &jobID, uint32(0)))
}

// Test task query with sort by message
func (suite *CassandraStoreTestSuite) TestQueryTasksSortByMessage() {
	var jobID = peloton.JobID{Value: uuid.New()}
	var taskStore = suite.createTasksForSortBy(jobID)
	tasks, _, err := taskStore.QueryTasks(context.Background(), &jobID, suite.prepareQuerySpec([]string{messageField}))
	suite.Nil(err)

	for i := 1; i < 100; i++ {
		suite.Equal(tasks[i-1].GetRuntime().GetMessage() > tasks[i].GetRuntime().GetMessage(), true)
	}
	suite.NoError(taskStore.DeleteTaskRuntime(context.Background(), &jobID, uint32(0)))
}

// Test task query with sort by name
func (suite *CassandraStoreTestSuite) TestQueryTasksSortByName() {
	var jobID = peloton.JobID{Value: uuid.New()}
	var taskStore = suite.createTasksForSortBy(jobID)
	tasks, _, err := taskStore.QueryTasks(context.Background(), &jobID, suite.prepareQuerySpec([]string{nameField}))
	suite.Nil(err)

	for i := 1; i < 100; i++ {
		suite.Equal(tasks[i-1].GetConfig().GetName() > tasks[i].GetConfig().GetName(), true)
	}
	suite.NoError(taskStore.DeleteTaskRuntime(context.Background(), &jobID, uint32(0)))
}

// Test task query with sort by reason
func (suite *CassandraStoreTestSuite) TestQueryTasksSortByReason() {
	var jobID = peloton.JobID{Value: uuid.New()}
	var taskStore = suite.createTasksForSortBy(jobID)
	tasks, _, err := taskStore.QueryTasks(context.Background(), &jobID, suite.prepareQuerySpec([]string{reasonField}))
	suite.Nil(err)

	for i := 1; i < 100; i++ {
		suite.Equal(tasks[i-1].GetRuntime().GetReason() > tasks[i].GetRuntime().GetReason(), true)
	}
	suite.NoError(taskStore.DeleteTaskRuntime(context.Background(), &jobID, uint32(0)))
}

// sortedByList has name and reason, since name of each task is the same(empty) in this TC, it should use second sortBy reason to compare instead
func (suite *CassandraStoreTestSuite) TestQueryTasksSortByNameReasonEmptyName() {
	var jobID = peloton.JobID{Value: uuid.New()}
	var taskStore storage.TaskStore
	taskStore = store

	configAddOn := &models.ConfigAddOn{}
	runtimes := make(map[uint32]*task.RuntimeInfo)
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = 100
	jobConfig.InstanceConfig = map[uint32]*task.TaskConfig{}

	for i := 0; i < 100; i++ {
		taskInfo := createTaskInfo(jobConfig, &jobID, 0)
		taskInfo.Config = &task.TaskConfig{}
		jobConfig.InstanceConfig[uint32(i)] = taskInfo.Config
		taskInfo.Runtime.Reason = fmt.Sprintf("REASON_COMMAND_EXECUTOR_FAILED_%d", i)
		runtimes[uint32(i)] = taskInfo.Runtime
	}

	err := suite.createJob(context.Background(), &jobID, jobConfig, configAddOn, "user1")
	suite.Nil(err)

	for i := 0; i < 100; i++ {
		runtimes[uint32(i)].ConfigVersion = jobConfig.GetChangeLog().GetVersion()
		err = taskStore.CreateTaskRuntime(context.Background(),
			&jobID, uint32(i), runtimes[uint32(i)], "test", jobConfig.GetType())
		suite.NoError(err)
	}

	// testing sorting by message and name
	tasks, _, err := taskStore.QueryTasks(context.Background(), &jobID, suite.prepareQuerySpec([]string{reasonField, nameField}))

	for i := 1; i < len(tasks); i++ {
		suite.Equal(tasks[i-1].GetRuntime().GetReason() > tasks[i].GetRuntime().GetReason(), true)
		suite.Equal(tasks[i-1].GetConfig().GetName() == tasks[i].GetConfig().GetName(), true)
	}
	suite.NoError(taskStore.DeleteTaskRuntime(context.Background(), &jobID, uint32(0)))
}

func (suite *CassandraStoreTestSuite) prepareQuerySpec(sortTypeArr []string) *task.QuerySpec {
	queryOrderByList := make([]*query.OrderBy, len(sortTypeArr))
	for i := 0; i < len(sortTypeArr); i++ {
		queryOrderByList[i] = &query.OrderBy{
			Order: query.OrderBy_DESC,
			Property: &query.PropertyPath{
				Value: sortTypeArr[i],
			},
		}
	}
	return &task.QuerySpec{
		Pagination: &query.PaginationSpec{
			Limit:   100,
			OrderBy: queryOrderByList,
		},
	}
}

func (suite *CassandraStoreTestSuite) createTasksForSortBy(jobID peloton.JobID) storage.TaskStore {
	var taskStore storage.TaskStore
	taskStore = store

	configAddOn := &models.ConfigAddOn{}
	runtimes := make(map[uint32]*task.RuntimeInfo)
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = 100
	jobConfig.InstanceConfig = map[uint32]*task.TaskConfig{}

	for i := 0; i < 100; i++ {
		taskInfo := createTaskInfo(jobConfig, &jobID, 0)
		taskInfo.Config = &task.TaskConfig{Name: fmt.Sprintf("task_%d", i)}
		jobConfig.InstanceConfig[uint32(i)] = taskInfo.Config
		taskInfo.Runtime.Host = fmt.Sprintf("host_%d", i)
		taskInfo.Runtime.Reason = fmt.Sprintf("REASON_COMMAND_EXECUTOR_FAILED_%d", i)
		taskInfo.Runtime.Message = fmt.Sprintf("Container exited with status %d", i)
		runtimes[uint32(i)] = taskInfo.Runtime
	}

	err := suite.createJob(context.Background(), &jobID, jobConfig, configAddOn, "user1")
	suite.Nil(err)

	for i := 0; i < 100; i++ {
		runtimes[uint32(i)].ConfigVersion = jobConfig.GetChangeLog().GetVersion()
		err = taskStore.CreateTaskRuntime(context.Background(),
			&jobID, uint32(i), runtimes[uint32(i)], "test", jobConfig.GetType())
		suite.NoError(err)
	}

	return taskStore
}

func (suite *CassandraStoreTestSuite) TestPodEvents() {
	hostName := "mesos-slave-01"
	testTable := []struct {
		mesosTaskID        string
		prevMesosTaskID    string
		desiredMesosTaskID string
		actualState        task.TaskState
		goalState          task.TaskState
		jobID              peloton.JobID
		healthy            task.HealthState
		returnErr          bool
	}{
		{
			mesosTaskID:        "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1",
			prevMesosTaskID:    "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1a37d6ee-5da1-4d7a-9e91-91185990fbb1",
			desiredMesosTaskID: "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1",
			actualState:        task.TaskState_PENDING,
			goalState:          task.TaskState_RUNNING,
			jobID:              peloton.JobID{Value: uuid.NewRandom().String()},
			healthy:            task.HealthState_DISABLED,
			returnErr:          true,
		},
		{
			mesosTaskID:        "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-test",
			prevMesosTaskID:    "",
			desiredMesosTaskID: "",
			actualState:        task.TaskState_PENDING,
			goalState:          task.TaskState_RUNNING,
			jobID:              peloton.JobID{Value: uuid.NewRandom().String()},
			healthy:            task.HealthState_HEALTH_UNKNOWN,
			returnErr:          true,
		},
		{
			mesosTaskID:        "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1",
			prevMesosTaskID:    "",
			desiredMesosTaskID: "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1",
			actualState:        task.TaskState_PENDING,
			goalState:          task.TaskState_RUNNING,
			jobID:              peloton.JobID{Value: "incorrect-jobID"},
			healthy:            task.HealthState_HEALTH_UNKNOWN,
			returnErr:          true,
		},
		{
			mesosTaskID:        "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-2",
			prevMesosTaskID:    "",
			desiredMesosTaskID: "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-2",
			actualState:        task.TaskState_RUNNING,
			goalState:          task.TaskState_RUNNING,
			jobID:              peloton.JobID{Value: "incorrect-jobID"},
			healthy:            task.HealthState_HEALTHY,
			returnErr:          true,
		},
		{
			mesosTaskID:        "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1",
			prevMesosTaskID:    "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-0",
			desiredMesosTaskID: "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1",
			actualState:        task.TaskState_PENDING,
			goalState:          task.TaskState_RUNNING,
			jobID:              peloton.JobID{Value: uuid.NewRandom().String()},
			healthy:            task.HealthState_DISABLED,
			returnErr:          false,
		},
		{
			mesosTaskID:        "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1",
			prevMesosTaskID:    "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-0",
			desiredMesosTaskID: "",
			actualState:        task.TaskState_PENDING,
			goalState:          task.TaskState_RUNNING,
			jobID:              peloton.JobID{Value: uuid.NewRandom().String()},
			healthy:            task.HealthState_DISABLED,
			returnErr:          false,
		},
	}

	for _, tt := range testTable {
		runtime := &task.RuntimeInfo{
			StartTime:      time.Now().String(),
			CompletionTime: time.Now().String(),
			State:          tt.actualState,
			GoalState:      tt.goalState,
			Healthy:        tt.healthy,
			Host:           hostName,
			MesosTaskId: &mesos.TaskID{
				Value: &tt.mesosTaskID,
			},
			PrevMesosTaskId: &mesos.TaskID{
				Value: &tt.prevMesosTaskID,
			},
			DesiredMesosTaskId: &mesos.TaskID{
				Value: &tt.desiredMesosTaskID,
			},
		}
		err := store.addPodEvent(context.Background(), &tt.jobID, 0, runtime)
		if tt.returnErr {
			suite.Error(err)
		} else {
			suite.NoError(err)
		}
	}
}

func (suite *CassandraStoreTestSuite) TestGetPodEvent() {
	dummyJobID := &peloton.JobID{Value: "dummy id"}
	_, err := store.GetPodEvents(
		context.Background(),
		dummyJobID.GetValue(),
		0)
	suite.Error(err)

	jobID := &peloton.JobID{Value: "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06"}
	mesosTaskID := "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-2"
	prevMesosTaskID := "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1"
	desiredMesosTaskID := "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-2"
	runtime := &task.RuntimeInfo{
		StartTime:      time.Now().String(),
		CompletionTime: time.Now().String(),
		State:          task.TaskState_RUNNING,
		GoalState:      task.TaskState_SUCCEEDED,
		Healthy:        task.HealthState_HEALTHY,
		Host:           "mesos-slave-01",
		Message:        "",
		Reason:         "",
		MesosTaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
		PrevMesosTaskId: &mesos.TaskID{
			Value: &prevMesosTaskID,
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &desiredMesosTaskID,
		},
		ConfigVersion:        3,
		DesiredConfigVersion: 4,
	}

	store.addPodEvent(context.Background(), jobID, 0, runtime)
	podEvents, err := store.GetPodEvents(
		context.Background(),
		jobID.GetValue(),
		0,
		"7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-2")
	suite.Equal(len(podEvents), 1)
	suite.NoError(err)

	mesosTaskID = "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-3"
	prevMesosTaskID = "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-2"
	desiredMesosTaskID = "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-3"
	runtime = &task.RuntimeInfo{
		StartTime:      time.Now().String(),
		CompletionTime: time.Now().String(),
		State:          task.TaskState_RUNNING,
		GoalState:      task.TaskState_SUCCEEDED,
		Healthy:        task.HealthState_HEALTHY,
		Host:           "mesos-slave-01",
		Message:        "",
		Reason:         "",
		MesosTaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
		PrevMesosTaskId: &mesos.TaskID{
			Value: &prevMesosTaskID,
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &desiredMesosTaskID,
		},
		ConfigVersion:        3,
		DesiredConfigVersion: 4,
	}

	store.addPodEvent(context.Background(), jobID, 0, runtime)
	podEvents, err = store.GetPodEvents(
		context.Background(),
		jobID.GetValue(),
		0)
	suite.Equal(len(podEvents), 1)
	suite.NoError(err)

	podEvents, err = store.GetPodEvents(
		context.Background(),
		jobID.GetValue(),
		0,
		"7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-3")
	suite.Equal(len(podEvents), 1)
	suite.NoError(err)

	err = store.DeletePodEvents(context.Background(), jobID.GetValue(), 0, 2, 3)
	suite.NoError(err)

	podEvents, err = store.GetPodEvents(
		context.Background(),
		jobID.GetValue(),
		0)
	suite.Equal(len(podEvents), 1)
	suite.NoError(err)
}

func TestLess(t *testing.T) {
	// testing sort by state
	stateOrder := query.OrderBy{
		Order: query.OrderBy_DESC,
		Property: &query.PropertyPath{
			Value: "state",
		},
	}

	orderByList := []*query.OrderBy{&stateOrder}

	jobConfig := createJobConfig()
	taskInfo0 := createTaskInfo(jobConfig, &peloton.JobID{Value: uuid.New()}, 0)
	taskInfo2 := createTaskInfo(jobConfig, &peloton.JobID{Value: uuid.New()}, 2)

	taskInfo0.Runtime.State = task.TaskState_RUNNING
	taskInfo0.Runtime.StartTime = "2018-04-24T01:50:38Z"
	taskInfo2.Runtime.StartTime = "2018-04-24T01:40:38Z"
	taskInfo0.Config = &task.TaskConfig{Name: "taskInfo0"}
	taskInfo2.Config = &task.TaskConfig{Name: "taskInfo2"}
	taskInfo0.Runtime.Host = "taskHost0"
	taskInfo2.Runtime.Host = "taskHost2"
	taskInfo0.Runtime.Reason = "reason0"
	taskInfo2.Runtime.Reason = "reason2"

	assert.Equal(t, true, Less(orderByList, taskInfo0, taskInfo2))

	// testing sort by creation_time
	timeOrder := query.OrderBy{
		Order: query.OrderBy_ASC,
		Property: &query.PropertyPath{
			Value: creationTimeField,
		},
	}
	orderByList = []*query.OrderBy{&timeOrder}
	assert.Equal(t, Less(orderByList, taskInfo0, taskInfo2), false)

	// testing first sort by state, then creation_time
	taskInfo2.Runtime.State = task.TaskState_RUNNING
	orderByList = []*query.OrderBy{&stateOrder, &timeOrder}
	assert.Equal(t, Less(orderByList, taskInfo0, taskInfo2), false)

	/*
		| 	 			|  A  | B 	| C   |
		| id 			|  0  | 1 	| 2   |
		| creation_time | '2' | '' 	| '1' |
		Test this case to make sure
		Less(A,B)=True, Less(B,C)=False and Less(A,C)=False
	*/
	taskInfo1 := createTaskInfo(jobConfig, &peloton.JobID{Value: uuid.New()}, 1)
	taskInfo1.Runtime.StartTime = ""
	orderByList = []*query.OrderBy{&timeOrder}
	assert.Equal(t, Less(orderByList, taskInfo0, taskInfo1), true)
	assert.Equal(t, Less(orderByList, taskInfo1, taskInfo2), false)
	assert.Equal(t, Less(orderByList, taskInfo0, taskInfo2), false)

	// testing sort by instanceId
	instanceIDOrder := query.OrderBy{
		Order: query.OrderBy_DESC,
		Property: &query.PropertyPath{
			Value: instanceIDField,
		},
	}
	orderByList = []*query.OrderBy{&instanceIDOrder}
	assert.Equal(t, Less(orderByList, taskInfo0, taskInfo2), false)

	// testing sort by host
	hostOrder := query.OrderBy{
		Order: query.OrderBy_DESC,
		Property: &query.PropertyPath{
			Value: hostField,
		},
	}
	orderByList = []*query.OrderBy{&hostOrder}
	assert.Equal(t, Less(orderByList, taskInfo0, taskInfo2), false)

	// testing sort by reason
	reasonOrder := query.OrderBy{
		Order: query.OrderBy_DESC,
		Property: &query.PropertyPath{
			Value: reasonField,
		},
	}
	orderByList = []*query.OrderBy{&reasonOrder}
	assert.Equal(t, Less(orderByList, taskInfo0, taskInfo2), false)

	// testing sort by name
	nameOrder := query.OrderBy{
		Order: query.OrderBy_DESC,
		Property: &query.PropertyPath{
			Value: nameField,
		},
	}
	orderByList = []*query.OrderBy{&nameOrder}
	assert.Equal(t, Less(orderByList, taskInfo0, taskInfo2), false)
}

// TestSortedTaskInfoList tests sort functions for SortedTaskInfoList
func (suite *CassandraStoreTestSuite) TestSortedTaskInfoList() {
	l := SortedTaskInfoList{
		&task.TaskInfo{
			InstanceId: uint32(0),
		},
		&task.TaskInfo{
			InstanceId: uint32(1),
		},
	}

	suite.Equal(l.Len(), 2)
	suite.True(l.Less(0, 1))
	l.Swap(0, 1)
	suite.Equal(l[0].InstanceId, uint32(1))
}

// TestSortedUpdateList tests sort functions for SortedUpdateList
func (suite *CassandraStoreTestSuite) TestSortedUpdateList() {
	l := SortedUpdateList{
		&SortUpdateInfo{
			jobConfigVersion: uint64(0),
		},
		&SortUpdateInfo{
			jobConfigVersion: uint64(1),
		},
	}

	suite.Equal(l.Len(), 2)
	suite.True(l.Less(0, 1))
	l.Swap(0, 1)
	suite.Equal(l[0].jobConfigVersion, uint64(1))
}

// TestHandleDataStoreError tests data store error handling
func (suite *CassandraStoreTestSuite) TestHandleDataStoreError() {
	policy := backoff.NewRetryPolicy(5, 5*time.Millisecond)

	nonRetryableErrs := []error{
		gocql.RequestErrReadFailure{},
		gocql.RequestErrWriteFailure{},
		gocql.RequestErrAlreadyExists{},
		gocql.RequestErrReadTimeout{},
		gocql.RequestErrWriteTimeout{},
		gocql.ErrTooManyTimeouts,
	}
	for _, nErr := range nonRetryableErrs {
		suite.Error(store.handleDataStoreError(nErr, backoff.NewRetrier(policy)))
	}

	retryableErrs := []error{
		gocql.ErrUnavailable,
		gocql.ErrSessionClosed,
		gocql.ErrNoConnections,
		gocql.ErrConnectionClosed,
		gocql.ErrNoStreams,
	}
	for _, nErr := range retryableErrs {
		suite.NoError(store.handleDataStoreError(nErr, backoff.NewRetrier(policy)))
	}

}

func (suite *CassandraStoreTestSuite) TestCreateTaskRuntimeForServiceJob() {
	taskStore := store
	var jobID = peloton.JobID{Value: uuid.New()}
	configAddOn := &models.ConfigAddOn{}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = 1
	jobConfig.Type = job.JobType_SERVICE

	err := suite.createJob(context.Background(), &jobID, jobConfig, configAddOn, "uber")
	suite.NoError(err)

	taskInfo := createTaskInfo(jobConfig, &jobID, 0)
	taskInfo.Runtime.DesiredMesosTaskId = taskInfo.Runtime.MesosTaskId
	err = taskStore.CreateTaskRuntime(
		context.Background(),
		&jobID,
		0,
		taskInfo.Runtime,
		"test",
		jobConfig.GetType())
	suite.NoError(err)

}

func (suite *CassandraStoreTestSuite) TestGetTasksForJobError() {
	jobID := peloton.JobID{Value: "dummy_jobID"}
	_, err := store.GetTasksForJob(context.Background(), &jobID)
	suite.Error(err)
}

func (suite *CassandraStoreTestSuite) TestDeleteTaskRuntimeError() {
	suite.Error(store.DeleteTaskRuntime(context.Background(), &peloton.JobID{Value: "error"}, uint32(0)))
}

// TestCreateTaskConfigSuccess tests success case of creating the task configuration
func (suite *CassandraStoreTestSuite) TestCreateTaskConfigSuccess() {
	taskConfig := &task.TaskConfig{
		Name: testJob,
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
		RestartPolicy: &task.RestartPolicy{
			MaxFailures: 3,
		},
		HealthCheck: &task.HealthCheckConfig{
			InitialIntervalSecs:    10,
			IntervalSecs:           10,
			MaxConsecutiveFailures: 5,
			TimeoutSecs:            5,
			Enabled:                false,
		},
	}

	suite.NoError(store.CreateTaskConfig(
		context.Background(),
		&peloton.JobID{Value: testJob},
		0,
		taskConfig,
		&models.ConfigAddOn{},
		1,
	))
}

// TestTaskConfigsForLegacyJobs tests if a legacy task config be retrieved
// successfully using GetTaskConfig and GetTaskConfigs call
func (suite *CassandraStoreTestSuite) TestTaskConfigsForLegacyJobs() {
	var numTasks = 5
	instanceConfig := make(map[uint32]*task.TaskConfig)
	var jobID = peloton.JobID{Value: uuid.New()}
	for i := 0; i < numTasks; i++ {
		taskConfig := &task.TaskConfig{
			Resource: &task.ResourceConfig{
				CpuLimit:    0.8,
				MemLimitMb:  800,
				DiskLimitMb: 1500,
				FdLimit:     1000 + uint32(i),
			},
		}
		instanceConfig[uint32(i)] = taskConfig
	}
	var jobConfig = job.JobConfig{
		Name:           fmt.Sprintf("TestTaskConfigLegacy"),
		DefaultConfig:  &task.TaskConfig{},
		InstanceCount:  10,
		Type:           job.JobType_BATCH,
		InstanceConfig: instanceConfig,
	}
	// Manually create entries for the task configs in the task_config table
	// to simulate legacy jobs which don't use task_config_v2
	err := createTaskConfigsLegacy(context.Background(), &jobID, &jobConfig)
	suite.NoError(err)

	taskConfigs, _, err := store.GetTaskConfigs(
		context.Background(), &jobID, []uint32{0, 1, 2, 3, 4}, 0)
	suite.Equal(len(taskConfigs), 5)
	suite.NoError(err)

	for i := 0; i < numTasks; i++ {
		taskConfig, _, err := store.GetTaskConfig(
			context.Background(), &jobID, uint32(i), 0)
		suite.NoError(err)
		expectedCfg, ok := instanceConfig[uint32(i)]
		suite.True(ok)
		suite.Equal(*taskConfig, *expectedCfg)
	}
}
