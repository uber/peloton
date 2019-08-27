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

package objects

import (
	"context"
	"time"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/pkg/storage/objects/base"
)

var (
	_defaultActiveJobsShardID = 0
)

// ActiveJobsObject corresponds to a row in active_jobs table.
type ActiveJobsObject struct {
	// base.Object DB specific annotations.
	base.Object `cassandra:"name=active_jobs, primaryKey=((shard_id), job_id)"`
	// Shard id of the active job.
	ShardID *base.OptionalUInt64 `column:"name=shard_id"`
	// Job id of the active job.
	JobID *base.OptionalString `column:"name=job_id"`
}

// transform will convert all the value from DB into the corresponding type
// in ORM object to be interpreted by base store client
func (o *ActiveJobsObject) transform(row map[string]interface{}) {
	o.ShardID = base.NewOptionalUInt64(row["shard_id"])
	o.JobID = base.NewOptionalString(row["job_id"])
}

// ActiveJobsOps provides methods for manipulating active_jobs table.
type ActiveJobsOps interface {
	// Create inserts a jobID in the active_job table.
	Create(
		ctx context.Context,
		id *peloton.JobID,
	) error

	// GetAll retrieves all the job ids from the active_jobs table.
	GetAll(ctx context.Context) ([]*peloton.JobID, error)

	// Delete removes the job from the active_jobs table.
	Delete(ctx context.Context, jobID *peloton.JobID) error
}

// activeJobsOps implements ActiveJobsOps using a particular Store.
type activeJobsOps struct {
	store *Store
}

// init adds a ActiveJobsObject instance to the global list of storage objects.
func init() {
	Objs = append(Objs, &ActiveJobsObject{})
}

// newActiveJobsObject creates a ActiveJobsObject.
func newActiveJobsObject(jobID *peloton.JobID) *ActiveJobsObject {
	obj := &ActiveJobsObject{
		// active jobs table is shareded using synthetic shardIDs derived from jobID
		// This is to prevent large partitions in cassandra. We may choose to add
		// synthetic sharding later, but for now we will use just one shardID for
		// this table and recover all jobs in that one shardID. This code is for
		// future proofing.
		ShardID: base.NewOptionalUInt64(getActiveJobShardIDFromJobID(jobID)),
		JobID:   base.NewOptionalString(jobID.GetValue()),
	}
	return obj
}

// Default activeJobsOps implementation.
var _ ActiveJobsOps = (*activeJobsOps)(nil)

// NewActiveJobsOps constructs a ActiveJobsOps object for provided Store.
func NewActiveJobsOps(s *Store) ActiveJobsOps {
	return &activeJobsOps{store: s}
}

// Create adds a job to active_jobs table
func (a *activeJobsOps) Create(ctx context.Context, id *peloton.JobID) error {
	obj := newActiveJobsObject(id)
	if err := a.store.oClient.Create(ctx, obj); err != nil {
		a.store.metrics.OrmJobMetrics.ActiveJobsCreateFail.Inc(1)
		return err
	}
	a.store.metrics.OrmJobMetrics.ActiveJobsCreate.Inc(1)
	return nil
}

// GetAll returns active jobs at any given time. This means Batch jobs
// in PENDING, INITIALIZED, RUNNING, KILLING state and ALL Stateless jobs.
func (a *activeJobsOps) GetAll(ctx context.Context) ([]*peloton.JobID, error) {
	resultObjs := []*peloton.JobID{}

	callStart := time.Now()
	rows, err := a.store.oClient.GetAll(ctx, &ActiveJobsObject{})
	if err != nil {
		a.store.metrics.OrmJobMetrics.ActiveJobsGetAllFail.Inc(1)
		callDuration := time.Since(callStart)
		a.store.metrics.OrmJobMetrics.ActiveJobsGetAllDuration.Record(callDuration)
		return nil, err
	}

	for _, row := range rows {
		activeJobsObj := &ActiveJobsObject{}
		activeJobsObj.transform(row)
		jobId := activeJobsObj.JobID.Value

		// If we return an error here because of one potentially corrupt
		// job_id entry, it will break recovery and jobmgr/resmgr will be
		// thrown in a crash loop. This is a highly unlikely error, so we
		// should investigate it if we catch it on sentry without breaking
		// peloton restarts
		_, err := gocql.ParseUUID(jobId)
		if err != nil {
			log.WithField("job_id", jobId).
				Error("Invalid jobID in active jobs table")
			continue
		}

		resultObjs = append(resultObjs, &peloton.JobID{Value: jobId})
	}

	a.store.metrics.OrmJobMetrics.ActiveJobsGetAll.Inc(1)
	callDuration := time.Since(callStart)
	a.store.metrics.OrmJobMetrics.ActiveJobsGetAllDuration.Record(callDuration)
	return resultObjs, nil
}

// Delete deletes a job from active jobs table
func (a *activeJobsOps) Delete(ctx context.Context, jobID *peloton.JobID) error {
	activeJobObject := &ActiveJobsObject{
		ShardID: base.NewOptionalUInt64(getActiveJobShardIDFromJobID(jobID)),
		JobID:   base.NewOptionalString(jobID.GetValue()),
	}
	if err := a.store.oClient.Delete(ctx, activeJobObject); err != nil {
		a.store.metrics.OrmJobMetrics.ActiveJobsDeleteFail.Inc(1)
		return err
	}
	a.store.metrics.OrmJobMetrics.ActiveJobsDelete.Inc(1)
	return nil
}

// Get a list of shardIDs to query active jobs.
func getActiveJobShardIDFromJobID(jobID *peloton.JobID) uint64 {
	// This can be constructed from jobID (ex: first byte of job_id is shard id)
	// For now, we can stick to default shard id
	return uint64(_defaultActiveJobsShardID)
}
