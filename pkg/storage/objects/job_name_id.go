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

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"

	"github.com/uber/peloton/pkg/storage/objects/base"

	"github.com/gocql/gocql"
)

// init adds a JobNameToIDObject instance to the global list of storage objects
func init() {
	Objs = append(Objs, &JobNameToIDObject{})
}

// JobNameToIDObject corresponds to a row in job_name_to_id table.
type JobNameToIDObject struct {
	// DB specific annotations
	base.Object `cassandra:"name=job_name_to_id, primaryKey=((job_name), update_time)"`

	// Name of the job
	JobName string `column:"name=job_name"`
	// Update time of the job
	UpdateTime *base.OptionalString `column:"name=update_time"`
	// JobID of the job
	JobID string `column:"name=job_id"`
}

// transform will convert all the value from DB into the corresponding type
// in ORM object to be interpreted by base store client
func (o *JobNameToIDObject) transform(row map[string]interface{}) {
	o.JobID = row["job_id"].(string)
	o.JobName = row["job_name"].(string)
	o.UpdateTime = base.NewOptionalString(row["update_time"])
}

// JobNameToIDOps provides methods for manipulating job_name_to_id table.
type JobNameToIDOps interface {
	// Create inserts a row in the table.
	Create(
		ctx context.Context,
		jobName string,
		id *peloton.JobID,
	) error

	// GetAll retrieves a row from the table.
	GetAll(
		ctx context.Context,
		jobName string,
	) ([]*JobNameToIDObject, error)
}

// ensure that default implementation (JobNameToIDOps) satisfies the interface
var _ JobNameToIDOps = (*jobNameToIDOps)(nil)

// jobNameToIDOps implements JobNameToIDOps using a particular Store
type jobNameToIDOps struct {
	store *Store
}

// NewJobNameToIDOps constructs a JobNameToIDOps object for provided Store.
func NewJobNameToIDOps(s *Store) JobNameToIDOps {
	return &jobNameToIDOps{store: s}
}

// Create creates a JobNameToIDObject in db
func (d *jobNameToIDOps) Create(
	ctx context.Context,
	jobName string,
	id *peloton.JobID,
) error {

	obj := &JobNameToIDObject{
		JobName: jobName,
		JobID:   id.GetValue(),
		UpdateTime: base.NewOptionalString(gocql.UUIDFromTime(time.Now()).
			String()),
	}

	if err := d.store.oClient.Create(ctx, obj); err != nil {
		d.store.metrics.OrmJobMetrics.JobNameToIDCreateFail.Inc(1)
		return err
	}

	d.store.metrics.OrmJobMetrics.JobNameToIDCreate.Inc(1)
	return nil
}

// GetAll gets all jobIDs for a particular name from DB
func (d *jobNameToIDOps) GetAll(
	ctx context.Context,
	jobName string,
) ([]*JobNameToIDObject, error) {

	resultObjs := []*JobNameToIDObject{}

	jobNameToIDObject := &JobNameToIDObject{
		JobName: jobName,
	}

	rows, err := d.store.oClient.GetAll(ctx, jobNameToIDObject)
	if err != nil {
		d.store.metrics.OrmJobMetrics.JobNameToIDGetAllFail.Inc(1)
		return nil, err
	}

	for _, row := range rows {
		jobNameToIDObj := &JobNameToIDObject{}
		jobNameToIDObj.transform(row)
		resultObjs = append(resultObjs, jobNameToIDObj)
	}

	d.store.metrics.OrmJobMetrics.JobNameToIDGetAll.Inc(1)
	return resultObjs, nil
}
