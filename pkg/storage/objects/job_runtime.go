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
	"go.uber.org/yarpc/yarpcerrors"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"

	"github.com/uber/peloton/pkg/storage/objects/base"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

// init adds a JobRuntimeObject instance to the global list of storage objects
func init() {
	Objs = append(Objs, &JobRuntimeObject{})
}

// JobRuntimeObject corresponds to a row in job_config table.
type JobRuntimeObject struct {
	// DB specific annotations
	base.Object `cassandra:"name=job_runtime, primaryKey=((job_id))"`

	// JobID of the job
	JobID string `column:"name=job_id"`
	// RuntimeInfo of the job
	RuntimeInfo []byte `column:"name=runtime_info"`
	// Current state of the job
	State string `column:"name=state"`
	// Update time of the job
	UpdateTime time.Time `column:"name=update_time"`
}

// transform will convert all the value from DB into the corresponding type
// in ORM object to be interpreted by base store client
func (o *JobRuntimeObject) transform(row map[string]interface{}) {
	o.JobID = row["job_id"].(string)
	o.RuntimeInfo = row["runtime_info"].([]byte)
	o.State = row["state"].(string)
	o.UpdateTime = row["update_time"].(time.Time)
}

// JobRuntimeOps provides methods for manipulating job_config table.
type JobRuntimeOps interface {
	// Upsert inserts/updates a row in the table.
	Upsert(
		ctx context.Context,
		id *peloton.JobID,
		runtime *job.RuntimeInfo,
	) error

	// Get retrieves a row from the table.
	Get(
		ctx context.Context,
		id *peloton.JobID,
	) (*job.RuntimeInfo, error)

	// Delete removes an object from the table.
	Delete(
		ctx context.Context,
		id *peloton.JobID,
	) error
}

// ensure that default implementation (jobRuntimeOps) satisfies the interface
var _ JobRuntimeOps = (*jobRuntimeOps)(nil)

// newJobRuntimeObject creates a JobRuntimeObject from job runtime
func newJobRuntimeObject(
	id *peloton.JobID,
	runtime *job.RuntimeInfo,
) (*JobRuntimeObject, error) {
	obj := &JobRuntimeObject{JobID: id.GetValue()}

	runtimeBuffer, err := proto.Marshal(runtime)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to marshal jobRuntime")
	}

	obj.RuntimeInfo = runtimeBuffer
	obj.State = runtime.GetState().String()
	obj.UpdateTime = time.Now().UTC()
	return obj, nil
}

// jobRuntimeOps implements jobRuntimeOps using a particular Store
type jobRuntimeOps struct {
	store *Store
}

// NewJobRuntimeOps constructs a jobRuntimeOps object for provided Store.
func NewJobRuntimeOps(s *Store) JobRuntimeOps {
	return &jobRuntimeOps{store: s}
}

// Upsert creates/updates a JobRuntimeObject in db
func (d *jobRuntimeOps) Upsert(
	ctx context.Context,
	id *peloton.JobID,
	runtime *job.RuntimeInfo,
) error {

	obj, err := newJobRuntimeObject(id, runtime)
	if err != nil {
		return errors.Wrap(err, "Failed to construct JobRuntimeObject")
	}

	if err := d.store.oClient.Create(ctx, obj); err != nil {
		return err
	}

	return nil
}

// Get gets a JobRuntimeObject from db
func (d *jobRuntimeOps) Get(
	ctx context.Context,
	id *peloton.JobID,
) (*job.RuntimeInfo, error) {
	obj := &JobRuntimeObject{
		JobID: id.GetValue(),
	}

	row, err := d.store.oClient.Get(ctx, obj)
	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		return nil, yarpcerrors.NotFoundErrorf(
			"Job runtime not found %s", id.Value)
	}
	obj.transform(row)
	runtime := &job.RuntimeInfo{}
	if err := proto.Unmarshal(obj.RuntimeInfo, runtime); err != nil {
		return nil, errors.Wrap(err, "Failed to unmarshal job runtime")
	}

	return runtime, nil
}

// Delete deletes a JobRuntimeObject from db
func (d *jobRuntimeOps) Delete(
	ctx context.Context,
	id *peloton.JobID,
) error {
	obj := &JobRuntimeObject{
		JobID: id.GetValue(),
	}
	if err := d.store.oClient.Delete(ctx, obj); err != nil {
		return err
	}

	return nil
}
