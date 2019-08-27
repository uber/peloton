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
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/storage/objects/base"

	"github.com/gocql/gocql"
)

// init adds a JobUpdateEvents instance to the global list of storage objects
func init() {
	Objs = append(Objs, &JobUpdateEventsObject{})
}

// JobUpdateEventsObject corresponds to a row in job_update_events table.
type JobUpdateEventsObject struct {
	// base.Object DB specific annotations
	base.Object `cassandra:"name=job_update_events, primaryKey=((update_id),create_time)"`
	// UpdateID of the update (uuid)
	UpdateID string `column:"name=update_id"`
	// Type of the job update
	Type string `column:"name=type"`
	// State of the job update
	State string `column:"name=state"`
	// CreateTime of the job update events
	CreateTime *base.OptionalString `column:"name=create_time"`
}

// transform will convert all the value from DB into the corresponding type
// in ORM object to be interpreted by base store client
func (o *JobUpdateEventsObject) transform(row map[string]interface{}) {
	o.UpdateID = row["update_id"].(string)
	o.Type = row["type"].(string)
	o.State = row["state"].(string)
	o.CreateTime = base.NewOptionalString(row["create_time"])
}

// JobUpdateEventsOps provides methods for manipulating job_update_events table.
type JobUpdateEventsOps interface {
	// Create upserts single job state state change for a job.
	Create(
		ctx context.Context,
		updateID *peloton.UpdateID,
		updateType models.WorkflowType,
		updateState update.State,
	) error

	// GetAll returns job update events for an update.
	// Update state events are sorted by
	// reverse order of time of event.
	GetAll(
		ctx context.Context,
		updateID *peloton.UpdateID,
	) ([]*stateless.WorkflowEvent, error)

	// Delete deletes job update events for an update of a job
	Delete(
		ctx context.Context,
		updateID *peloton.UpdateID,
	) error
}

// ensure that default implementation (jobUpdateEventsOps) satisfies the interface
var _ JobUpdateEventsOps = (*jobUpdateEventsOps)(nil)

// jobUpdateEventsOps implements JobUpdateEventsOps using a particular Store
type jobUpdateEventsOps struct {
	store *Store
}

// NewJobUpdateEventsOps constructs a JobUpdateEventsOps object for provided Store.
func NewJobUpdateEventsOps(s *Store) JobUpdateEventsOps {
	return &jobUpdateEventsOps{store: s}
}

// Create upserts single job state state change for a job.
func (d *jobUpdateEventsOps) Create(
	ctx context.Context,
	updateID *peloton.UpdateID,
	updateType models.WorkflowType,
	updateState update.State,
) error {
	obj := &JobUpdateEventsObject{
		UpdateID:   updateID.GetValue(),
		Type:       updateType.String(),
		State:      updateState.String(),
		CreateTime: base.NewOptionalString(gocql.TimeUUID().String()),
	}

	if err := d.store.oClient.Create(ctx, obj); err != nil {
		d.store.metrics.OrmJobUpdateEventsMetrics.JobUpdateEventsCreateFail.Inc(1)
		return err
	}

	d.store.metrics.OrmJobUpdateEventsMetrics.JobUpdateEventsCreate.Inc(1)
	return nil
}

// GetAll returns job update events for an update.
// Update state events are sorted by
// reverse order of time of event.
func (d *jobUpdateEventsOps) GetAll(
	ctx context.Context,
	updateID *peloton.UpdateID,
) ([]*stateless.WorkflowEvent, error) {
	obj := &JobUpdateEventsObject{
		UpdateID: updateID.GetValue(),
	}

	rows, err := d.store.oClient.GetAll(ctx, obj)
	if err != nil {
		d.store.metrics.OrmJobUpdateEventsMetrics.JobUpdateEventsGetFail.Inc(1)
		return nil, err
	}

	var workflowEvents []*stateless.WorkflowEvent
	for _, row := range rows {
		jobUpdateEventsObjectValue := &JobUpdateEventsObject{}
		jobUpdateEventsObjectValue.transform(row)
		timeUUID, err := gocql.ParseUUID(jobUpdateEventsObjectValue.CreateTime.Value)
		if err != nil {
			d.store.metrics.OrmJobUpdateEventsMetrics.JobUpdateEventsGetFail.Inc(1)
			return nil, err
		}

		workflowEvents = append(workflowEvents, &stateless.WorkflowEvent{
			Type: stateless.WorkflowType(
				models.WorkflowType_value[jobUpdateEventsObjectValue.Type]),
			Timestamp: timeUUID.Time().Format(time.RFC3339),
			State: stateless.WorkflowState(
				update.State_value[jobUpdateEventsObjectValue.State]),
		})
	}

	d.store.metrics.OrmJobUpdateEventsMetrics.JobUpdateEventsGet.Inc(1)
	return workflowEvents, nil
}

// Delete deletes job update events for an update of a job
func (d *jobUpdateEventsOps) Delete(
	ctx context.Context,
	updateID *peloton.UpdateID,
) error {
	obj := &JobUpdateEventsObject{
		UpdateID: updateID.GetValue(),
	}

	if err := d.store.oClient.Delete(ctx, obj); err != nil {
		d.store.metrics.OrmJobUpdateEventsMetrics.JobUpdateEventsDeleteFail.Inc(1)
		return err
	}

	d.store.metrics.OrmJobUpdateEventsMetrics.JobUpdateEventsDelete.Inc(1)
	return nil
}
