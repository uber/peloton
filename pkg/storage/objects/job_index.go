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
	"encoding/json"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/pkg/storage/objects/base"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	_configFields = []string{
		"Name",
		"Owner",
		"RespoolID",
		"JobType",
		"Config",
		"InstanceCount",
		"Labels",
	}
	_runtimeFields = []string{
		"RuntimeInfo",
		"State",
		"UpdateTime",
		"CreationTime",
		"StartTime",
		"CompletionTime",
	}
)

// init adds a JobIndexObject instance to the global list of storage objects
func init() {
	Objs = append(Objs, &JobIndexObject{})
}

// JobIndexObject corresponds to a row in job_index table.
type JobIndexObject struct {
	// DB specific annotations
	base.Object `cassandra:"name=job_index, primaryKey=((job_id))"`

	// JobID of the job
	JobID string `column:"name=job_id"`
	// Type of job
	JobType uint32 `column:"name=job_type"`

	// Name of the job
	Name string `column:"name=name"`
	// Owner of the job
	Owner string `column:"name=owner"`
	// Resource-pool to which the job belongs
	RespoolID string `column:"name=respool_id"`

	// Configuration of the job
	Config string `column:"name=config"`
	// Number of task instances
	InstanceCount uint32 `column:"name=instance_count"`
	// Labels for the job
	Labels string `column:"name=labels"`

	// Runtime info of the job
	RuntimeInfo string `column:"name=runtime_info"`
	// State of the job
	State string `column:"name=state"`

	// Creation time of the job
	CreationTime time.Time `column:"name=creation_time"`
	// Start time of the job
	StartTime time.Time `column:"name=start_time"`
	// Completion time of the job
	CompletionTime time.Time `column:"name=completion_time"`
	// Time when job was updated
	UpdateTime time.Time `column:"name=update_time"`
}

// JobIndexOps provides methods for manipulating job_index table.
type JobIndexOps interface {
	// Create inserts a row in the table.
	Create(
		ctx context.Context,
		id *peloton.JobID,
		config *job.JobConfig,
		runtime *job.RuntimeInfo,
	) error

	// Get retrieves a row from the table.
	Get(ctx context.Context, id *peloton.JobID) (*JobIndexObject, error)

	// GetSummary returns a JobSummary for a row in the table
	GetSummary(ctx context.Context, id *peloton.JobID) (*job.JobSummary, error)

	// Update modifies an object in the table.
	Update(
		ctx context.Context,
		id *peloton.JobID,
		config *job.JobConfig,
		runtime *job.RuntimeInfo,
	) error

	// Delete removes an object from the table.
	Delete(ctx context.Context, id *peloton.JobID) error
}

// ensure that default implementation (jobIndexOps) satisfies the interface
var _ JobIndexOps = (*jobIndexOps)(nil)

// newJobIndexObject creates a JobIndexObject from job config and runtime
func newJobIndexObject(
	id *peloton.JobID,
	config *job.JobConfig,
	runtime *job.RuntimeInfo) (*JobIndexObject, error) {

	obj := &JobIndexObject{JobID: id.GetValue()}
	jobLog := log.WithField("object", "JobIndex").
		WithField("job_id", id.GetValue())

	if config != nil {
		// Do not save the instance config with the job
		// configuration in the job_index table.
		instanceConfig := config.GetInstanceConfig()
		config.InstanceConfig = nil
		configBuffer, err := json.Marshal(config)
		config.InstanceConfig = instanceConfig
		if err != nil {
			return nil, errors.Wrap(err, "Failed to marshal jobConfig")
		}

		labelBuffer, err := json.Marshal(config.Labels)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to marshal labels")
		}
		obj.JobType = uint32(config.GetType())

		obj.Name = config.GetName()
		obj.Owner = config.GetOwningTeam()
		obj.RespoolID = config.GetRespoolID().GetValue()

		obj.Config = string(configBuffer)
		obj.InstanceCount = uint32(config.GetInstanceCount())
		obj.Labels = string(labelBuffer)
	}

	if runtime != nil {
		runtimeBuffer, err := json.Marshal(runtime)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to marshal job runtime")
		}

		obj.RuntimeInfo = string(runtimeBuffer)
		obj.State = runtime.GetState().String()
		obj.UpdateTime = time.Now()

		if runtime.GetCreationTime() != "" {
			t, err := time.Parse(time.RFC3339Nano, runtime.GetCreationTime())
			if err == nil {
				obj.CreationTime = t
			} else {
				jobLog.WithField("runtime", runtime).
					WithError(err).
					Info("Fail to parse creationTime")
			}
		}
		if runtime.GetStartTime() != "" {
			t, err := time.Parse(time.RFC3339Nano, runtime.GetStartTime())
			if err == nil {
				obj.StartTime = t
			} else {
				jobLog.WithField("runtime", runtime).
					WithError(err).
					Info("Fail to parse startTime")
			}
		}

		if runtime.GetCompletionTime() != "" {
			t, err := time.Parse(time.RFC3339Nano, runtime.GetCompletionTime())
			if err == nil {
				obj.CompletionTime = t
			} else {
				jobLog.WithField("runtime", runtime).
					WithError(err).
					Info("Fail to parse completionTime")
			}
		}
	}
	return obj, nil
}

// ToJobSummary generates a JobSummary from the JobIndexObject
func (j *JobIndexObject) ToJobSummary() *job.JobSummary {
	summary := &job.JobSummary{
		Id:            &peloton.JobID{Value: j.JobID},
		Name:          j.Name,
		Owner:         j.Owner,
		OwningTeam:    j.Owner,
		InstanceCount: uint32(j.InstanceCount),
		Type:          job.JobType(j.JobType),
		RespoolID:     &peloton.ResourcePoolID{Value: j.RespoolID},
	}
	err := json.Unmarshal([]byte(j.RuntimeInfo), &summary.Runtime)
	if err != nil {
		log.WithField("job_id", j.JobID).
			WithField("runtime_info", j.RuntimeInfo).
			WithError(err).
			Info("JobIndexObject: failed to unmarshal runtime info")
	}
	err = json.Unmarshal([]byte(j.Labels), &summary.Labels)
	if err != nil {
		log.WithField("job_id", j.JobID).
			WithField("labels", j.Labels).
			WithError(err).
			Info("JobIndexObject: failed to unmarshal labels")
	}
	return summary
}

// jobIndexOps implements JobIndexOps using a particular Store
type jobIndexOps struct {
	store *Store
}

// NewJobIndexOps constructs a JobIndexOps object for provided Store.
func NewJobIndexOps(s *Store) JobIndexOps {
	return &jobIndexOps{store: s}
}

// Create creates a JobIndexObject in db
func (d *jobIndexOps) Create(
	ctx context.Context,
	id *peloton.JobID,
	config *job.JobConfig,
	runtime *job.RuntimeInfo,
) error {
	obj, err := newJobIndexObject(id, config, runtime)
	if err != nil {
		d.store.metrics.OrmJobMetrics.JobIndexCreateFail.Inc(1)
		return errors.Wrap(err, "Failed to construct JobIndexObject")
	}
	if err = d.store.oClient.Create(ctx, obj); err != nil {
		d.store.metrics.OrmJobMetrics.JobIndexCreateFail.Inc(1)
		return err
	}
	d.store.metrics.OrmJobMetrics.JobIndexCreate.Inc(1)
	return nil
}

// Get gets a JobIndexObject from db
func (d *jobIndexOps) Get(
	ctx context.Context,
	id *peloton.JobID,
) (*JobIndexObject, error) {
	jobIndexObject := &JobIndexObject{
		JobID: id.GetValue(),
	}
	if err := d.store.oClient.Get(ctx, jobIndexObject); err != nil {
		d.store.metrics.OrmJobMetrics.JobIndexGetFail.Inc(1)
		return nil, err
	}
	d.store.metrics.OrmJobMetrics.JobIndexGet.Inc(1)
	return jobIndexObject, nil
}

// GetSummary gets JobSummary for JobIndexObject from db
func (d *jobIndexOps) GetSummary(
	ctx context.Context,
	id *peloton.JobID,
) (*job.JobSummary, error) {
	jobIndexObject, err := d.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return jobIndexObject.ToJobSummary(), nil
}

// Update updates a JobIndexObject in db
func (d *jobIndexOps) Update(
	ctx context.Context,
	id *peloton.JobID,
	config *job.JobConfig,
	runtime *job.RuntimeInfo,
) error {
	if config == nil && runtime == nil {
		return nil
	}
	obj, err := newJobIndexObject(id, config, runtime)
	if err != nil {
		d.store.metrics.OrmJobMetrics.JobIndexUpdateFail.Inc(1)
		return errors.Wrap(err, "Failed to construct JobIndexObject")
	}
	fields := []string{}
	if config != nil {
		fields = append(fields, _configFields...)
	}
	if runtime != nil {
		fields = append(fields, _runtimeFields...)
	}

	err = d.store.oClient.Update(ctx, obj, fields...)
	if err != nil {
		log.WithField("job_id", id.GetValue()).
			WithField("config", config).
			WithField("runtime", runtime).
			WithError(err).Error("Failed to update job_index")
		d.store.metrics.OrmJobMetrics.JobIndexUpdateFail.Inc(1)
		return err
	}
	d.store.metrics.OrmJobMetrics.JobIndexUpdate.Inc(1)
	return nil
}

// Delete deletes a JobIndexObject from db
func (d *jobIndexOps) Delete(
	ctx context.Context,
	id *peloton.JobID,
) error {
	jobIndexObject := &JobIndexObject{
		JobID: id.GetValue(),
	}
	if err := d.store.oClient.Delete(ctx, jobIndexObject); err != nil {
		d.store.metrics.OrmJobMetrics.JobIndexDeleteFail.Inc(1)
		return err
	}
	d.store.metrics.OrmJobMetrics.JobIndexDelete.Inc(1)
	return nil
}
