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
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/storage/objects/base"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

// init adds a TaskConfigV2Object instance to the global list of storage objects
func init() {
	Objs = append(Objs, &TaskConfigV2Object{}, &TaskConfigObject{})
}

// TaskConfigV2Object corresponds to a row in task_config_v2 table.
type TaskConfigV2Object struct {
	// base.Object DB specific annotations
	base.Object `cassandra:"name=task_config_v2, primaryKey=((job_id,version,instance_id))"`
	// JobID of the job which the task belongs to (uuid)
	JobID string `column:"name=job_id"`
	// Version of the config
	Version uint64 `column:"name=version"`
	// InstanceID of the task
	InstanceID int64 `column:"name=instance_id"`
	// Config of the task config
	Config []byte `column:"name=config"`
	// ConfigAddOn of the task config
	ConfigAddOn []byte `column:"name=config_addon"`
	// CreationTime of the task config
	CreationTime time.Time `column:"name=creation_time"`
	// Spec of the task config
	Spec []byte `column:"name=spec"`
	// APIVersion of the task config
	APIVersion string `column:"name=api_version"`
}

// transform will convert all the value from DB into the corresponding type
// in ORM object to be interpreted by base store client
func (o *TaskConfigV2Object) transform(row map[string]interface{}) {
	o.JobID = row["job_id"].(string)
	o.Version = row["version"].(uint64)
	o.InstanceID = int64(row["instance_id"].(uint64))
	o.Config = row["config"].([]byte)
	o.ConfigAddOn = row["config_addon"].([]byte)
	o.CreationTime = row["creation_time"].(time.Time)
	o.Spec = row["spec"].([]byte)
	o.APIVersion = row["api_version"].(string)

}

// TaskConfigObject corresponds to a row in task_config table. This is a legacy
// table to which nothing should be written, only used for reading.
type TaskConfigObject struct {
	// base.Object DB specific annotations
	base.Object `cassandra:"name=task_config, primaryKey=((job_id),version,instance_id)"`
	// JobID of the job which the task belongs to (uuid)
	JobID string `column:"name=job_id"`
	// Version of the config
	Version uint64 `column:"name=version"`
	// InstanceID of the task
	InstanceID int64 `column:"name=instance_id"`
	// Config of the task config
	Config []byte `column:"name=config"`
	// ConfigAddOn of the task config
	ConfigAddOn []byte `column:"name=config_addon"`
	// CreationTime of the task config
	CreationTime time.Time `column:"name=creation_time"`
}

func (o *TaskConfigObject) transform(row map[string]interface{}) {
	o.JobID = row["job_id"].(string)
	o.Version = row["version"].(uint64)
	o.InstanceID = int64(row["instance_id"].(uint64))
	o.Config = row["config"].([]byte)
	o.ConfigAddOn = row["config_addon"].([]byte)
	o.CreationTime = row["creation_time"].(time.Time)
}

const (
	specColumn        = "spec"
	configColumn      = "config"
	configAddOnColumn = "config_addon"
)

// TaskConfigV2Ops provides methods for manipulating task_config_v2 table.
type TaskConfigV2Ops interface {
	// Create creates task config with version number for a task
	Create(
		ctx context.Context,
		id *peloton.JobID,
		instanceID int64,
		taskConfig *pbtask.TaskConfig,
		configAddOn *models.ConfigAddOn,
		podSpec *pbpod.PodSpec,
		version uint64,
	) error

	// GetPodSpec returns the pod spec of a task config
	GetPodSpec(
		ctx context.Context,
		id *peloton.JobID,
		instanceID uint32,
		version uint64,
	) (*pbpod.PodSpec, error)

	// GetTaskConfig returns the task specific config
	GetTaskConfig(
		ctx context.Context,
		id *peloton.JobID,
		instanceID uint32,
		version uint64,
	) (*pbtask.TaskConfig, *models.ConfigAddOn, error)
}

// ensure that default implementation (taskConfigV2Object) satisfies the interface
var _ TaskConfigV2Ops = (*taskConfigV2Object)(nil)

// taskConfigV2Object implements TaskConfigV2Ops using a particular Store
type taskConfigV2Object struct {
	store *Store
}

// NewTaskConfigV2Ops constructs a TaskConfigV2Ops object for provided Store.
func NewTaskConfigV2Ops(s *Store) TaskConfigV2Ops {
	return &taskConfigV2Object{store: s}
}

// Create creates task config with version number for a task
func (d *taskConfigV2Object) Create(
	ctx context.Context,
	id *peloton.JobID,
	instanceID int64,
	taskConfig *pbtask.TaskConfig,
	configAddOn *models.ConfigAddOn,
	podSpec *pbpod.PodSpec,
	version uint64,
) (err error) {
	defer func() {
		if err != nil {
			d.store.metrics.OrmTaskMetrics.TaskConfigV2CreateFail.Inc(1)
		} else {
			d.store.metrics.OrmTaskMetrics.TaskConfigV2Create.Inc(1)
		}
	}()

	configBuffer, err := proto.Marshal(taskConfig)
	if err != nil {
		return errors.Wrap(yarpcerrors.InvalidArgumentErrorf(err.Error()),
			"fail to unmarshal task config")
	}

	addOnBuffer, err := proto.Marshal(configAddOn)
	if err != nil {
		return errors.Wrap(yarpcerrors.InvalidArgumentErrorf(err.Error()),
			"fail to unmarshal config addon")
	}

	var specBuffer []byte
	apiVersion := api.V0
	if podSpec != nil {
		specBuffer, err = proto.Marshal(podSpec)
		if err != nil {
			return errors.Wrap(yarpcerrors.InvalidArgumentErrorf(err.Error()),
				"fail to unmarshal pod spec")
		}
		apiVersion = api.V1
	}

	obj := &TaskConfigV2Object{
		JobID:        id.GetValue(),
		Version:      version,
		InstanceID:   instanceID,
		Config:       configBuffer,
		ConfigAddOn:  addOnBuffer,
		CreationTime: time.Now(),
		Spec:         specBuffer,
		APIVersion:   apiVersion.String(),
	}

	return d.store.oClient.Create(ctx, obj)
}

// GetPodSpec returns the pod spec of a task config
func (d *taskConfigV2Object) GetPodSpec(
	ctx context.Context,
	id *peloton.JobID,
	instanceID uint32,
	version uint64,
) (result *pbpod.PodSpec, err error) {
	defer func() {
		if err != nil {
			d.store.metrics.OrmTaskMetrics.PodSpecGetFail.Inc(1)
		} else {
			d.store.metrics.OrmTaskMetrics.PodSpecGet.Inc(1)
		}
	}()

	obj := &TaskConfigV2Object{
		JobID:      id.GetValue(),
		InstanceID: int64(instanceID),
		Version:    version,
	}

	row, err := d.store.oClient.Get(ctx, obj, specColumn)
	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		// per-instance spec not found, return default spec in this case.
		obj.InstanceID = common.DefaultTaskConfigID
		row, err = d.store.oClient.Get(ctx, obj, specColumn)
		if err != nil {
			return nil, err
		}
	}

	if len(row) == 0 {
		return nil, yarpcerrors.NotFoundErrorf("pod spec " +
			"not found")
	}
	obj.Spec = row["spec"].([]byte)
	// no spec set, return nil
	if len(obj.Spec) == 0 {
		return nil, nil
	}

	podSpec := &pbpod.PodSpec{}
	if err := proto.Unmarshal(obj.Spec, podSpec); err != nil {
		return nil, errors.Wrap(yarpcerrors.InternalErrorf(err.Error()),
			"Failed to unmarshal pod spec")
	}

	return podSpec, nil
}

// GetTaskConfig returns the task specific config
func (d *taskConfigV2Object) GetTaskConfig(
	ctx context.Context,
	id *peloton.JobID,
	instanceID uint32,
	version uint64,
) (taskConfig *pbtask.TaskConfig, configAddOn *models.ConfigAddOn, err error) {
	defer func() {
		if err != nil {
			d.store.metrics.OrmTaskMetrics.TaskConfigV2GetFail.Inc(1)
		} else {
			d.store.metrics.OrmTaskMetrics.TaskConfigV2Get.Inc(1)
		}
	}()

	taskConfig, configAddOn, err = d.getTaskConfig(ctx, id, int64(instanceID),
		version)

	// no instance config, return default config
	if taskConfig == nil {
		return d.getTaskConfig(ctx, id,
			common.DefaultTaskConfigID,
			version)
	}

	// either an error occurs or there is instance config
	return taskConfig, configAddOn, err
}

// getTaskConfig returns config of specific version,
// different from GetTaskConfig it does not which version
// number is default config version and which is instance
// specific config version
func (d *taskConfigV2Object) getTaskConfig(
	ctx context.Context,
	id *peloton.JobID,
	instanceID int64,
	version uint64,
) (*pbtask.TaskConfig, *models.ConfigAddOn, error) {
	obj := &TaskConfigV2Object{
		JobID:      id.GetValue(),
		InstanceID: int64(instanceID),
		Version:    version,
	}

	row, err := d.store.oClient.Get(
		ctx, obj, configColumn, configAddOnColumn)
	if err != nil {
		if yarpcerrors.IsNotFound(errors.Cause(err)) {
			return d.handleLegacyConfig(ctx, id, instanceID, version)
		}
		return nil, nil, err
	}
	// if db return no row, return legacy config
	if len(row) == 0 {
		return d.handleLegacyConfig(ctx, id, instanceID, version)
	}

	// no config set, return nil
	if row["config"] == nil {
		return nil, nil, nil
	}
	obj.Config = row["config"].([]byte)
	obj.ConfigAddOn = row["config_addon"].([]byte)

	taskConfig := &pbtask.TaskConfig{}
	if err := proto.Unmarshal(obj.Config, taskConfig); err != nil {
		return nil, nil, errors.Wrap(yarpcerrors.InternalErrorf(err.Error()),
			"Failed to unmarshal task config")
	}

	configAddOn := &models.ConfigAddOn{}
	if err := proto.Unmarshal(obj.ConfigAddOn, configAddOn); err != nil {
		return nil, nil, errors.Wrap(yarpcerrors.InternalErrorf(err.Error()),
			"Failed to unmarshal config addOn")
	}

	return taskConfig, configAddOn, nil
}

// Read config from legacy task_config table and back fill to task_config_v2.
func (d *taskConfigV2Object) handleLegacyConfig(
	ctx context.Context,
	id *peloton.JobID,
	instanceID int64,
	version uint64,
) (*pbtask.TaskConfig, *models.ConfigAddOn, error) {
	var err error
	taskConfig := &pbtask.TaskConfig{}
	configAddOn := &models.ConfigAddOn{}

	defer func() {
		if err != nil {
			d.store.metrics.OrmTaskMetrics.TaskConfigLegacyGetFail.Inc(1)
		} else {
			if taskConfig != nil {
				// if taskConfig is not nil, this means the config definitely
				// exists in legacy task_config table.
				d.store.metrics.OrmTaskMetrics.TaskConfigLegacyGet.Inc(1)
			}
		}
	}()

	objLegacy := &TaskConfigObject{
		JobID:      id.GetValue(),
		InstanceID: int64(instanceID),
		Version:    version,
	}

	row, err := d.store.oClient.Get(
		ctx, objLegacy, configColumn, configAddOnColumn)
	if err != nil {
		return nil, nil, err
	}
	// if db return no row, return nil
	if len(row) == 0 {
		return nil, nil, yarpcerrors.NotFoundErrorf(
			"task config not found")
	}

	// no config set, return nil
	if row["config"] == nil {
		return nil, nil, nil
	}
	objLegacy.Config = row["config"].([]byte)
	objLegacy.ConfigAddOn = row["config_addon"].([]byte)
	// no config set, return nil
	if len(objLegacy.Config) == 0 {
		return nil, nil, nil
	}

	if err = proto.Unmarshal(objLegacy.Config, taskConfig); err != nil {
		return nil, nil, errors.Wrap(yarpcerrors.InternalErrorf(err.Error()),
			"Failed to unmarshal legacy task config")
	}

	if err = proto.Unmarshal(objLegacy.ConfigAddOn, configAddOn); err != nil {
		return nil, nil, errors.Wrap(yarpcerrors.InternalErrorf(err.Error()),
			"Failed to unmarshal legacy config addOn")
	}

	// Now write the legacy config back to task_config_v2 table.
	if err = d.Create(
		ctx,
		id,
		instanceID,
		taskConfig,
		configAddOn,
		nil,
		version,
	); err != nil {
		// Do not fail read path because write path failed.
		log.WithError(err).
			WithFields(log.Fields{
				"job_id":      id.GetValue(),
				"instance_id": instanceID,
			}).
			Info("back fill to task_config_v2 failed")
	}
	return taskConfig, configAddOn, nil
}
