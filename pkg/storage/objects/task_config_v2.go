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
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/storage/objects/base"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"go.uber.org/yarpc/yarpcerrors"
)

// init adds a TaskConfigV2Object instance to the global list of storage objects
func init() {
	Objs = append(Objs, &TaskConfigV2Object{})
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
}

// ensure that default implementation (taskConfigV2Object) satisfies the interface
var _ TaskConfigV2Ops = (*taskConfigV2Object)(nil)

// taskConfigV2Object implements TaskConfigV2Ops using a particular Store
type taskConfigV2Object struct {
	store *Store
}

// NewTaskConfigV2OpsOps constructs a TaskConfigV2Ops object for provided Store.
func NewTaskConfigV2OpsOps(s *Store) TaskConfigV2Ops {
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
