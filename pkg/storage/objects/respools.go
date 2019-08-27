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

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/pkg/storage/objects/base"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	_respoolConfigFields = []string{
		"RespoolConfig",
		"UpdateTime",
	}
)

// ResPoolObject corresponds to a row in respools table.
type ResPoolObject struct {
	// base.Object DB specific annotations.
	base.Object `cassandra:"name=respools, primaryKey=((respool_id))"`
	// RespoolID is the ID of the resource pool being created.
	RespoolID *base.OptionalString `column:"name=respool_id"`
	// RespoolConfig contains the resource pool's basic config information.
	RespoolConfig string `column:"name=respool_config"`
	// Owner of the resource pool.
	Owner string `column:"name=owner"`
	// Timestamp of the resource pool when it's created.
	CreationTime time.Time `column:"name=creation_time"`
	// Most recent timestamp when resource pool is updated.
	UpdateTime time.Time `column:"name=update_time"`
}

// transform will convert all the value from DB into the corresponding type
// in ORM object to be interpreted by base store client
func (o *ResPoolObject) transform(row map[string]interface{}) {
	o.RespoolID = base.NewOptionalString(row["respool_id"])
	o.RespoolConfig = row["respool_config"].(string)
	o.Owner = row["owner"].(string)
	o.UpdateTime = row["update_time"].(time.Time)
	o.CreationTime = row["creation_time"].(time.Time)
}

// ResPoolOpsResult contains the unmarshalled result of a respool_config Get()
// From this object, the caller can retrieve respool config.
type ResPoolOpsResult struct {
	// Timestamp of the resource pool when it's created.
	CreationTime time.Time
	// Owner of the resource pool.
	Owner string
	// RespoolConfig is the unmarshalled respool config
	RespoolConfig *respool.ResourcePoolConfig
}

// ResPoolOps provides methods for manipulating respool table.
type ResPoolOps interface {
	// Create inserts a new respool in the table.
	Create(
		ctx context.Context,
		id *peloton.ResourcePoolID,
		config *respool.ResourcePoolConfig,
		owner string,
	) error

	// GetAll gets all the resource pool configs.
	GetAll(
		ctx context.Context,
	) (map[string]*respool.ResourcePoolConfig, error)

	// Update modifies the respool in the table.
	Update(
		ctx context.Context,
		id *peloton.ResourcePoolID,
		config *respool.ResourcePoolConfig,
	) error

	// Delete removes the respool from the table.
	Delete(
		ctx context.Context,
		id *peloton.ResourcePoolID,
	) error

	// GetResult retrieves the ResPoolObject from the table.
	GetResult(
		ctx context.Context,
		respoolId string,
	) (*ResPoolOpsResult, error)
}

// resPoolOps implements ResPoolOps using a particular Store.
type resPoolOps struct {
	store *Store
}

// init adds a ResPoolObject instance to the global list of storage objects.
func init() {
	Objs = append(Objs, &ResPoolObject{})
}

// newResPoolObject creates a ResPoolObject.
func newResPoolObject(
	id *peloton.ResourcePoolID,
	config *respool.ResourcePoolConfig,
	creationTime time.Time,
	updateTime time.Time,
	owner string,
) (*ResPoolObject, error) {
	obj := &ResPoolObject{
		RespoolID:    base.NewOptionalString(id.GetValue()),
		CreationTime: creationTime,
		Owner:        owner,
	}
	obj.UpdateTime = updateTime

	if config != nil {
		configBuffer, err := json.Marshal(config)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to marshal resourcePoolConfig")
		}
		obj.RespoolConfig = string(configBuffer)
		obj.Owner = config.GetOwningTeam()
	}
	return obj, nil
}

// Default resPoolOps implementation.
var _ ResPoolOps = (*resPoolOps)(nil)

// NewResPoolOps constructs a ResPoolOps object for provided Store.
func NewResPoolOps(s *Store) ResPoolOps {
	return &resPoolOps{store: s}
}

// Create creates a ResPoolObject in db.
func (r *resPoolOps) Create(
	ctx context.Context,
	id *peloton.ResourcePoolID,
	Config *respool.ResourcePoolConfig,
	owner string,
) error {
	createTime := time.Now().UTC()
	obj, err := newResPoolObject(id, Config, createTime, createTime, owner)
	if err != nil {
		r.store.metrics.OrmRespoolMetrics.RespoolCreateFail.Inc(1)
		return errors.Wrap(err, "Failed to construct ResPoolObject")
	}

	if err = r.store.oClient.CreateIfNotExists(ctx, obj); err != nil {
		r.store.metrics.OrmRespoolMetrics.RespoolCreateFail.Inc(1)
		return err
	}

	r.store.metrics.OrmRespoolMetrics.RespoolCreate.Inc(1)
	return nil
}

// GetAll gets all the resource pool configs from the table.
func (r *resPoolOps) GetAll(ctx context.Context) (map[string]*respool.ResourcePoolConfig, error) {
	resultObjs := map[string]*respool.ResourcePoolConfig{}

	rows, err := r.store.oClient.GetAll(ctx, &ResPoolObject{})
	if err != nil {
		r.store.metrics.OrmRespoolMetrics.RespoolGetAllFail.Inc(1)
		return nil, err
	}

	for _, row := range rows {
		resPoolObj := &ResPoolObject{}
		resPoolObj.transform(row)
		respoolConfig, err := resPoolObj.toConfig()
		if err != nil {
			r.store.metrics.OrmRespoolMetrics.RespoolGetAllFail.Inc(1)
			return nil, err
		}
		resultObjs[resPoolObj.RespoolID.Value] = respoolConfig
	}

	r.store.metrics.OrmRespoolMetrics.RespoolGetAll.Inc(1)
	return resultObjs, nil
}

// Get retrieves the ResPoolObject from the table.
func (r *resPoolOps) GetResult(
	ctx context.Context,
	respoolId string,
) (*ResPoolOpsResult, error) {
	respoolObj := &ResPoolObject{
		RespoolID: base.NewOptionalString(respoolId),
	}
	row, err := r.store.oClient.Get(ctx, respoolObj)
	if err != nil {
		r.store.metrics.OrmRespoolMetrics.RespoolGetFail.Inc(1)
		return nil, err
	}
	if len(row) == 0 {
		return nil, nil
	}
	respoolObj.transform(row)
	config, err := respoolObj.toConfig()
	if err != nil {
		r.store.metrics.OrmRespoolMetrics.RespoolGetFail.Inc(1)
		return nil, errors.Wrap(err, "Failed to unmarshal config")
	}

	r.store.metrics.OrmRespoolMetrics.RespoolGet.Inc(1)
	return &ResPoolOpsResult{
		CreationTime:  respoolObj.CreationTime,
		Owner:         respoolObj.Owner,
		RespoolConfig: config,
	}, nil
}

// Update modifies ResPoolObject in db.
func (r *resPoolOps) Update(
	ctx context.Context,
	id *peloton.ResourcePoolID,
	config *respool.ResourcePoolConfig,
) error {
	if config == nil {
		return nil
	}

	// update `RespoolConfig` and `UpdateTime` field.
	obj := &ResPoolObject{
		RespoolID: base.NewOptionalString(id.GetValue()),
	}
	row, err := r.store.oClient.Get(ctx, obj)
	if err != nil {
		r.store.metrics.OrmRespoolMetrics.RespoolGetFail.Inc(1)
		return err
	}
	if len(row) == 0 {
		return nil
	}
	obj.transform(row)
	configBuffer, err := json.Marshal(config)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal resourcePoolConfig")
	}
	obj.RespoolConfig = string(configBuffer)
	obj.UpdateTime = time.Now().UTC()

	err = r.store.oClient.Update(ctx, obj, _respoolConfigFields...)
	if err != nil {
		log.WithFields(log.Fields{
			"respool_id": id.GetValue(),
			"config":     config,
		}).Error(err)

		r.store.metrics.OrmRespoolMetrics.RespoolUpdateFail.Inc(1)
		return err
	}

	r.store.metrics.OrmRespoolMetrics.RespoolUpdate.Inc(1)
	return nil
}

// Delete removes the ResPoolObject from the db.
func (r *resPoolOps) Delete(ctx context.Context, id *peloton.ResourcePoolID) error {
	resPoolObject := &ResPoolObject{
		RespoolID: base.NewOptionalString(id.GetValue()),
	}
	if err := r.store.oClient.Delete(ctx, resPoolObject); err != nil {
		r.store.metrics.OrmRespoolMetrics.RespoolDeleteFail.Inc(1)
		return err
	}
	r.store.metrics.OrmRespoolMetrics.RespoolDelete.Inc(1)
	return nil
}

func (r *ResPoolObject) toConfig() (*respool.ResourcePoolConfig, error) {
	configBuffer := []byte(r.RespoolConfig)
	config := &respool.ResourcePoolConfig{}
	err := json.Unmarshal(configBuffer, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}
