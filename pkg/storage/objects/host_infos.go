// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law orupd agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objects

import (
	"context"
	"encoding/json"
	"go.uber.org/yarpc/yarpcerrors"
	"time"

	hostpb "github.com/uber/peloton/.gen/peloton/api/v0/host"
	pelotonpb "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/pkg/storage/objects/base"
)

// it adds a HostInfoObject instance to the global list of storage objects
func init() {
	Objs = append(Objs, &HostInfoObject{})
}

// HostInfoObject corresponds to a row in host_info table.
type HostInfoObject struct {
	// DB specific annotations
	base.Object `cassandra:"name=host_info, primaryKey=((hostname))"`
	// Hostname of the host
	Hostname *base.OptionalString `column:"name=hostname"`
	// IP address of the host
	IP string `column:"name=ip"`
	// HostState of the host
	State string `column:"name=state"`
	// GoalState of the host
	GoalState string `column:"name=goal_state"`
	// Labels of the host
	Labels string `column:"name=labels"`
	// Current host Pool for the host
	// This will indicate which host pool this host belongs to
	CurrentPool string `column:"name=current_pool"`
	// Desired host pool for the host
	// This will indicate which host pool this host should be.
	DesiredPool string `column:"name=desired_pool"`
	// Last update time of the host maintenance
	UpdateTime time.Time `column:"name=update_time"`
}

// transform will convert all the value from DB into the corresponding type
// in ORM object to be interpreted by base store client
func (o *HostInfoObject) transform(row map[string]interface{}) {
	o.Hostname = base.NewOptionalString(row["hostname"])
	o.IP = row["ip"].(string)
	o.State = row["state"].(string)
	o.GoalState = row["goal_state"].(string)
	o.Labels = row["labels"].(string)
	o.CurrentPool = row["current_pool"].(string)
	o.DesiredPool = row["desired_pool"].(string)
	o.UpdateTime = row["update_time"].(time.Time)
}

// HostInfoOps provides methods for manipulating host_maintenance table.
type HostInfoOps interface {
	// Create inserts a row in the table.
	Create(
		ctx context.Context,
		hostname string,
		ip string,
		state hostpb.HostState,
		goalState hostpb.HostState,
		labels map[string]string,
		currentPool string,
		desiredPool string,
	) error

	// Get retrieves the row based on the primary key from the table.
	Get(
		ctx context.Context,
		hostname string,
	) (*hostpb.HostInfo, error)

	// GetAll retrieves all rows from the table (with no selection on any key).
	GetAll(ctx context.Context) ([]*hostpb.HostInfo, error)

	// Update modifies an object in the table.
	Update(
		ctx context.Context,
		hostname string,
		state hostpb.HostState,
		goalState hostpb.HostState,
		labels map[string]string,
		currentPool string,
		desiredPool string,
	) error

	// Delete removes an object from the table based on primary key.
	Delete(ctx context.Context, hostname string) error
}

// hostInfoOps implements HostInfoOps using a particular Store
type hostInfoOps struct {
	store *Store
}

// Ensure that default implementation hostInfoOps satisfies the interface
var _ HostInfoOps = (*hostInfoOps)(nil)

// NewHostInfoOps constructs a HostInfoOps object for a provided Store.
func NewHostInfoOps(s *Store) HostInfoOps {
	return &hostInfoOps{store: s}
}

// Create creates a host info in db
func (d *hostInfoOps) Create(
	ctx context.Context,
	hostname string,
	ip string,
	state hostpb.HostState,
	goalState hostpb.HostState,
	labels map[string]string,
	currentPool string,
	desiredPool string,
) error {
	bytes, err := json.Marshal(&labels)
	if err != nil {
		return err
	}
	hostInfoObject := &HostInfoObject{
		Hostname:    base.NewOptionalString(hostname),
		IP:          ip,
		State:       state.String(),
		GoalState:   goalState.String(),
		Labels:      string(bytes),
		CurrentPool: currentPool,
		DesiredPool: desiredPool,
		UpdateTime:  time.Now(),
	}
	if err := d.store.oClient.Create(ctx, hostInfoObject); err != nil {
		d.store.metrics.OrmHostInfoMetrics.HostInfoAddFail.Inc(1)
		return err
	}
	d.store.metrics.OrmHostInfoMetrics.HostInfoAdd.Inc(1)
	return nil
}

// Get gets a host info from db by its hostname pk
func (d *hostInfoOps) Get(
	ctx context.Context,
	hostname string,
) (*hostpb.HostInfo, error) {
	var row map[string]interface{}
	hostInfoObject := &HostInfoObject{
		Hostname: base.NewOptionalString(hostname),
	}
	row, err := d.store.oClient.Get(ctx, hostInfoObject)
	if err != nil {
		d.store.metrics.OrmHostInfoMetrics.HostInfoGetFail.Inc(1)
		return nil, err
	}
	if len(row) == 0 {
		return nil, yarpcerrors.NotFoundErrorf(
			"host info not found %s", hostname)
	}
	hostInfoObject.transform(row)
	info, err := newHostInfoFromHostInfoObject(hostInfoObject)
	if err != nil {
		d.store.metrics.OrmHostInfoMetrics.HostInfoGetFail.Inc(1)
		return nil, err
	}
	d.store.metrics.OrmHostInfoMetrics.HostInfoGet.Inc(1)
	return info, nil
}

// GetAll gets all host infos from db without any pk specified
func (d *hostInfoOps) GetAll(ctx context.Context) ([]*hostpb.HostInfo, error) {
	rows, err := d.store.oClient.GetAll(ctx, &HostInfoObject{})
	if err != nil {
		d.store.metrics.OrmHostInfoMetrics.HostInfoGetAllFail.Inc(1)
		return nil, err
	}
	d.store.metrics.OrmHostInfoMetrics.HostInfoGetAll.Inc(1)

	var hostInfos []*hostpb.HostInfo
	for _, row := range rows {
		obj := &HostInfoObject{}
		obj.transform(row)
		info, err := newHostInfoFromHostInfoObject(obj)
		if err != nil {
			d.store.metrics.OrmHostInfoMetrics.HostInfoGetAllFail.Inc(1)
			return nil, err
		}
		hostInfos = append(hostInfos, info)
	}
	return hostInfos, nil
}

// Update the host state and goal state of a host info by its hostname pk
func (d *hostInfoOps) Update(
	ctx context.Context,
	hostname string,
	state hostpb.HostState,
	goalState hostpb.HostState,
	labels map[string]string,
	currentPool string,
	desiredPool string,
) error {
	bytes, err := json.Marshal(&labels)
	if err != nil {
		return err
	}
	hostInfoObject := &HostInfoObject{
		Hostname:    base.NewOptionalString(hostname),
		State:       state.String(),
		GoalState:   goalState.String(),
		Labels:      string(bytes),
		CurrentPool: currentPool,
		DesiredPool: desiredPool,
		UpdateTime:  time.Now(),
	}
	fieldsToUpdate := []string{"State", "GoalState", "Labels", "CurrentPool",
		"DesiredPool", "UpdateTime"}
	if err := d.store.oClient.Update(
		ctx,
		hostInfoObject,
		fieldsToUpdate...); err != nil {
		d.store.metrics.OrmHostInfoMetrics.HostInfoUpdateFail.Inc(1)
		return err
	}
	d.store.metrics.OrmHostInfoMetrics.HostInfoUpdate.Inc(1)
	return nil
}

// Delete deletes a host info from db by its hostname pk
func (d *hostInfoOps) Delete(ctx context.Context, hostname string) error {
	hostInfoObject := &HostInfoObject{
		Hostname: base.NewOptionalString(hostname),
	}
	if err := d.store.oClient.Delete(ctx, hostInfoObject); err != nil {
		d.store.metrics.OrmHostInfoMetrics.HostInfoDeleteFail.Inc(1)
		return err
	}
	d.store.metrics.OrmHostInfoMetrics.HostInfoDelete.Inc(1)
	return nil
}

// newHostInfoFromHostInfoObject creates a new *hostpb.HostInfo
// and sets each field from a HostInfoObject object
func newHostInfoFromHostInfoObject(
	hostInfoObject *HostInfoObject) (*hostpb.HostInfo, error) {
	hostInfo := &hostpb.HostInfo{}
	hostInfo.Hostname = hostInfoObject.Hostname.String()
	hostInfo.Ip = hostInfoObject.IP
	hostInfo.State = hostpb.HostState(
		hostpb.HostState_value[hostInfoObject.State])
	hostInfo.GoalState = hostpb.HostState(
		hostpb.HostState_value[hostInfoObject.GoalState])

	if hostInfoObject.Labels != "" {
		labels := make(map[string]string)
		err := json.Unmarshal([]byte(hostInfoObject.Labels), &labels)
		if err != nil {
			return nil, err
		}
		for l, v := range labels {
			hostInfo.Labels = append(
				hostInfo.Labels,
				&pelotonpb.Label{Key: l, Value: v},
			)
		}
	}
	hostInfo.CurrentPool = hostInfoObject.CurrentPool
	hostInfo.DesiredPool = hostInfoObject.DesiredPool

	return hostInfo, nil
}
