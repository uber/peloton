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

package mpb

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesos_master "github.com/uber/peloton/.gen/mesos/v1/master"

	"go.uber.org/yarpc/api/transport"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber/peloton/.gen/mesos/v1/maintenance"
)

const (
	_procedure = "Call_MasterOperator"
	_timeout   = 10 * 1000 * time.Millisecond
)

// MasterOperatorClient makes Mesos JSON requests to Mesos Master endpoint(s)
type MasterOperatorClient interface {
	Agents() (*mesos_master.Response_GetAgents, error)
	GetTasksAllocation(ID string) ([]*mesos.Resource, []*mesos.Resource, error)
	AllocatedResources(ID string) ([]*mesos.Resource, error)
	GetMaintenanceSchedule() (*mesos_master.Response_GetMaintenanceSchedule, error)
	GetMaintenanceStatus() (*mesos_master.Response_GetMaintenanceStatus, error)
	StartMaintenance([]*mesos.MachineID) error
	StopMaintenance([]*mesos.MachineID) error
	GetQuota(role string) ([]*mesos.Resource, error)
	UpdateMaintenanceSchedule(*mesos_v1_maintenance.Schedule) error
}

type masterOperatorClient struct {
	cfg         transport.ClientConfig
	contentType string
}

// NewMasterOperatorClient builds a new Mesos Master Operator JSON client.
func NewMasterOperatorClient(
	c transport.ClientConfig,
	contentType string) MasterOperatorClient {
	return &masterOperatorClient{
		cfg:         c,
		contentType: contentType,
	}
}

// Makes the actual RPC call and returns Master operator API response
func (mo *masterOperatorClient) call(ctx context.Context, msg *mesos_master.Call) (
	*mesos_master.Response, error) {
	// Create Headers
	headers := transport.NewHeaders().
		With("Content-Type", fmt.Sprintf("application/%s", mo.contentType)).
		With("Accept", fmt.Sprintf("application/%s", mo.contentType))

	// Create pb Request
	reqBody, err := MarshalPbMessage(msg, mo.contentType)
	if err != nil {
		errMsg := fmt.Sprintf(
			"failed to marshal %s call",
			_procedure)
		log.WithError(err).Error(errMsg)
		return nil, errors.Wrap(err, errMsg)
	}
	// Create yarpc Request
	tReq := transport.Request{
		Caller:    mo.cfg.Caller(),
		Service:   mo.cfg.Service(),
		Encoding:  Encoding,
		Procedure: _procedure,
		Headers:   transport.Headers(headers),
		Body:      strings.NewReader(reqBody),
	}

	resp, err := mo.cfg.GetUnaryOutbound().Call(ctx, &tReq)
	if err != nil {
		errMsg := fmt.Sprintf("error making call %s", _procedure)
		log.WithError(err).Error(errMsg)
		return nil, errors.Wrap(err, errMsg)
	}

	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		errMsg := "error reading response body"
		log.WithError(err).Error(errMsg)
		return nil, errors.Wrap(err, errMsg)
	}

	respMsg := &mesos_master.Response{}
	err = proto.Unmarshal(bodyBytes, respMsg)

	if err != nil {
		errMsg := "error unmarshal response body"
		log.WithError(err).Error(errMsg)
		return nil, errors.Wrap(err, errMsg)
	}

	return respMsg, nil

}

// Agents returns all agents from Mesos master with the `GetAgents` API.
func (mo *masterOperatorClient) Agents() (
	*mesos_master.Response_GetAgents, error) {
	// Set the CALL TYPE
	callType := mesos_master.Call_GET_AGENTS

	masterMsg := &mesos_master.Call{
		Type: &callType,
	}

	// Create context to cancel automatically when Timeout expires
	ctx, cancel := context.WithTimeout(
		context.Background(), _timeout,
	)

	defer cancel()

	// Make Call
	response, err := mo.call(ctx, masterMsg)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Fetch GetAgents result
	getAgents := response.GetGetAgents()

	if getAgents == nil {
		return nil, errors.New("no agents returned from get agents call")
	}

	return getAgents, nil
}

// GetTasksAllocation returns resources for Peloton framework
// allocatedResources: actual resource allocated for task + offered resources
// offeredResources: offered resources are also assumed to be allocated to Peloton framework
// by Mesos Master
func (mo *masterOperatorClient) GetTasksAllocation(
	ID string) ([]*mesos.Resource, []*mesos.Resource, error) {
	var allocatedResources, offeredResources []*mesos.Resource

	// Set the CALL TYPE
	callType := mesos_master.Call_GET_FRAMEWORKS

	masterMsg := &mesos_master.Call{
		Type: &callType,
	}

	// Create context to cancel automatically when Timeout expires
	ctx, cancel := context.WithTimeout(
		context.Background(), _timeout,
	)

	defer cancel()

	// Make Call
	response, err := mo.call(ctx, masterMsg)
	if err != nil {
		return allocatedResources, offeredResources, errors.WithStack(err)
	}

	for _, framework := range response.GetGetFrameworks().GetFrameworks() {
		if ID != framework.GetFrameworkInfo().GetId().GetValue() {
			continue
		}

		allocatedResources = append(allocatedResources, framework.GetAllocatedResources()...)
		offeredResources = append(offeredResources, framework.GetOfferedResources()...)
	}

	return allocatedResources, offeredResources, nil
}

// AllocatedResources returns the roles information from the Master Operator API
func (mo *masterOperatorClient) AllocatedResources(ID string) (
	[]*mesos.Resource, error) {

	var mClusterResources []*mesos.Resource

	// Validate FrameworkID
	if len(ID) == 0 {
		return mClusterResources, errors.New(
			"frameworkID cannot be empty")
	}

	// Set the CALL TYPE
	callType := mesos_master.Call_GET_ROLES

	masterMsg := &mesos_master.Call{
		Type: &callType,
	}

	// Create context to cancel automatically when Timeout expires
	ctx, cancel := context.WithTimeout(
		context.Background(), _timeout,
	)

	defer cancel()

	// Make Call
	response, err := mo.call(ctx, masterMsg)
	if err != nil {
		return mClusterResources, errors.WithStack(err)
	}

	// Fetch Roles
	roles := response.GetGetRoles()

	if roles != nil {
		// Filter Roles
		filteredRoles := filterRoles(roles.Roles, func(role *mesos.Role) bool {
			// We filter out roles which are specific to Peloton
			for _, actualFrameWorkID := range role.Frameworks {
				// Match Expected vs Actual FrameWorkID
				if ID == *actualFrameWorkID.Value {
					return true
				}
			}
			return false
		})

		// Check if resources configured
		if len(filteredRoles) == 0 {
			return nil, errors.New("no resources configured")
		}

		// Extract Resources from roles
		for _, role := range filteredRoles {
			mClusterResources = append(
				mClusterResources,
				role.Resources...)
		}

		return mClusterResources, nil
	}

	return mClusterResources, errors.New("no resources fetched")
}

type rolesFilterFn func(*mesos.Role) bool

// Filters Roles based on the provided filter
func filterRoles(roles []*mesos.Role, fn rolesFilterFn) []*mesos.Role {
	filteredRoles := make([]*mesos.Role, 0, len(roles))

	for _, role := range roles {
		if fn(role) {
			filteredRoles = append(filteredRoles, role)
		}
	}
	return filteredRoles
}

// GetMaintenanceSchedule returns the current Mesos Maintenance Schedule
func (mo *masterOperatorClient) GetMaintenanceSchedule() (*mesos_master.Response_GetMaintenanceSchedule, error) {
	// Set the CALL TYPE
	callType := mesos_master.Call_GET_MAINTENANCE_SCHEDULE

	masterMsg := &mesos_master.Call{
		Type: &callType,
	}

	// Create context to cancel automatically when Timeout expires
	ctx, cancel := context.WithTimeout(
		context.Background(), _timeout,
	)

	defer cancel()

	// Make Call
	response, err := mo.call(ctx, masterMsg)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return response.GetGetMaintenanceSchedule(), nil
}

// UpdateMaintenanceSchedule updates the Mesos Maintenance Schedule
func (mo *masterOperatorClient) UpdateMaintenanceSchedule(schedule *mesos_v1_maintenance.Schedule) error {
	// Set the CALL TYPE
	callType := mesos_master.Call_UPDATE_MAINTENANCE_SCHEDULE

	updateMaintenanceSchedule := &mesos_master.Call_UpdateMaintenanceSchedule{
		Schedule: schedule,
	}
	masterMsg := &mesos_master.Call{
		Type:                      &callType,
		UpdateMaintenanceSchedule: updateMaintenanceSchedule,
	}

	// Create context to cancel automatically when Timeout expires
	ctx, cancel := context.WithTimeout(
		context.Background(), _timeout,
	)

	defer cancel()

	// Make Call
	_, err := mo.call(ctx, masterMsg)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// GetMaintenanceStatus returns the current Mesos Cluster Status
func (mo *masterOperatorClient) GetMaintenanceStatus() (*mesos_master.Response_GetMaintenanceStatus, error) {
	// Set the CALL TYPE
	callType := mesos_master.Call_GET_MAINTENANCE_STATUS

	masterMsg := &mesos_master.Call{
		Type: &callType,
	}

	// Create context to cancel automatically when Timeout expires
	ctx, cancel := context.WithTimeout(
		context.Background(), _timeout,
	)

	defer cancel()

	// Make Call
	response, err := mo.call(ctx, masterMsg)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return response.GetGetMaintenanceStatus(), nil
}

// StartMaintenance brings 'DOWN' the specified machines by un-registering, from Mesos
// Master, the agents running on these machines. Any agents on machines in maintenance
// are also prevented from reregistering with the master in the future until
// maintenance is completed and the machine is brought back up (by StopMaintenance).
func (mo *masterOperatorClient) StartMaintenance(machines []*mesos.MachineID) error {
	// Set the CALL TYPE
	callType := mesos_master.Call_START_MAINTENANCE

	startMaintenance := &mesos_master.Call_StartMaintenance{
		Machines: machines,
	}
	masterMsg := &mesos_master.Call{
		Type:             &callType,
		StartMaintenance: startMaintenance,
	}

	// Create context to cancel automatically when Timeout expires
	ctx, cancel := context.WithTimeout(
		context.Background(), _timeout,
	)

	defer cancel()

	// Make Call
	_, err := mo.call(ctx, masterMsg)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// StopMaintenance brings the specified machines back 'UP'
func (mo *masterOperatorClient) StopMaintenance(machines []*mesos.MachineID) error {
	// Set the CALL TYPE
	callType := mesos_master.Call_STOP_MAINTENANCE

	stopMaintenance := &mesos_master.Call_StopMaintenance{
		Machines: machines,
	}
	masterMsg := &mesos_master.Call{
		Type:            &callType,
		StopMaintenance: stopMaintenance,
	}

	// Create context to cancel automatically when Timeout expires
	ctx, cancel := context.WithTimeout(
		context.Background(), _timeout,
	)

	defer cancel()

	// Make Call
	_, err := mo.call(ctx, masterMsg)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// GetQuota returns the quota set for specified role
func (mo *masterOperatorClient) GetQuota(role string) (
	[]*mesos.Resource, error) {

	// GetQuota call only has `Call.Type` and no embedded message.
	callType := mesos_master.Call_GET_QUOTA

	masterMsg := &mesos_master.Call{
		Type: &callType,
	}

	// Create context to cancel automatically when Timeout expires
	ctx, cancel := context.WithTimeout(
		context.Background(), _timeout,
	)
	defer cancel()

	// Make Call
	response, err := mo.call(ctx, masterMsg)

	if err != nil {
		return nil, errors.WithStack(err)
	}

	quota := response.GetGetQuota()

	if quota != nil {
		quotaInfo := quota.GetStatus().GetInfos()
		for _, info := range quotaInfo {
			if role == info.GetRole() {
				// return specified roles quota
				return info.GetGuarantee(), nil
			}
		}
	}
	return nil, nil
}
