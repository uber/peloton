package mpb

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	mesos_master "code.uber.internal/infra/peloton/.gen/mesos/v1/master"

	"go.uber.org/yarpc/api/transport"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

const (
	_procedure = "Call_MasterOperator"
	_timeout   = 10 * 1000 * time.Millisecond
)

// MasterOperatorClient makes Mesos JSON requests to Mesos Master endpoint(s)
type MasterOperatorClient interface {
	Agents() (*mesos_master.Response_GetAgents, error)
	AllocatedResources(ID string) ([]*mesos.Resource, error)
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
