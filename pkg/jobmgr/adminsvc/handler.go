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

package adminsvc

import (
	"context"

	adminsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/admin/svc"
	yarpcutil "github.com/uber/peloton/pkg/common/util/yarpc"
	"github.com/uber/peloton/pkg/jobmgr/goalstate"
	"github.com/uber/peloton/pkg/middleware/inbound"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

// InitServiceHandler initializes administrative handler
func InitServiceHandler(
	d *yarpc.Dispatcher,
	goalStateDriver goalstate.Driver,
	apiLock inbound.APILockInterface,
) {
	handler := createServiceHandler(goalStateDriver, apiLock)
	d.Register(adminsvc.BuildAdminServiceYARPCProcedures(handler))
}

func createServiceHandler(goalStateDriver goalstate.Driver, apiLock inbound.APILockInterface) *serviceHandler {
	handler := &serviceHandler{components: make(map[adminsvc.Component]lockableComponent)}

	for _, component := range createLockableComponents(goalStateDriver, apiLock) {
		handler.components[component.component()] = component
	}

	return handler
}

type serviceHandler struct {
	goalStateDriver goalstate.Driver
	components      map[adminsvc.Component]lockableComponent
}

// Lockdown locks the components requested in LockdownRequest
func (h *serviceHandler) Lockdown(
	ctx context.Context,
	request *adminsvc.LockdownRequest,
) (response *adminsvc.LockdownResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)

		if len(response.GetFailures()) != 0 && err == nil {
			err = yarpcerrors.InternalErrorf("fail to lockdown some components")
		}

		if err != nil {
			err = yarpcutil.ConvertToYARPCError(err)
			log.WithField("headers", headers).
				WithField("components", request.GetComponents()).
				WithField("response", response).
				WithError(err).
				Warn("AdminService.Lockdown failed")
			return
		}

		log.WithField("headers", headers).
			WithField("components", request.GetComponents()).
			Info("AdminService.Lockdown succeeded")
	}()

	response = &adminsvc.LockdownResponse{}

	components, err := h.getLockableComponents(request.GetComponents())
	if err != nil {
		return
	}

	for _, component := range components {
		if err := component.lock(); err != nil {
			response.Failures = append(response.Failures, &adminsvc.ComponentFailure{
				Component:      component.component(),
				FailureMessage: err.Error(),
			})
		} else {
			response.Successes = append(response.Successes, component.component())
		}
	}

	return
}

// RemoveLockdown removes locks the components requested in RemoveLockdownRequest
func (h *serviceHandler) RemoveLockdown(
	ctx context.Context,
	request *adminsvc.RemoveLockdownRequest,
) (response *adminsvc.RemoveLockdownResponse, err error) {
	defer func() {
		headers := yarpcutil.GetHeaders(ctx)

		if len(response.GetFailures()) != 0 && err == nil {
			err = yarpcerrors.InternalErrorf("fail to remove lock on some components")
		}

		if err != nil {
			err = yarpcutil.ConvertToYARPCError(err)
			log.WithField("headers", headers).
				WithField("components", request.GetComponents()).
				WithField("response", response).
				WithError(err).
				Warn("AdminService.RemoveLockdown failed")
			return
		}

		log.WithField("headers", headers).
			WithField("components", request.GetComponents()).
			Info("AdminService.RemoveLockdown succeeded")
	}()

	response = &adminsvc.RemoveLockdownResponse{}

	components, err := h.getLockableComponents(request.GetComponents())
	if err != nil {
		return
	}

	for _, component := range components {
		if err := component.unlock(); err != nil {
			response.Failures = append(response.Failures, &adminsvc.ComponentFailure{
				Component:      component.component(),
				FailureMessage: err.Error(),
			})
		} else {
			response.Successes = append(response.Successes, component.component())
		}
	}

	return
}

func (h *serviceHandler) getLockableComponents(components []adminsvc.Component) ([]lockableComponent, error) {
	var result []lockableComponent

	for _, component := range components {
		if c, ok := h.components[component]; !ok {
			return nil, yarpcerrors.InvalidArgumentErrorf("unknown component: %s", component.String())
		} else {
			result = append(result, c)
		}
	}

	return result, nil
}

func createLockableComponents(goalStateDriver goalstate.Driver, apiLock inbound.APILockInterface) []lockableComponent {
	var result []lockableComponent

	result = append(result, &goalStateComponent{driver: goalStateDriver})
	result = append(result, &readAPILockComponent{apiLock: apiLock})
	result = append(result, &writeAPILockComponent{apiLock: apiLock})
	result = append(result, &killLockComponent{lockable: goalStateDriver.GetLockable()})
	result = append(result, &launchLockComponent{lockable: goalStateDriver.GetLockable()})

	return result
}

// NewTestServiceHandler returns an empty new ServiceHandler ptr for testing.
func NewTestServiceHandler() *serviceHandler {
	return &serviceHandler{}
}
