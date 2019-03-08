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

package mesos

import (
	"context"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc"

	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"

	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/storage"
)

// InitManager initializes the mesosManager
func InitManager(
	d *yarpc.Dispatcher,
	mesosConfig *Config,
	store storage.FrameworkInfoStore) {

	m := mesosManager{
		store:         store,
		frameworkName: mesosConfig.Framework.Name,
	}

	for name, hdl := range getCallbacks(&m) {
		mpb.Register(d, ServiceName, mpb.Procedure(name, hdl))
	}
}

// mesosManager is a handler for Mesos scheduler API events.
type mesosManager struct {
	store         storage.FrameworkInfoStore
	frameworkName string
}

type schedulerEventCallback func(context.Context, *sched.Event) error

// getCallbacks returns a map from name to usable callbacks
// which can be registered to yarpc.Dispatcher.
func getCallbacks(m *mesosManager) map[string]schedulerEventCallback {
	procedures := make(map[string]schedulerEventCallback)
	callbacks := map[sched.Event_Type]schedulerEventCallback{
		sched.Event_SUBSCRIBED: m.Subscribed,
		sched.Event_MESSAGE:    m.Message,
		sched.Event_FAILURE:    m.Failure,
		sched.Event_ERROR:      m.Error,
		sched.Event_HEARTBEAT:  m.Heartbeat,
		sched.Event_UNKNOWN:    m.Unknown,
	}
	for typ, hdl := range callbacks {
		name := typ.String()
		procedures[name] = hdl
	}
	return procedures
}

func (m *mesosManager) Subscribed(ctx context.Context, body *sched.Event) error {
	subscribed := body.GetSubscribed()
	log.WithField("subscribed", subscribed).
		Info("mesosManager: subscribed called")
	frameworkID := subscribed.GetFrameworkId().GetValue()
	err := m.store.SetMesosFrameworkID(ctx, m.frameworkName, frameworkID)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"framework_id": frameworkID,
			"name":         m.frameworkName,
		}).Error("Failed to SetMesosFrameworkId")
	}
	return err
}

func (m *mesosManager) Message(ctx context.Context, body *sched.Event) error {
	msg := body.GetMessage()
	log.WithField("msg", msg).Debug("mesosManager: message called")
	return nil
}

func (m *mesosManager) Failure(ctx context.Context, body *sched.Event) error {
	failure := body.GetFailure()
	log.WithField("failure", failure).Debug("mesosManager: failure called")
	return nil
}

func (m *mesosManager) Error(ctx context.Context, body *sched.Event) error {
	err := body.GetError()
	log.WithField("error", err).Debug("mesosManager: error called")
	return nil
}

func (m *mesosManager) Heartbeat(ctx context.Context, body *sched.Event) error {
	log.Debug("mesosManager: heartbeat called")
	return nil
}

func (m *mesosManager) Unknown(ctx context.Context, body *sched.Event) error {
	log.WithField("event", body).Info("mesosManager: unknown event called")
	return nil
}
