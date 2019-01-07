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

package aurorabridge

import (
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/uber/peloton/common"
	"github.com/uber/peloton/leader"
)

// Server contains all structs necessary to run a aurorabrdige server.
// This struct also implements leader.Node interface so that it can
// perform leader election among multiple aurorabridge server
// instances.
type Server struct {
	sync.Mutex

	ID   string
	role string
}

// NewServer creates a aurorabridge Server instance.
func NewServer(
	httpPort int) *Server {
	endpoint := leader.NewEndpoint(httpPort)
	additionalEndpoints := make(map[string]leader.Endpoint)
	additionalEndpoints["http"] = endpoint

	return &Server{
		ID:   leader.NewServiceInstance(endpoint, additionalEndpoints),
		role: common.PelotonAuroraBridgeRole,
	}
}

// GainedLeadershipCallback is the callback when the current node
// becomes the leader
func (s *Server) GainedLeadershipCallback() error {
	log.WithFields(log.Fields{"role": s.role}).Info("Gained leadership")

	return nil
}

// LostLeadershipCallback is the callback when the current node lost
// leadership
func (s *Server) LostLeadershipCallback() error {
	log.WithField("role", s.role).Info("Lost leadership")

	return nil
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (s *Server) ShutDownCallback() error {
	log.WithFields(log.Fields{"role": s.role}).Info("Quitting election")

	return nil
}

// GetID function returns jobmgr app address.
// This implements leader.Nomination.
func (s *Server) GetID() string {
	return s.ID
}
