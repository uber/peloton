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

package leader

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/uber/peloton/pkg/common"

	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/zookeeper"
	log "github.com/sirupsen/logrus"
)

// Discovery is the service discovery interface for Peloton clients
type Discovery interface {

	// Returns the app URL for a given Peloton role such as
	// peloton-jobmgr, peloton-resmgr or peloton-hostmgr etc.
	GetAppURL(role string) (*url.URL, error)
}

// NewStaticServiceDiscovery creates a staticDiscovery object
func NewStaticServiceDiscovery(
	jobmgrURL *url.URL,
	resmgrURL *url.URL,
	hostmgrURL *url.URL) (Discovery, error) {

	discovery := &staticDiscovery{
		jobmgrURL:  jobmgrURL,
		resmgrURL:  resmgrURL,
		hostmgrURL: hostmgrURL,
	}
	discovery.jobmgrURL.Host = jobmgrURL.String()
	discovery.resmgrURL.Host = resmgrURL.String()
	discovery.hostmgrURL.Host = hostmgrURL.String()
	return discovery, nil
}

// staticDiscovery is the static implementation of Discovery
type staticDiscovery struct {
	jobmgrURL  *url.URL
	resmgrURL  *url.URL
	hostmgrURL *url.URL
}

// GetAppURL returns the app URL for a given Peloton role
func (s *staticDiscovery) GetAppURL(role string) (*url.URL, error) {
	switch role {
	case common.JobManagerRole:
		return s.jobmgrURL, nil
	case common.ResourceManagerRole:
		return s.resmgrURL, nil
	case common.HostManagerRole:
		return s.hostmgrURL, nil
	default:
		return nil, fmt.Errorf("invalid Peloton role %s", role)
	}
}

// NewZkServiceDiscovery creates a zkDiscovery object
func NewZkServiceDiscovery(
	zkServers []string,
	zkRoot string) (Discovery, error) {

	zkClient, err := zookeeper.New(
		zkServers,
		&store.Config{ConnectionTimeout: zkConnErrRetry},
	)
	if err != nil {
		return nil, err
	}

	discovery := &zkDiscovery{
		zkClient: zkClient,
		zkRoot:   zkRoot,
	}
	return discovery, nil
}

// zkDiscovery is the zk based implementation of Discovery
type zkDiscovery struct {
	zkClient store.Store
	zkRoot   string
}

// GetAppURL reads app URL from Zookeeper for a given Peloton role
func (s *zkDiscovery) GetAppURL(role string) (*url.URL, error) {
	zkPath := leaderZkPath(s.zkRoot, role)
	leader, err := s.zkClient.Get(zkPath)
	if err != nil {
		return nil, err
	}

	id := ID{}
	if err := json.Unmarshal([]byte(leader.Value), &id); err != nil {
		log.WithField("leader", leader.Value).Error("Failed to parse leader json")
		return nil, err
	}
	return &url.URL{
		Host: fmt.Sprintf("%s:%d", id.IP, id.GRPCPort),
	}, nil
}
