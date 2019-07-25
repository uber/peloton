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

package cassandra

import (
	"time"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

const (
	defaultConnectionsPerHost = 3
	// defaultTimeout is overwritten by timeout provided
	// in cassandra config. Config values for this were bumped to 20s
	// In case any new component doesn't have this set in config,
	// it is good to keep it consistent.
	defaultTimeout           = 20000 * time.Millisecond
	defaultProtoVersion      = 3
	defaultConsistency       = "LOCAL_QUORUM"
	defaultSerialConsistency = "LOCAL_SERIAL"
	defaultSocketKeepAlive   = 30 * time.Second
	defaultPageSize          = 1000
	defaultConcurrency       = 1000
	defaultPort              = 9042
)

// NewCluster returns a clusterConfig object
func newCluster(storeConfig *CassandraConn) *gocql.ClusterConfig {

	config := storeConfig
	cluster := gocql.NewCluster(config.ContactPoints...)

	consistency := config.Consistency
	if consistency == "" {
		consistency = defaultConsistency
	}
	cluster.Consistency = gocql.ParseConsistency(consistency)

	serialConsistency := config.SerialConsistency
	if serialConsistency == "" || serialConsistency == defaultSerialConsistency {
		cluster.SerialConsistency = gocql.LocalSerial
	} else {
		cluster.SerialConsistency = gocql.Serial
	}

	cluster.Timeout = config.Timeout
	if cluster.Timeout == 0 {
		cluster.Timeout = defaultTimeout
	}

	cluster.NumConns = config.ConnectionsPerHost
	if cluster.NumConns == 0 {
		cluster.NumConns = defaultConnectionsPerHost
	}

	cluster.ProtoVersion = config.ProtoVersion
	if cluster.ProtoVersion == 0 {
		cluster.ProtoVersion = defaultProtoVersion
	} else if cluster.ProtoVersion != 3 {
		log.Warn("protocol version 2/4 is not compatible between " +
			"2.2.x and 3.y. use 3 instead.")
	}

	cluster.SocketKeepalive = config.SocketKeepalive
	if cluster.SocketKeepalive == 0 {
		cluster.SocketKeepalive = defaultSocketKeepAlive
	}

	cluster.PageSize = config.PageSize
	if cluster.PageSize == 0 {
		cluster.PageSize = defaultPageSize
	}

	cluster.Port = config.Port
	if cluster.Port == 0 {
		cluster.Port = defaultPort
	}

	dc := config.DataCenter
	if dc != "" {
		cluster.HostFilter = gocql.DataCentreHostFilter(dc)
	}

	if config.HostPolicy == "TokenAwareHostPolicy" {
		if dc != "" {
			cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(dc))
		} else {
			cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
		}
	} else {
		cluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
	}

	if len(config.CQLVersion) > 0 {
		cluster.CQLVersion = config.CQLVersion
	}

	if config.RetryCount != 0 {
		cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: config.RetryCount}
	} else {
		cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}
	}

	if config.TimeoutLimit > 10 {
		gocql.TimeoutLimit = int64(config.TimeoutLimit)
	}

	return cluster
}

// CreateStoreSession is to create clusters and connections
func CreateStoreSession(
	storeConfig *CassandraConn, keySpace string) (*gocql.Session, error) {
	cluster := newCluster(storeConfig)
	cluster.Keyspace = keySpace

	if len(storeConfig.Username) != 0 {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: storeConfig.Username,
			Password: storeConfig.Password,
		}
	}

	cSession, err := cluster.CreateSession()
	if err != nil {
		log.WithError(err).Error("Fail to create C* session")
		return nil, err
	}

	log.WithFields(log.Fields{
		"key_space":      keySpace,
		"cassandra_port": storeConfig.Port,
	}).Info("ORM C* Session Created.")
	return cSession, nil
}
