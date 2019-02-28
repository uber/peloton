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

package impl

import (
	"time"
)

// CassandraConn describes the properties to manage a Cassandra connection.
type CassandraConn struct {
	ContactPoints      []string      `yaml:"contactPoints"`
	Port               int           `yaml:"port"`
	Username           string        `yaml:"username"`
	Password           string        `yaml:"password"`
	Consistency        string        `yaml:"consistency"`
	ConnectionsPerHost int           `yaml:"connectionsPerHost"`
	Timeout            time.Duration `yaml:"timeout"`
	SocketKeepalive    time.Duration `yaml:"socketKeepalive"`
	ProtoVersion       int           `yaml:"protoVersion"`
	TTL                time.Duration `yaml:"ttl"`
	LocalDCOnly        bool          `yaml:"localDCOnly"` // deprecated
	DataCenter         string        `yaml:"dataCenter"`  // data center filter
	PageSize           int           `yaml:"pageSize"`
	RetryCount         int           `yaml:"retryCount"`
	HostPolicy         string        `yaml:"hostPolicy"`
	TimeoutLimit       int           `yaml:"timeoutLimit"`  // number of timeouts allowed
	CQLVersion         string        `yaml:"cqlVersion"`    // set only on C* 3.x
	MaxGoRoutines      int           `yaml:"maxGoroutines"` // a capacity limit
}
