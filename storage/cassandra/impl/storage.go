package impl

import (
	"time"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/storage/cassandra/api"
	"github.com/uber-go/tally"
)

// CreateStore is to create clusters and connections
func CreateStore(storeConfig *CassandraConn, keySpace string, scope tally.Scope) (*Store, error) {
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
		log.Error("Fail to create session: ", err.Error())
		return nil, api.ErrConnection
	}
	storeScope := scope.Tagged(map[string]string{"store": keySpace})
	cb := Store{
		keySpace:       keySpace,
		cSession:       cSession,
		scope:          storeScope,
		concurrency:    0,
		maxBatch:       50,
		maxConcurrency: int32(storeConfig.MaxGoRoutines),
		metrics:        NewMetrics(storeScope),
	}
	log.WithFields(log.Fields{
		"key_space":      keySpace,
		"store":          cb.String(),
		"cassandra_port": storeConfig.Port,
	}).Info("C* Session Created.")
	return &cb, nil
}

const (
	defaultConnectionsPerHost = 3
	defaultTimeout            = 1000 * time.Millisecond
	defaultProtoVersion       = 3
	defaultConsistency        = "LOCAL_QUORUM"
	defaultSocketKeepAlive    = 30 * time.Second
	defaultPageSize           = 1000
	defaultConcurrency        = 1000
	defaultPort               = 9042
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
			"2.2.x and 3.y. use 3 instead. https://code.uberinternal.com/T607909")
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

	if config.HostPolicy == "TokenAwareHostPolicy" {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
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

	if dc := config.DataCenter; dc != "" {
		cluster.HostFilter = gocql.DataCentreHostFilter(dc)
	}

	if config.TimeoutLimit > 10 {
		gocql.TimeoutLimit = int64(config.TimeoutLimit)
	}

	return cluster
}
