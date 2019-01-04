package stores

import (
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"github.com/uber/peloton/storage"
	"github.com/uber/peloton/storage/cassandra"
	storage_config "github.com/uber/peloton/storage/config"
)

// MustCreateStore creates a generic store that is needed by peloton
// and exits if store can't be created
func MustCreateStore(
	cfg *storage_config.Config, rootScope tally.Scope) storage.Store {
	log.WithFields(log.Fields{
		"cassandra_connection": cfg.Cassandra.CassandraConn,
		"cassandra_config":     cfg.Cassandra,
	}).Info("Cassandra Config")
	if cfg.AutoMigrate {
		if errs := cfg.Cassandra.AutoMigrate(); errs != nil {
			log.Fatalf("Could not migrate database: %+v", errs)
		}
	}
	store, err := cassandra.NewStore(&cfg.Cassandra, rootScope)
	if err != nil {
		log.Fatalf("Could not create cassandra store: %+v", err)
	}
	return store
}
