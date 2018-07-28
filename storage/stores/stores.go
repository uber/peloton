package stores

import (
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/storage/cassandra"
	storage_config "code.uber.internal/infra/peloton/storage/config"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// MustCreateStore creates a generic store that is needed by peloton
// and exits if store can't be created
func MustCreateStore(
	cfg *storage_config.Config, rootScope tally.Scope) storage.Store {
	log.Infof("cassandra Config: %v", cfg.Cassandra)
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
