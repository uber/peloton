package stores

import (
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/storage/cassandra"
	storage_config "code.uber.internal/infra/peloton/storage/config"
	"code.uber.internal/infra/peloton/storage/mysql"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// MustCreateStore creates a generic store that is needed by peloton
// and exits if store can't be created
func MustCreateStore(
	cfg *storage_config.Config, rootScope tally.Scope) storage.Store {
	if !cfg.UseCassandra {
		// Connect to mysql DB
		if err := cfg.MySQL.Connect(); err != nil {
			log.Fatalf("Could not connect to database: %+v", err)
		}
		// Migrate DB if necessary

		if cfg.AutoMigrate {
			if errs := cfg.MySQL.AutoMigrate(); errs != nil {
				log.Fatalf("Could not migrate database: %+v", errs)
			}
		}
		// Initialize framework store
		store := mysql.NewStore(cfg.MySQL, rootScope)
		store.DB.SetMaxOpenConns(cfg.DbWriteConcurrency)
		store.DB.SetMaxIdleConns(cfg.DbWriteConcurrency)
		store.DB.SetConnMaxLifetime(cfg.MySQL.ConnLifeTime)
		return store
	}
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
