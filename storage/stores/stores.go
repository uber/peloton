package stores

import (
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/storage/cassandra"
	storage_config "code.uber.internal/infra/peloton/storage/config"
	"code.uber.internal/infra/peloton/storage/mysql"
	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
)

// CreateStores creates all stores that is needed by peloton
func CreateStores(
	cfg *storage_config.Config, rootScope tally.Scope) (
	storage.JobStore,
	storage.TaskStore,
	storage.ResourcePoolStore,
	storage.FrameworkInfoStore,
	storage.PersistentVolumeStore) {

	var jobStore storage.JobStore
	var taskStore storage.TaskStore
	var resourcePoolStore storage.ResourcePoolStore
	var frameworkInfoStore storage.FrameworkInfoStore
	var volumeStore storage.PersistentVolumeStore

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

		jobStore = store
		taskStore = store
		resourcePoolStore = store
		frameworkInfoStore = store
		volumeStore = store
	} else {
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

		jobStore = store
		taskStore = store
		resourcePoolStore = store
		frameworkInfoStore = store
		volumeStore = store
	}
	return jobStore, taskStore, resourcePoolStore, frameworkInfoStore, volumeStore
}
