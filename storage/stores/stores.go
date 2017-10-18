package stores

import (
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/storage/cassandra"
	storage_config "code.uber.internal/infra/peloton/storage/config"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// CreateStores creates all stores that is needed by peloton
func CreateStores(
	cfg *storage_config.Config, rootScope tally.Scope) (
	storage.JobStore,
	storage.TaskStore,
	storage.UpgradeStore,
	storage.ResourcePoolStore,
	storage.FrameworkInfoStore,
	storage.PersistentVolumeStore) {

	var jobStore storage.JobStore
	var taskStore storage.TaskStore
	var upgradeStore storage.UpgradeStore
	var resourcePoolStore storage.ResourcePoolStore
	var frameworkInfoStore storage.FrameworkInfoStore
	var volumeStore storage.PersistentVolumeStore

	if !cfg.UseCassandra {
		log.Fatal("only cassandra storage is supported")
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

	jobStore = store
	taskStore = store
	upgradeStore = store
	resourcePoolStore = store
	frameworkInfoStore = store
	volumeStore = store

	return jobStore, taskStore, upgradeStore, resourcePoolStore, frameworkInfoStore, volumeStore
}
