package updatesvc

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/storage"
	"go.uber.org/atomic"
)

// Manager of updates, and how they are progressed. The manager will fetch
// updates from the storage periodicely, such that no update is ever
// dropped.
type Manager interface {
	// AddUpdate to the update manager. When added to the manager, it will
	// process the update until terminated.
	AddUpdate(update *update.UpdateInfo) error

	// Start the update manager.
	Start() error

	// Stop the update manager.
	Stop() error
}

// NewManager from the provided config.
func NewManager(
	jobStore storage.JobStore, /*
		updateStore storage.UpdateStore*/
	cfg Config) Manager {
	cfg.normalize()
	return &manager{
		jobStore: jobStore,
		// updateStore: updateStore,
		running:        atomic.NewBool(false),
		stopChan:       make(chan struct{}),
		progressTicker: time.NewTicker(cfg.ProgressInterval),
		reloadTicker:   time.NewTicker(cfg.ReloadInterval),
	}
}

type manager struct {
	sync.Mutex

	jobStore storage.JobStore
	// updateStore storage.UpdateStore

	running  *atomic.Bool
	stopChan chan struct{}

	progressTicker *time.Ticker
	reloadTicker   *time.Ticker
	updates        map[string]*updateState
}

func (m *manager) AddUpdate(info *update.UpdateInfo) error {
	m.Lock()
	defer m.Unlock()

	id := info.GetUpdateId().GetValue()
	if _, ok := m.updates[id]; ok {
		return fmt.Errorf("update already in progress")
	}

	m.updates[id] = &updateState{
		info: info,
	}

	return nil
}

func (m *manager) Start() error {
	if m.running.Swap(true) {
		return fmt.Errorf("Update Manager is already running")
	}

	go m.run()

	return nil
}

func (m *manager) Stop() error {
	if !m.running.Swap(false) {
		return fmt.Errorf("Update Manager is not running")
	}

	// Block until ack'ed.
	m.stopChan <- struct{}{}

	return nil
}

func (m *manager) run() {
	log.Info("started update manager")
	for {
		select {
		case <-m.reloadTicker.C:
			m.reloadUpdates()

		case <-m.progressTicker.C:
			m.progressUpdates()

		case <-m.stopChan:
			return
		}
	}
}

func (m *manager) reloadUpdates() {
	// TODO: Read updates from config and AddUpdate.
}

func (m *manager) progressUpdates() {
	m.Lock()
	defer m.Unlock()

	for id, us := range m.updates {
		switch us.info.Status.State {
		default:
			log.WithField("update_id", id).
				Warnf("unhandled update state %v", us.info.Status.State)
		}
	}
}

type updateState struct {
	info *update.UpdateInfo
}
