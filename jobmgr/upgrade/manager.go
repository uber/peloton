package upgrade

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/upgrade"
	"code.uber.internal/infra/peloton/storage"
	"go.uber.org/atomic"
)

// Manager of upgrades, and how they are progressed. The manager will fetch
// workflows from the storage periodicely, such that no workflow is ever
// dropped.
type Manager interface {
	// AddUpgrade to the upgrade manager. When added to the manager, it will
	// process the upgrade until terminated.
	AddUpgrade(upgrade *upgrade.UpgradeInfo) error

	// Start the upgrade manager.
	Start() error

	// Stop the upgrade manager.
	Stop() error
}

// NewManager from the provided config.
func NewManager(
	jobStore storage.JobStore, /*
		upgradeStore storage.UpgradeStore*/
	cfg Config) Manager {
	cfg.normalize()
	return &manager{
		jobStore: jobStore,
		// upgradeStore: upgradeStore,
		running:        atomic.NewBool(false),
		stopChan:       make(chan struct{}),
		progressTicker: time.NewTicker(cfg.ProgressInterval),
		reloadTicker:   time.NewTicker(cfg.ReloadInterval),
	}
}

type manager struct {
	sync.Mutex

	jobStore storage.JobStore
	// upgradeStore storage.UpgradeStore

	running  *atomic.Bool
	stopChan chan struct{}

	progressTicker *time.Ticker
	reloadTicker   *time.Ticker
	upgrades       map[string]*upgradeState
}

func (m *manager) AddUpgrade(info *upgrade.UpgradeInfo) error {
	m.Lock()
	defer m.Unlock()

	id := info.GetWorkflowId().GetValue()
	if _, ok := m.upgrades[id]; ok {
		return fmt.Errorf("upgrade already in progress")
	}

	m.upgrades[id] = &upgradeState{
		info: info,
	}

	return nil
}

func (m *manager) Start() error {
	if m.running.Swap(true) {
		return fmt.Errorf("Upgrade Manager is already running")
	}

	go m.run()

	return nil
}

func (m *manager) Stop() error {
	if !m.running.Swap(false) {
		return fmt.Errorf("Upgrade Manager is not running")
	}

	// Block until ack'ed.
	m.stopChan <- struct{}{}

	return nil
}

func (m *manager) run() {
	log.Info("started upgrade manager")
	for {
		select {
		case <-m.reloadTicker.C:
			m.reloadUpgrades()

		case <-m.progressTicker.C:
			m.progressUpgrades()

		case <-m.stopChan:
			return
		}
	}
}

func (m *manager) reloadUpgrades() {
	// TODO: Read workflows from config and AddUpgrade.
}

func (m *manager) progressUpgrades() {
	m.Lock()
	defer m.Unlock()

	for id, us := range m.upgrades {
		switch us.info.State {
		default:
			log.WithField("upgrade_id", id).
				Warnf("unhandled upgrade state %v", us.info.State)
		}
	}
}

type upgradeState struct {
	info *upgrade.UpgradeInfo
}
