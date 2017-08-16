package upgrade

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/upgrade"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/storage"
)

// Manager of upgrades, and how they are progressed. The manager will fetch
// workflows from the storage periodicely, such that no workflow is ever
// dropped.
type Manager interface {
	// TrackUpgrade in the upgrade manager. When added to the manager, it will
	// process the upgrade until terminated.
	TrackUpgrade(status *upgrade.Status)

	// Start the upgrade manager.
	Start() error

	// Stop the upgrade manager.
	Stop() error
}

// NewManager from the provided config.
func NewManager(
	trackedManager tracked.Manager,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	upgradeStore storage.UpgradeStore,
	cfg Config) Manager {
	cfg.normalize()
	return &manager{
		trackedManager: trackedManager,
		jobStore:       jobStore,
		taskStore:      taskStore,
		upgradeStore:   upgradeStore,
		progressTicker: time.NewTicker(cfg.ProgressInterval),
		reloadTicker:   time.NewTicker(cfg.ReloadInterval),
		upgrades:       map[string]*upgradeState{},
	}
}

type manager struct {
	sync.Mutex

	trackedManager tracked.Manager
	jobStore       storage.JobStore
	taskStore      storage.TaskStore
	upgradeStore   storage.UpgradeStore

	stopChan chan struct{}

	progressTicker *time.Ticker
	reloadTicker   *time.Ticker
	upgrades       map[string]*upgradeState
}

func (m *manager) TrackUpgrade(status *upgrade.Status) {
	m.Lock()
	defer m.Unlock()

	id := status.GetId().GetValue()
	if _, ok := m.upgrades[id]; ok {
		return
	}

	m.upgrades[id] = &upgradeState{
		status: status,
	}
}

func (m *manager) Start() error {
	m.Lock()
	defer m.Unlock()

	if m.stopChan != nil {
		return nil
	}

	m.stopChan = make(chan struct{})

	go m.run(m.stopChan)

	return nil
}

func (m *manager) Stop() error {
	m.Lock()
	defer m.Unlock()

	if m.stopChan == nil {
		return nil
	}

	close(m.stopChan)
	m.stopChan = nil

	return nil
}

func (m *manager) run(stopChan <-chan struct{}) {
	log.Info("started upgrade manager")
	m.reloadUpgrades()

	for {
		select {
		case <-m.reloadTicker.C:
			m.reloadUpgrades()

		case <-m.progressTicker.C:
			m.progressUpgrades()

		case <-stopChan:
			return
		}
	}
}

func (m *manager) reloadUpgrades() {
	statuses, err := m.upgradeStore.GetUpgrades(context.Background())
	if err != nil {
		log.WithError(err).Error("error while loading upgrades")
	}

	for _, s := range statuses {
		m.TrackUpgrade(s)
	}
}

func (m *manager) progressUpgrades() {
	// TODO: Process upgrades in parallel, without a global lock.
	m.Lock()
	defer m.Unlock()

	for id, us := range m.upgrades {
		if err := m.processUpgrade(context.Background(), us); err != nil {
			log.
				WithField("upgrade_id", id).
				WithField("job_id", us.status.JobId.Value).
				WithError(err).
				Error("error while processing job upgrade")
		}
	}
}

func (m *manager) processUpgrade(ctx context.Context, us *upgradeState) error {
	switch us.status.State {
	case upgrade.State_SUCCEEDED:
		delete(m.upgrades, us.status.Id.Value)

	case upgrade.State_ROLLING_FORWARD:
		if err := m.ensureOptionsAreLoaded(ctx, us); err != nil {
			return err
		}

		j := m.trackedManager.GetJob(us.status.JobId)
		if j == nil {
			return fmt.Errorf("cannot upgrade untracked job")
		}

		var done uint32
		for id := uint32(0); id < us.instanceCount; id++ {
			t := j.GetTask(id)

			// TODO: We don't process "terminated" tasks.
			if t == nil {
				continue
			}

			cs := t.CurrentState()
			gs := t.GoalState()

			// Skip tasks we don't know the state for.
			if t == nil || cs.State == task.TaskState_UNKNOWN || gs.State == task.TaskGoalState_UNKNOWN_GOAL_STATE {
				continue
			}

			if gs.ConfigVersion != us.toConfigVersion {
				if err := m.setGoalStateVersion(ctx, us, id); err != nil {
					log.
						WithField("job_id", us.status.JobId.Value).
						WithField("instance_id", id).
						WithError(err).
						Warn("failed to set task goal state")
				}
				continue
			}

			if cs.ConfigVersion == gs.ConfigVersion {
				done++
			}
		}

		state := us.status.State
		if done == us.instanceCount {
			state = upgrade.State_SUCCEEDED
		}

		return m.updateUpgradeStatus(ctx, us, state, done)

	default:
		return fmt.Errorf("unhandled upgrade state %v", us.status.State)
	}

	return nil
}

func (m *manager) setGoalStateVersion(ctx context.Context, us *upgradeState, instanceID uint32) error {
	runtime, err := m.trackedManager.GetTaskRuntime(ctx, us.status.JobId, instanceID)
	if err != nil {
		return err
	}

	runtime.DesiredConfigVersion = us.toConfigVersion

	return m.trackedManager.UpdateTaskRuntime(ctx, us.status.JobId, instanceID, runtime)
}

func (m *manager) updateUpgradeStatus(ctx context.Context, us *upgradeState, state upgrade.State, tasksDone uint32) error {
	status := *us.status
	status.State = state
	status.NumTasksDone = tasksDone

	if err := m.upgradeStore.UpdateUpgradeStatus(ctx, us.status.Id, &status); err != nil {
		return err
	}

	us.status = &status
	return nil
}

func (m *manager) ensureOptionsAreLoaded(ctx context.Context, us *upgradeState) error {
	// Lazily load upgrade options and instance count.
	if us.options == nil {
		var err error
		if us.options, us.fromConfigVersion, us.toConfigVersion, err = m.upgradeStore.GetUpgradeOptions(ctx, us.status.Id); err != nil {
			return err
		}
	}

	if us.instanceCount == 0 {
		cfg, err := m.jobStore.GetJobConfig(ctx, us.status.JobId, us.toConfigVersion)
		if err != nil {
			return err
		}

		us.instanceCount = cfg.InstanceCount
	}

	return nil
}

type upgradeState struct {
	status            *upgrade.Status
	options           *upgrade.Options
	fromConfigVersion uint64
	toConfigVersion   uint64

	// TODO: This should probably be in the tracker.
	instanceCount uint32
}
