package task

import (
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"math"
)

// Policy is the interface for calculating the next duration for the
// back off time based on its implementation.
type Policy interface {
	// GetNextBackoffDuration returns the next backoff duration
	// based on policy implementation
	GetNextBackoffDuration(task *resmgr.Task, config *Config) float64
	// IsCycleCompleted returns true/false based on backoff policy cycle.
	IsCycleCompleted(task *resmgr.Task, config *Config) bool
}

// exponentialPolicy implements the Policy interface. It calculates the next
// backoff period by number of retries ans backoff time interval
type exponentialPolicy struct {
	// timeout stores the current timeout based on exponentialPolicy
	timeOut float64
}

// GetNextBackoffDuration returns the next backoff duration
// based on policy implementation
func (p exponentialPolicy) GetNextBackoffDuration(task *resmgr.Task, config *Config) float64 {
	// if task is nil or config is nil,
	// next timeout period will be 0
	if task == nil || config == nil {
		return 0
	}
	// if PlacementRetryCycle or PlacementRetryBackoff is 0
	// then next timeout period will be 0

	if config.PlacementRetryCycle == 0 || config.PlacementRetryBackoff.Seconds() == 0 {
		return 0
	}
	// exponentialPolicy multiply the number of retries with backoff period
	// till it reaches to end of the cycle. After cycle is finished
	// backoff will start all over again
	p.timeOut = math.Mod(task.PlacementRetryCount,
		config.PlacementRetryCycle) * config.PlacementRetryBackoff.Seconds()
	return p.timeOut
}

// IsCycleCompleted returns true/false based on backoff policy cycle.
func (p exponentialPolicy) IsCycleCompleted(task *resmgr.Task, config *Config) bool {
	// Backoff cycle will return true retry count reaches to placement retry cycle.
	// else it returns false.
	if task.PlacementRetryCount > 0 && math.Mod(task.PlacementRetryCount,
		config.PlacementRetryCycle) > 0 {
		return false
	}
	return true
}
