package config

import (
	"errors"
	"fmt"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

var (
	errPortNameMissing    = errors.New("port name is missing")
	errPortEnvNameMissing = errors.New("env name is missing for dynamic port")
	errMaxInstancesTooBig = errors.New("job specified MaximumRunningInstances > InstanceCount")
	errMInInstancesTooBig = errors.New("job specified MinimumRunningInstances > MaximumRunningInstances")
)

// validatePortConfig checks port name and port env name exists for dynamic port.
func validatePortConfig(portConfigs []*task.PortConfig) error {
	for _, port := range portConfigs {
		if len(port.GetName()) == 0 {
			return errPortNameMissing
		}
		if port.GetValue() == 0 && len(port.GetEnvName()) == 0 {
			return errPortEnvNameMissing
		}
	}
	return nil
}

// ValidateTaskConfig checks whether the task configs in a job config
// is missing or not, also validates port configs.
func ValidateTaskConfig(jobConfig *job.JobConfig, maxTasksPerJob uint32) error {

	// Check if each instance has a default or instance-specific config
	defaultConfig := jobConfig.GetDefaultConfig()
	if err := validatePortConfig(defaultConfig.GetPorts()); err != nil {
		return err
	}

	// Jobs with more than 100k tasks create large Cassandra partitions
	// of more than 100MB. These combined with Size tiered compaction strategy,
	// will trigger large partition summary files to be brought onto the heap and trigger GC.
	// GC trigger will cause read write latency spikes on Cassandra.
	// This was the root cause of the Peloton outage on 04-05-2018
	// Including this artificial limit for now till we change the data model
	// to prevent such large partitions. After changing the data model we can tweak
	// this limit from the job service config or decide to remove the limit altogether.
	if jobConfig.InstanceCount > maxTasksPerJob {
		err := fmt.Errorf("Requested tasks: %v for job is greater than supported: %v tasks/job",
			jobConfig.InstanceCount, maxTasksPerJob)
		return err
	}

	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		taskConfig := jobConfig.GetInstanceConfig()[i]
		if taskConfig == nil && defaultConfig == nil {
			err := fmt.Errorf("missing task config for instance %v", i)
			return err
		}

		// Validate port config
		if err := validatePortConfig(taskConfig.GetPorts()); err != nil {
			return err
		}

		// Validate command info
		cmd := defaultConfig.GetCommand()
		if taskConfig.GetCommand() != nil {
			cmd = taskConfig.GetCommand()
		}
		if cmd == nil {
			err := fmt.Errorf("missing command info for instance %v", i)
			return err
		}
	}

	// Validate sla max/min running instances wrt instanceCount
	instanceCount := jobConfig.InstanceCount
	maxRunningInstances := jobConfig.GetSla().GetMaximumRunningInstances()
	if maxRunningInstances == 0 {
		maxRunningInstances = instanceCount
	} else if maxRunningInstances > instanceCount {
		return errMaxInstancesTooBig
	}
	minRunningInstances := jobConfig.GetSla().GetMinimumRunningInstances()
	if minRunningInstances > maxRunningInstances {
		return errMaxInstancesTooBig
	}

	return nil
}
