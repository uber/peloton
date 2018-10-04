package jobconfig

import (
	"errors"
	"fmt"
	"reflect"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/common/taskconfig"

	"github.com/hashicorp/go-multierror"
)

const (
	_updateNotSupported = "updating %s not supported"
	// Max retries on task failures.
	_maxTaskRetries = 100
)

var (
	errPortNameMissing = errors.New(
		"Port name is missing")
	errPortEnvNameMissing = errors.New(
		"Env name is missing for dynamic port")
	errMaxInstancesTooBig = errors.New(
		"Job specified MaximumRunningInstances > InstanceCount")
	errIncorrectMaxInstancesSLA = errors.New(
		"MaximumRunningInstances should be 0 for stateless job")
	errMinInstancesTooBig = errors.New(
		"Job specified MinimumRunningInstances > MaximumRunningInstances")
	errIncorrectMinInstancesSLA = errors.New(
		"MinimumRunningInstances should be 0 for stateless job")
	errIncorrectMaxRunningTimeSLA = errors.New(
		"MaxRunningTime should be 0 for stateless job")
	errKillOnPreemptNotFalse = errors.New(
		"Task preemption policy should be false for stateless job")
	errIncorrectHealthCheck = errors.New(
		"Batch job task should not set health check ")

	_jobTypeTaskValidate = map[job.JobType]func(*task.TaskConfig) error{
		job.JobType_BATCH:   validateBatchTaskConfig,
		job.JobType_SERVICE: validateStatelessTaskConfig,
	}

	_jobTypeJobValidate = map[job.JobType]func(*job.JobConfig) error{
		job.JobType_BATCH:   validateBatchJobConfig,
		job.JobType_SERVICE: validateStatelessJobConfig,
	}
)

// ValidateTaskConfig checks whether the task configs in a job config
// is missing or not, also validates port configs.
func ValidateTaskConfig(jobConfig *job.JobConfig, maxTasksPerJob uint32) error {
	return validateTaskConfigWithRange(jobConfig, maxTasksPerJob, 0, jobConfig.InstanceCount)
}

// ValidateUpdatedConfig validates the changes in the new config
func ValidateUpdatedConfig(oldConfig *job.JobConfig,
	newConfig *job.JobConfig,
	maxTasksPerJob uint32) error {
	errs := new(multierror.Error)
	// Should only update config for new instances,
	// existing config should remain the same,
	// except SLA and Description
	if oldConfig.Name != newConfig.Name {
		errs = multierror.Append(errs,
			fmt.Errorf(_updateNotSupported, "Name"))
	}

	if !reflect.DeepEqual(oldConfig.Labels, newConfig.Labels) {
		errs = multierror.Append(errs,
			fmt.Errorf(_updateNotSupported, "Labels"))
	}

	if oldConfig.OwningTeam != newConfig.OwningTeam {
		errs = multierror.Append(errs,
			fmt.Errorf(_updateNotSupported, "OwningTeam"))
	}

	if oldConfig.RespoolID.GetValue() != newConfig.RespoolID.GetValue() {
		errs = multierror.Append(errs,
			fmt.Errorf(_updateNotSupported, "RespoolID"))
	}

	if oldConfig.Type != newConfig.Type {
		errs = multierror.Append(errs,
			fmt.Errorf(_updateNotSupported, "Type"))
	}

	if !reflect.DeepEqual(oldConfig.LdapGroups, newConfig.LdapGroups) {
		errs = multierror.Append(errs,
			fmt.Errorf(_updateNotSupported, "LdapGroups"))
	}
	if !reflect.DeepEqual(oldConfig.DefaultConfig, newConfig.DefaultConfig) {
		errs = multierror.Append(errs,
			fmt.Errorf(_updateNotSupported, "DefaultConfig"))
	}

	if newConfig.InstanceCount < oldConfig.InstanceCount {
		errs = multierror.Append(errs,
			errors.New("new instance count can't be less"))
	}

	// existing instance config should not be updated
	for i := uint32(0); i < oldConfig.InstanceCount; i++ {
		// no instance config in new config, skip the comparison
		if _, ok := newConfig.InstanceConfig[i]; !ok {
			continue
		}
		// either oldConfig is nil, and newConfig is non-nil, or
		// both are not nil and the configs are different
		if !reflect.DeepEqual(oldConfig.InstanceConfig[i], newConfig.InstanceConfig[i]) {
			errs = multierror.Append(errs,
				errors.New("existing instance config can't be updated"))
			break
		}
	}

	// validate the task configs of new instances
	err := validateTaskConfigWithRange(newConfig,
		maxTasksPerJob,
		oldConfig.InstanceCount,
		newConfig.InstanceCount)
	if err != nil {
		errs = multierror.Append(err)
	}

	return errs.ErrorOrNil()
}

// validateTaskConfigWithRange validates jobConfig with instancesNumber within [from, to)
func validateTaskConfigWithRange(jobConfig *job.JobConfig, maxTasksPerJob uint32, from uint32, to uint32) error {
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
		err := fmt.Errorf(
			"Requested tasks: %v for job is greater than supported: %v tasks/job",
			jobConfig.InstanceCount, maxTasksPerJob)
		return err
	}

	jobType := jobConfig.GetType()
	if err := _jobTypeJobValidate[jobType](jobConfig); err != nil {
		return err
	}

	for i := from; i < to; i++ {
		taskConfig := taskconfig.Merge(
			defaultConfig, jobConfig.GetInstanceConfig()[i])
		if taskConfig == nil {
			err := fmt.Errorf("missing task config for instance %v", i)
			return err
		}

		//TODO: uncomment the following once all Peloton clients have been
		// modified to not add these labels to jobs submitted through them

		//for _, label := range taskConfig.GetLabels() {
		//	if strings.HasPrefix(label.GetKey(), common.SystemLabelPrefix+".") {
		//		return fmt.Errorf(
		//			"keys with prefix"+
		//				" '%s' are reserved for system labels",
		//			common.SystemLabelPrefix)
		//	}
		//}

		restartPolicy := taskConfig.GetRestartPolicy()
		if restartPolicy.GetMaxFailures() > _maxTaskRetries {
			restartPolicy.MaxFailures = _maxTaskRetries
		}

		// Validate port config
		if err := validatePortConfig(taskConfig.GetPorts()); err != nil {
			return instanceConfigErrorGenerate(i, err)
		}

		// Validate command info
		if taskConfig.GetCommand() == nil {
			err := fmt.Errorf("missing command info for instance %v", i)
			return err
		}

		if taskConfig.GetController() && i != 0 {
			err := fmt.Errorf("only task 0 can be controller task")
			return err
		}

		if err := _jobTypeTaskValidate[jobType](taskConfig); err != nil {
			return instanceConfigErrorGenerate(i, err)
		}
	}

	// Validate sla max/min running instances wrt instanceCount
	instanceCount := jobConfig.InstanceCount
	maxRunningInstances := jobConfig.GetSLA().GetMaximumRunningInstances()
	if maxRunningInstances == 0 {
		maxRunningInstances = instanceCount
	} else if maxRunningInstances > instanceCount {
		return errMaxInstancesTooBig
	}
	minRunningInstances := jobConfig.GetSLA().GetMinimumRunningInstances()
	if minRunningInstances > maxRunningInstances {
		return errMinInstancesTooBig
	}

	return nil
}

func instanceConfigErrorGenerate(instanceID uint32, err error) error {
	return fmt.Errorf(
		"Invalid config for instance %v, %v", instanceID, err)
}

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

// validateBatchJobConfig validate task config for batch job
func validateBatchTaskConfig(taskConfig *task.TaskConfig) error {
	// Healthy field should not be set for batch job
	if taskConfig.GetHealthCheck() != nil {
		return errIncorrectHealthCheck
	}
	return nil
}

// validateBatchJobConfig validate jobconfig for batch job
func validateBatchJobConfig(jobConfig *job.JobConfig) error {
	return nil
}

// validateStatelessTaskConfig validate task config for stateless job
func validateStatelessTaskConfig(taskConfig *task.TaskConfig) error {
	// KillOnPreempt should be false for stateless task config
	if taskConfig.GetPreemptionPolicy().GetKillOnPreempt() != false {
		return errKillOnPreemptNotFalse
	}
	return nil
}

// validateStatelessJobConfig validate jobconfig for stateless job
func validateStatelessJobConfig(jobConfig *job.JobConfig) error {
	configSLA := jobConfig.GetSLA()

	// stateless job should not set MaximumRunningInstances
	if configSLA.GetMaximumRunningInstances() != 0 {
		return errIncorrectMaxInstancesSLA
	}

	// stateless job should not set MinimumRunningInstances
	if configSLA.GetMinimumRunningInstances() != 0 {
		return errIncorrectMinInstancesSLA
	}

	// stateless job should not set MaxRunningTime
	if configSLA.GetMaxRunningTime() != 0 {
		return errIncorrectMaxRunningTimeSLA
	}

	return nil
}
