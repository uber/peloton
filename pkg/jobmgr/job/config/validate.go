// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jobconfig

import (
	"errors"
	"fmt"
	"reflect"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/common/taskconfig"

	"github.com/hashicorp/go-multierror"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	_updateNotSupported = "updating %s not supported"
	// Max retries on task failures.
	_maxTaskRetries = 100
)

var (
	errPortNameMissing = yarpcerrors.InvalidArgumentErrorf(
		"Port name is missing")
	errPortEnvNameMissing = yarpcerrors.InvalidArgumentErrorf(
		"Env name is missing for dynamic port")
	errMaxInstancesTooBig = yarpcerrors.InvalidArgumentErrorf(
		"Job specified MaximumRunningInstances > InstanceCount")
	errIncorrectMaxInstancesSLA = yarpcerrors.InvalidArgumentErrorf(
		"MaximumRunningInstances should be 0 for stateless job")
	errMinInstancesTooBig = yarpcerrors.InvalidArgumentErrorf(
		"Job specified MinimumRunningInstances > MaximumRunningInstances")
	errIncorrectMinInstancesSLA = yarpcerrors.InvalidArgumentErrorf(
		"MinimumRunningInstances should be 0 for stateless job")
	errIncorrectMaxRunningTimeSLA = yarpcerrors.InvalidArgumentErrorf(
		"MaxRunningTime should be 0 for stateless job")
	errKillOnPreemptNotFalse = yarpcerrors.InvalidArgumentErrorf(
		"Task preemption policy should be false for stateless job")
	errIncorrectHealthCheck = yarpcerrors.InvalidArgumentErrorf(
		"Batch job task should not set health check ")
	errIncorrectExecutor = yarpcerrors.InvalidArgumentErrorf(
		"Batch job task should not include executor config")
	errIncorrectExecutorType = yarpcerrors.InvalidArgumentErrorf(
		"Unsupported type in executor config")
	errExecutorConfigDataNotPresent = errors.New(
		"Data field not set in executor config")
	errIncorrectRevocableSLA = yarpcerrors.InvalidArgumentErrorf(
		"revocable job must be preemptible")
	errInvalidPreemptionOverride = yarpcerrors.InvalidArgumentErrorf(
		"can't override the preemption policy of a task" +
			" which is going to be a part of a gang having tasks with" +
			" a different preemption policy")

	_jobTypeTaskValidate = map[job.JobType]func(*task.TaskConfig) error{
		job.JobType_BATCH:   validateBatchTaskConfig,
		job.JobType_SERVICE: validateStatelessTaskConfig,
	}

	_jobTypeJobValidate = map[job.JobType]func(*job.JobConfig) error{
		job.JobType_BATCH:   validateBatchJobConfig,
		job.JobType_SERVICE: validateStatelessJobConfig,
	}
)

// ValidateConfig validates the job and instance specific configs
func ValidateConfig(jobConfig *job.JobConfig, maxTasksPerJob uint32) error {
	return validateTaskConfigWithRange(
		jobConfig,
		maxTasksPerJob,
		0, jobConfig.InstanceCount,
	)
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
	if err := validateTaskConfigWithRange(newConfig,
		maxTasksPerJob,
		oldConfig.InstanceCount,
		newConfig.InstanceCount); err != nil {
		errs = multierror.Append(errs, err)
	}

	return errs.ErrorOrNil()
}

// validateTaskConfigWithRange validates jobConfig with instancesNumber within [from, to)
func validateTaskConfigWithRange(jobConfig *job.JobConfig, maxTasksPerJob uint32, from uint32, to uint32) error {

	// validate job type
	jobType := jobConfig.GetType()
	validator, ok := _jobTypeJobValidate[jobType]
	if !ok {
		return yarpcerrors.InvalidArgumentErrorf("invalid job type: %v", jobType)
	}

	// validate job config based on type
	if err := validator(jobConfig); err != nil {
		return err
	}

	// validate ports
	defaultConfig := jobConfig.GetDefaultConfig()
	if err := validatePortConfig(defaultConfig); err != nil {
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
		err := yarpcerrors.InvalidArgumentErrorf(
			"Requested tasks: %v for job is greater than supported: %v tasks/job",
			jobConfig.InstanceCount, maxTasksPerJob)
		return err
	}

	// validate task config
	for i := from; i < to; i++ {
		taskConfig := taskconfig.Merge(
			defaultConfig, jobConfig.GetInstanceConfig()[i])
		if taskConfig == nil {
			return yarpcerrors.InvalidArgumentErrorf(
				"missing task config for instance %v", i)
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

		if err := validatePortConfig(taskConfig); err != nil {
			return errInvalidTaskConfig(i, err)
		}

		if taskConfig.GetCommand() == nil {
			return yarpcerrors.InvalidArgumentErrorf("missing command info for instance %v", i)
		}

		if taskConfig.GetController() && i != 0 {
			return yarpcerrors.InvalidArgumentErrorf("only task 0 can be controller task")
		}

		if err := _jobTypeTaskValidate[jobType](taskConfig); err != nil {
			return errInvalidTaskConfig(i, err)
		}

		if err := validatePreemptionPolicy(i, taskConfig, jobConfig); err != nil {
			return errInvalidTaskConfig(i, err)
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

func errInvalidTaskConfig(instanceID uint32, err error) error {
	return yarpcerrors.InvalidArgumentErrorf(
		"Invalid config for instance %v, %v", instanceID, err)
}

// validatePreemptionPolicy validates the tasks preemption policy override
func validatePreemptionPolicy(instanceID uint32, taskConfig *task.TaskConfig,
	jobConfig *job.JobConfig) error {

	var taskPreemptible bool
	switch taskConfig.GetPreemptionPolicy().GetType() {
	case task.PreemptionPolicy_TYPE_INVALID:
		// nothing to do
		return nil
	case task.PreemptionPolicy_TYPE_PREEMPTIBLE:
		taskPreemptible = true
	case task.PreemptionPolicy_TYPE_NON_PREEMPTIBLE:
		taskPreemptible = false
	}

	jobPreemptible := jobConfig.GetSLA().GetPreemptible()

	if taskPreemptible != jobPreemptible {
		// Override is only valid if the task's instance id is more than
		// the job's minimum running instances.
		// Otherwise we can end up with a gang which has both preemptible
		// and non-preemptible tasks.
		if instanceID < jobConfig.GetSLA().GetMinimumRunningInstances() {
			return errInvalidPreemptionOverride
		}
	}
	return nil
}

// validatePortConfig checks port name and port env name exists for dynamic port.
func validatePortConfig(taskConfig *task.TaskConfig) error {
	portConfigs := taskConfig.GetPorts()
	customExecutor := taskConfig.GetExecutor().GetType() == mesos.ExecutorInfo_CUSTOM
	for _, port := range portConfigs {
		if len(port.GetName()) == 0 {
			return errPortNameMissing
		}
		if !customExecutor && port.GetValue() == 0 && len(port.GetEnvName()) == 0 {
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
	// Batch jobs should not use custom executor (aurora thermos for now)
	if taskConfig.GetExecutor() != nil {
		return errIncorrectExecutor
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
	// Only support custom executor use case for now
	if taskConfig.GetExecutor() != nil &&
		taskConfig.GetExecutor().GetType() != mesos.ExecutorInfo_CUSTOM {
		return errIncorrectExecutorType
	}
	// Validate data field present in the executor config for stateless
	if taskConfig.GetExecutor() != nil &&
		len(taskConfig.GetExecutor().GetData()) == 0 {
		return errExecutorConfigDataNotPresent
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

	if configSLA.GetRevocable() == true &&
		configSLA.GetPreemptible() != true {
		return errIncorrectRevocableSLA
	}

	return nil
}
