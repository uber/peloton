package config

import (
	"errors"
	"fmt"
	"reflect"

	log "github.com/Sirupsen/logrus"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/util"
)

const (
	// PelotonJobID is the environment variable name for job ID
	PelotonJobID = "PELOTON_JOB_ID"
	// PelotonInstanceID is the environment variable name for instance ID
	PelotonInstanceID = "PELOTON_INSTANCE_ID"
	// PelotonTaskID is the environment variable name for task ID
	PelotonTaskID = "PELOTON_TASK_ID"
)

var (
	errPortNameMissing    = errors.New("port name is missing")
	errPortEnvNameMissing = errors.New("env name is missing for dynamic port")
	errMaxInstancesTooBig = errors.New("job specified MaximumRunningInstances > InstanceCount")
	errMInInstancesTooBig = errors.New("job specified MinimumRunningInstances > MaximumRunningInstances")
)

// GetTaskConfig returns the task config of a given task instance by
// merging the fields in default task config and instance task config
func GetTaskConfig(
	jobID *peloton.JobID,
	jobConfig *job.JobConfig,
	instanceID uint32) (*task.TaskConfig, error) {

	if instanceID >= jobConfig.InstanceCount {
		// InstanceId out of range
		errMsg := fmt.Sprintf("InstanceID %v out of range [0, %v)",
			instanceID, jobConfig.InstanceCount)
		log.Warnf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	result := task.TaskConfig{}

	// Update the task environment with Peloton specific variables
	// such as jobID, instanceID and taskID before returning the task
	// config
	defer updateEnvironment(&result, jobID, instanceID)

	// Shallow copy the default task config
	if jobConfig.GetDefaultConfig() != nil {
		result = *jobConfig.GetDefaultConfig()
	}

	// Look up the instance-specific task config
	cfgs := jobConfig.GetInstanceConfig()
	if cfgs == nil || cfgs[instanceID] == nil {
		// No instance specific config for task
		return &result, nil
	}
	cfg := cfgs[instanceID]

	// Apply instance specifc config to task
	val := reflect.ValueOf(cfg).Elem()
	for i := 0; i < val.NumField(); i++ {
		typ := val.Type().Field(i)
		field := val.Field(i)

		log.Debugf("type: %v, field: %v", typ, field)

		if (field.Kind() == reflect.String && field.String() == "") ||
			(field.Kind() != reflect.String && field.IsNil()) {
			// Ignore fields that are unset
			continue
		}
		resField := reflect.ValueOf(&result).Elem().FieldByName(typ.Name)
		if !resField.IsValid() {
			errMsg := fmt.Sprintf("Invalid field %v in task config", typ.Name)
			log.Errorf(errMsg)
			return nil, fmt.Errorf(errMsg)
		}

		if !resField.CanSet() {
			errMsg := fmt.Sprintf("Readonly field %v in task config", typ.Name)
			log.Errorf(errMsg)
			return nil, fmt.Errorf(errMsg)
		}
		resField.Set(field)
	}

	return &result, nil
}

// updateEnvironment adds the Peloton specific environment variables
// such as jobID, instanceID and taskID so that the command can access
// those information.
func updateEnvironment(
	taskConfig *task.TaskConfig,
	jobID *peloton.JobID,
	instanceID uint32) {

	// Check if the task config has command or not
	if taskConfig.GetCommand() == nil {
		log.WithField("config", taskConfig).
			Errorf("Missing command info in task config")
		return
	}

	// Add JobID, TaskID and InstanceID as environment variables
	variables := []*mesos.Environment_Variable{
		{
			Name:  util.PtrPrintf(PelotonJobID),
			Value: &jobID.Value,
		},
		{
			Name:  util.PtrPrintf(PelotonInstanceID),
			Value: util.PtrPrintf("%d", instanceID),
		},
		{
			Name:  util.PtrPrintf(PelotonTaskID),
			Value: util.PtrPrintf("%s-%d", jobID.Value, instanceID),
		},
	}

	// Make a shallow copy of the CommandInfo so that we can change
	// the environment
	cmd := *taskConfig.GetCommand()
	cmd.Environment = &mesos.Environment{
		Variables: append(cmd.GetEnvironment().GetVariables(), variables...),
	}
	taskConfig.Command = &cmd
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

// ValidateTaskConfig checks whether the task configs in a job config
// is missing or not, also validates port configs.
func ValidateTaskConfig(jobConfig *job.JobConfig) error {

	// Check if each instance has a default or instance-specific config
	defaultConfig := jobConfig.GetDefaultConfig()
	if err := validatePortConfig(defaultConfig.GetPorts()); err != nil {
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
