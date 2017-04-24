package task

import (
	"fmt"
	"reflect"

	log "github.com/Sirupsen/logrus"

	"peloton/api/job"
	"peloton/api/task"
)

// GetTaskConfig returns the task config of a given task instance by
// merging the fields in default task config and instance task config
func GetTaskConfig(
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
			(field.Kind() == reflect.Struct && field.IsNil()) ||
			(field.Kind() == reflect.Slice && field.IsNil()) {
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

// ValidateTaskConfig checks whether the task configs in a job config
// is missing or not.
func ValidateTaskConfig(jobConfig *job.JobConfig) error {

	// Check if each instance has a default or instance-specific config
	defaultConfig := jobConfig.GetDefaultConfig()
	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		taskConfig := jobConfig.GetInstanceConfig()[i]
		if taskConfig == nil && defaultConfig == nil {
			err := fmt.Errorf("Missing task config for instance %v", i)
			return err
		}
	}
	return nil
}
