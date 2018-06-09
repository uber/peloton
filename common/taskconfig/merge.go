package taskconfig

import (
	"reflect"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
)

// Merge returns the merged task config between a base and an override. The
// merge will only happen of top-level fields, not recursively.
// If any of the arguments is nil, no merge will happen, and the non-nil
// argument (if exists) is returned.
func Merge(base *task.TaskConfig, override *task.TaskConfig) *task.TaskConfig {
	if override == nil {
		return base
	}

	if base == nil {
		return override
	}

	merged := &task.TaskConfig{}

	baseVal := reflect.ValueOf(*base)
	overrideVal := reflect.ValueOf(*override)
	mergedVal := reflect.ValueOf(merged).Elem()
	for i := 0; i < baseVal.NumField(); i++ {
		field := overrideVal.Field(i)

		switch field.Kind() {
		case reflect.Bool:
			// override bool
			mergedVal.Field(i).Set(overrideVal.Field(i))
		case reflect.String:
			if field.String() == "" {
				// set to base config value if the string is empty
				mergedVal.Field(i).Set(baseVal.Field(i))
			} else {
				// merged config should have the overridden value
				mergedVal.Field(i).Set(overrideVal.Field(i))
			}
		default:
			if field.IsNil() {
				// set to base config value if the value is empty
				mergedVal.Field(i).Set(baseVal.Field(i))
			} else {
				// merged config should have the overridden value
				mergedVal.Field(i).Set(overrideVal.Field(i))
			}
		}
	}

	return merged
}
