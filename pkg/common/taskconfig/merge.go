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

package taskconfig

import (
	"reflect"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"
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
	merge(*base, *override, merged)

	return retainBaseSecretsInInstanceConfig(base, merged)
}

// MergePodSpec returns the merged v1 pod spec between a base and an override. The
// merge will only happen of top-level fields, not recursively.
// If any of the arguments is nil, no merge will happen, and the non-nil
// argument (if exists) is returned.
func MergePodSpec(base *pod.PodSpec, override *pod.PodSpec) *pod.PodSpec {
	if override == nil {
		return base
	}

	if base == nil {
		return override
	}

	merged := &pod.PodSpec{}
	merge(*base, *override, merged)

	// TODO(kevinxu): Support retaining secret volumes from base pod spec
	return merged
}

func merge(base interface{}, override interface{}, merged interface{}) {
	baseVal := reflect.ValueOf(base)
	overrideVal := reflect.ValueOf(override)
	mergedVal := reflect.ValueOf(merged).Elem()
	baseValType := baseVal.Type()

	for i := 0; i < baseVal.NumField(); i++ {
		field := overrideVal.Field(i)
		if strings.HasPrefix(baseValType.Field(i).Name, common.ReservedProtobufFieldPrefix) {
			continue
		}

		switch field.Kind() {
		case reflect.Bool:
			// override bool
			mergedVal.Field(i).Set(overrideVal.Field(i))
		case reflect.Uint32:
			if field.Uint() == 0 {
				// set to base config value if the override is 0
				mergedVal.Field(i).Set(baseVal.Field(i))
			} else {
				// merged config should have the overridden value
				mergedVal.Field(i).Set(overrideVal.Field(i))
			}
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
}

// retainBaseSecretsInInstanceConfig ensures that instance config retains all
// secrets from default config. We store secrets as secret volumes at the
// default config level for the job as part of container info.
// This works if instance config does not override the container info.
// However in some cases there is a use case for this override (ex: controller
// job and executor job use different images). In case where instance config
// overrides container info, the "merge" will blindly override the containerinfo
// in default config. So the instance which has container info in the instance
// config will never get secrets. This function ensures that even if the
// instance config overrides container info, it will still retain secrets if any
// from the default config.
func retainBaseSecretsInInstanceConfig(defaultConfig *task.TaskConfig,
	instanceConfig *task.TaskConfig) *task.TaskConfig {
	// if default config doesn't have secrets, just return
	if !util.ConfigHasSecretVolumes(defaultConfig) {
		return instanceConfig
	}

	clonedDefaultConfig := proto.Clone(defaultConfig).(*task.TaskConfig)
	secretVolumes := util.RemoveSecretVolumesFromConfig(
		clonedDefaultConfig)
	if instanceConfig.GetContainer().GetVolumes() != nil {
		for _, secretVolume := range secretVolumes {
			instanceConfig.GetContainer().Volumes = append(
				instanceConfig.GetContainer().Volumes, secretVolume)
		}
		return instanceConfig
	}

	instanceConfig.GetContainer().Volumes = secretVolumes
	return instanceConfig
}
