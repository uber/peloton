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

package handler

import (
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/query"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/apachemesos"
	v1alphaquery "github.com/uber/peloton/.gen/peloton/api/v1alpha/query"
	v1alpharespool "github.com/uber/peloton/.gen/peloton/api/v1alpha/respool"
	v1alphavolume "github.com/uber/peloton/.gen/peloton/api/v1alpha/volume"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common/util"
	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

const (
	_configVersion       = uint64(2)
	_workflowVersion     = uint64(2)
	_desiredStateVersion = uint64(2)
)

var (
	testPrevMesosTaskID = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1-2"
	testMesosTaskID     = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1-3"
	testAgentID         = "agent-id"
)

type apiConverterTestSuite struct {
	suite.Suite
}

// TestConvertTaskStateToPodState tests conversion from
// v0 task.TaskState to v1alpha pod.PodState and vice versa
func (suite *apiConverterTestSuite) TestConvertTaskStateToPodStateAndViceVersa() {
	taskStates := []task.TaskState{
		task.TaskState_UNKNOWN,
		task.TaskState_INITIALIZED,
		task.TaskState_PENDING,
		task.TaskState_READY,
		task.TaskState_PLACING,
		task.TaskState_PLACED,
		task.TaskState_LAUNCHING,
		task.TaskState_LAUNCHED,
		task.TaskState_STARTING,
		task.TaskState_RUNNING,
		task.TaskState_SUCCEEDED,
		task.TaskState_FAILED,
		task.TaskState_LOST,
		task.TaskState_PREEMPTING,
		task.TaskState_KILLING,
		task.TaskState_KILLED,
		task.TaskState_DELETED,
		task.TaskState_RESERVED,
	}

	podStates := []pod.PodState{
		pod.PodState_POD_STATE_INVALID,
		pod.PodState_POD_STATE_INITIALIZED,
		pod.PodState_POD_STATE_PENDING,
		pod.PodState_POD_STATE_READY,
		pod.PodState_POD_STATE_PLACING,
		pod.PodState_POD_STATE_PLACED,
		pod.PodState_POD_STATE_LAUNCHING,
		pod.PodState_POD_STATE_LAUNCHED,
		pod.PodState_POD_STATE_STARTING,
		pod.PodState_POD_STATE_RUNNING,
		pod.PodState_POD_STATE_SUCCEEDED,
		pod.PodState_POD_STATE_FAILED,
		pod.PodState_POD_STATE_LOST,
		pod.PodState_POD_STATE_PREEMPTING,
		pod.PodState_POD_STATE_KILLING,
		pod.PodState_POD_STATE_KILLED,
		pod.PodState_POD_STATE_DELETED,
		pod.PodState_POD_STATE_RESERVED,
	}

	for i, taskState := range taskStates {
		suite.Equal(podStates[i], ConvertTaskStateToPodState(taskState))
	}

	for i, podState := range podStates {
		suite.Equal(taskStates[i], ConvertPodStateToTaskState(podState))
	}
}

// TestConvertTaskRuntimeToPodStatus tests conversion from
// v0 task.RuntimeInfo to v1alpha pod.PodStatus
func (suite *apiConverterTestSuite) TestConvertTaskRuntimeToPodStatus() {
	ports := make(map[string]uint32)
	ports["test"] = 1000
	resourceUsage := make(map[string]float64)
	resourceUsage["cpus"] = 1.0
	resourceUsage["memory"] = 2.0
	startTime := time.Now().String()
	host := "test-host"
	message := "test message"
	reason := "test reason"
	volume := "test-volume-id"
	failureCount := uint32(2)
	version := uint64(1)

	taskRuntime := &task.RuntimeInfo{
		State: task.TaskState_RUNNING,
		MesosTaskId: &mesos.TaskID{
			Value: &testMesosTaskID,
		},
		StartTime:    startTime,
		Host:         host,
		Ports:        ports,
		GoalState:    task.TaskState_SUCCEEDED,
		Message:      message,
		Reason:       reason,
		FailureCount: failureCount,
		VolumeID: &peloton.VolumeID{
			Value: volume,
		},
		ConfigVersion:        version,
		DesiredConfigVersion: version,
		AgentID: &mesos.AgentID{
			Value: &testAgentID,
		},
		Revision: &peloton.ChangeLog{
			Version:   1,
			CreatedAt: 2,
			UpdatedAt: 3,
			UpdatedBy: "peloton",
		},
		PrevMesosTaskId: &mesos.TaskID{
			Value: &testPrevMesosTaskID,
		},
		ResourceUsage: resourceUsage,
		Healthy:       task.HealthState_HEALTHY,
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &testMesosTaskID,
		},
		TerminationStatus: &task.TerminationStatus{
			Reason:   task.TerminationStatus_TERMINATION_STATUS_REASON_FAILED,
			ExitCode: 128,
			Signal:   "Broken pipe",
		},
	}

	podStatus := &pod.PodStatus{
		State: pod.PodState_POD_STATE_RUNNING,
		PodId: &v1alphapeloton.PodID{
			Value: testMesosTaskID,
		},
		StartTime: startTime,
		Host:      host,
		ContainersStatus: []*pod.ContainerStatus{
			{
				Ports: ports,
				Healthy: &pod.HealthStatus{
					State: pod.HealthState_HEALTH_STATE_HEALTHY,
				},
				Message:   message,
				Reason:    reason,
				StartTime: startTime,
				TerminationStatus: &pod.TerminationStatus{
					Reason:   pod.TerminationStatus_TERMINATION_STATUS_REASON_FAILED,
					ExitCode: 128,
					Signal:   "Broken pipe",
				},
			},
		},
		DesiredState: pod.PodState_POD_STATE_SUCCEEDED,
		Message:      message,
		Reason:       reason,
		FailureCount: failureCount,
		VolumeId: &v1alphapeloton.VolumeID{
			Value: volume,
		},
		Version:        versionutil.GetPodEntityVersion(version),
		DesiredVersion: versionutil.GetPodEntityVersion(version),
		AgentId: &mesos.AgentID{
			Value: &testAgentID,
		},
		HostId: testAgentID,
		Revision: &v1alphapeloton.Revision{
			Version:   taskRuntime.GetRevision().GetVersion(),
			CreatedAt: taskRuntime.GetRevision().GetCreatedAt(),
			UpdatedAt: taskRuntime.GetRevision().GetUpdatedAt(),
			UpdatedBy: taskRuntime.GetRevision().GetUpdatedBy(),
		},
		PrevPodId: &v1alphapeloton.PodID{
			Value: testPrevMesosTaskID,
		},
		ResourceUsage: resourceUsage,
		DesiredPodId: &v1alphapeloton.PodID{
			Value: testMesosTaskID,
		},
	}

	suite.Equal(podStatus, ConvertTaskRuntimeToPodStatus(taskRuntime))
}

// TestTaskConfigToPodSpecAndViceVersa tests conversion from
// v0 task.TaskConfig to v1alpha pod.PodSpec and vice versa
func (suite *apiConverterTestSuite) TestConvertTaskConfigToPodSpecAndViceVersa() {
	tt := []struct {
		containerType    mesos.ContainerInfo_Type
		podContainerType apachemesos.PodSpec_ContainerType
		executorType     mesos.ExecutorInfo_Type
		podExecutorType  apachemesos.PodSpec_ExecutorSpec_ExecutorType
	}{
		{
			containerType:    mesos.ContainerInfo_MESOS,
			podContainerType: apachemesos.PodSpec_CONTAINER_TYPE_MESOS,
			executorType:     mesos.ExecutorInfo_CUSTOM,
			podExecutorType:  apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM,
		},
		{
			containerType:    mesos.ContainerInfo_DOCKER,
			podContainerType: apachemesos.PodSpec_CONTAINER_TYPE_DOCKER,
			executorType:     mesos.ExecutorInfo_DEFAULT,
			podExecutorType:  apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_DEFAULT,
		},
		{
			containerType:    mesos.ContainerInfo_MESOS,
			podContainerType: apachemesos.PodSpec_CONTAINER_TYPE_MESOS,
			executorType:     mesos.ExecutorInfo_UNKNOWN,
			podExecutorType:  apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_INVALID,
		},
	}

	for _, test := range tt {
		taskName := "test-task"
		label := &peloton.Label{
			Key:   "test-key",
			Value: "test-value",
		}

		testCmd := "echo test"
		testImage := "test-image"

		var volumes []*mesos.Volume
		volumeMode := mesos.Volume_RO
		containerPath := "/container/path"
		hostPath := "/host/path"
		volume := &mesos.Volume{
			Mode:          &volumeMode,
			ContainerPath: &containerPath,
			HostPath:      &hostPath,
		}
		volumes = append(volumes, volume)

		var podVolumes []*v1alphavolume.VolumeSpec
		podVolume := &v1alphavolume.VolumeSpec{
			Name: hostPath,
			Type: v1alphavolume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
			HostPath: &v1alphavolume.VolumeSpec_HostPathVolumeSource{
				Path: hostPath,
			},
		}
		podVolumes = append(podVolumes, podVolume)

		var containerVolumeMounts []*pod.VolumeMount
		containerVolumeMount := &pod.VolumeMount{
			Name:      hostPath,
			ReadOnly:  true,
			MountPath: containerPath,
		}
		containerVolumeMounts = append(containerVolumeMounts, containerVolumeMount)

		var dockerParameters []*mesos.Parameter
		dockerKey := "docker-key"
		dockerValue := "docker-value"
		dockerParameter := &mesos.Parameter{
			Key:   &dockerKey,
			Value: &dockerValue,
		}
		dockerParameters = append(dockerParameters, dockerParameter)

		var podDockerParameters []*apachemesos.PodSpec_DockerParameter
		podDockerParameter := &apachemesos.PodSpec_DockerParameter{
			Key:   dockerKey,
			Value: dockerValue,
		}
		podDockerParameters = append(podDockerParameters, podDockerParameter)

		var uris []*mesos.CommandInfo_URI
		uriValue := "uri-path"
		uriExecutable := true
		uriExtract := true
		uriCache := true
		uriOutputFile := "uri-file"
		uri := &mesos.CommandInfo_URI{
			Value:      &uriValue,
			Executable: &uriExecutable,
			Extract:    &uriExtract,
			Cache:      &uriCache,
			OutputFile: &uriOutputFile,
		}
		uris = append(uris, uri)

		var podUris []*apachemesos.PodSpec_URI
		podUri := &apachemesos.PodSpec_URI{
			Value:      uriValue,
			Executable: uriExecutable,
			Extract:    uriExtract,
			Cache:      uriCache,
			OutputFile: uriOutputFile,
		}
		podUris = append(podUris, podUri)

		var envVars []*mesos.Environment_Variable
		envName := "env"
		envValue := "staging"
		envType := mesos.Environment_Variable_VALUE
		envVar := &mesos.Environment_Variable{
			Name:  &envName,
			Value: &envValue,
			Type:  &envType,
		}
		envVars = append(envVars, envVar)

		var podEnvironments []*pod.Environment
		podEnvironment := &pod.Environment{
			Name:  envName,
			Value: envValue,
		}
		podEnvironments = append(podEnvironments, podEnvironment)

		shellCmd := true
		arguments := []string{"-v", "-j"}

		portName := "test-port"
		portValue := uint32(5292)

		executorIDValue := "executor_id"
		executorID := mesos.ExecutorID{
			Value: &executorIDValue,
		}

		jobID := uuid.New()
		instanceID := uint32(0)

		taskConfig := &task.TaskConfig{
			Name:   taskName,
			Labels: []*peloton.Label{label},
			Resource: &task.ResourceConfig{
				CpuLimit:    4,
				MemLimitMb:  200,
				DiskLimitMb: 400,
				FdLimit:     100,
				GpuLimit:    10,
			},
			Container: &mesos.ContainerInfo{
				Type:    &test.containerType,
				Volumes: volumes,
			},
			Command: &mesos.CommandInfo{
				Uris: uris,
				Environment: &mesos.Environment{
					Variables: envVars,
				},
				Shell:     &shellCmd,
				Value:     &testCmd,
				Arguments: arguments,
			},
			Executor: &mesos.ExecutorInfo{
				Type:       &test.executorType,
				ExecutorId: &executorID,
			},
			HealthCheck: &task.HealthCheckConfig{
				Enabled:      false,
				CommandCheck: &task.HealthCheckConfig_CommandCheck{},
				HttpCheck: &task.HealthCheckConfig_HTTPCheck{
					Scheme: "http",
					Port:   uint32(100),
					Path:   "/health",
				},
			},
			Ports: []*task.PortConfig{
				{
					Name:  portName,
					Value: portValue,
				},
			},
			Constraint: &task.Constraint{
				Type: task.Constraint_LABEL_CONSTRAINT,
				LabelConstraint: &task.LabelConstraint{
					Label: label,
				},
				AndConstraint: &task.AndConstraint{},
				OrConstraint:  &task.OrConstraint{},
			},
			RestartPolicy: &task.RestartPolicy{
				MaxFailures: 5,
			},
			Volume: &task.PersistentVolumeConfig{
				ContainerPath: "test/container/path",
				SizeMB:        100,
			},
			PreemptionPolicy: &task.PreemptionPolicy{
				Type:          task.PreemptionPolicy_TYPE_NON_PREEMPTIBLE,
				KillOnPreempt: false,
			},
			Controller:             false,
			KillGracePeriodSeconds: 5,
			Revocable:              false,
		}

		if test.containerType == mesos.ContainerInfo_MESOS {
			imageType := mesos.Image_DOCKER
			cachedImage := true
			taskConfig.Container.Mesos = &mesos.ContainerInfo_MesosInfo{
				Image: &mesos.Image{
					Type: &imageType,
					Docker: &mesos.Image_Docker{
						Name: &testImage,
					},
					Cached: &cachedImage,
				},
			}
		} else if test.containerType == mesos.ContainerInfo_DOCKER {
			hostNetwork := mesos.ContainerInfo_DockerInfo_HOST
			taskConfig.Container.Docker = &mesos.ContainerInfo_DockerInfo{
				Image:      &testImage,
				Parameters: dockerParameters,
				Network:    &hostNetwork,
			}
		}

		podSpec := &pod.PodSpec{
			PodName: &v1alphapeloton.PodName{
				Value: util.CreatePelotonTaskID(jobID, instanceID),
			},
			Labels: []*v1alphapeloton.Label{
				{
					Key:   label.GetKey(),
					Value: label.GetValue(),
				},
			},
			Containers: []*pod.ContainerSpec{
				{
					Name: taskName,
					Resource: &pod.ResourceSpec{
						CpuLimit:    taskConfig.GetResource().GetCpuLimit(),
						MemLimitMb:  taskConfig.GetResource().GetMemLimitMb(),
						DiskLimitMb: taskConfig.GetResource().GetDiskLimitMb(),
						FdLimit:     taskConfig.GetResource().GetFdLimit(),
						GpuLimit:    taskConfig.GetResource().GetGpuLimit(),
					},
					Container: taskConfig.GetContainer(),
					Command:   taskConfig.GetCommand(),
					Executor:  taskConfig.GetExecutor(),
					LivenessCheck: &pod.HealthCheckSpec{
						Enabled:      taskConfig.GetHealthCheck().GetEnabled(),
						CommandCheck: &pod.HealthCheckSpec_CommandCheck{},
						HttpCheck: &pod.HealthCheckSpec_HTTPCheck{
							Scheme: taskConfig.GetHealthCheck().GetHttpCheck().GetScheme(),
							Port:   taskConfig.GetHealthCheck().GetHttpCheck().GetPort(),
							Path:   taskConfig.GetHealthCheck().GetHttpCheck().GetPath(),
						},
					},
					Ports: []*pod.PortSpec{
						{
							Name:    taskConfig.GetPorts()[0].GetName(),
							Value:   taskConfig.GetPorts()[0].GetValue(),
							EnvName: taskConfig.GetPorts()[0].GetEnvName(),
						},
					},
					Entrypoint: &pod.CommandSpec{
						Value:     testCmd,
						Arguments: arguments,
					},
					Environment:  podEnvironments,
					Image:        testImage,
					VolumeMounts: containerVolumeMounts,
				},
			},
			Constraint: &pod.Constraint{
				Type: pod.Constraint_CONSTRAINT_TYPE_LABEL,
				LabelConstraint: &pod.LabelConstraint{
					Label: &v1alphapeloton.Label{
						Key:   label.GetKey(),
						Value: label.GetValue(),
					},
				},
				AndConstraint: &pod.AndConstraint{},
				OrConstraint:  &pod.OrConstraint{},
			},
			RestartPolicy: &pod.RestartPolicy{
				MaxFailures: taskConfig.GetRestartPolicy().GetMaxFailures(),
			},
			Volume: &pod.PersistentVolumeSpec{
				ContainerPath: taskConfig.GetVolume().GetContainerPath(),
				SizeMb:        taskConfig.GetVolume().GetSizeMB(),
			},
			PreemptionPolicy: &pod.PreemptionPolicy{
				KillOnPreempt: false,
			},
			Controller:             false,
			KillGracePeriodSeconds: 5,
			Revocable:              false,
			Volumes:                podVolumes,
			MesosSpec: &apachemesos.PodSpec{
				Type:  test.podContainerType,
				Uris:  podUris,
				Shell: shellCmd,
				ExecutorSpec: &apachemesos.PodSpec_ExecutorSpec{
					Type:       test.podExecutorType,
					ExecutorId: executorIDValue,
				},
			},
		}

		if test.containerType == mesos.ContainerInfo_DOCKER {
			podSpec.MesosSpec.DockerParameters = podDockerParameters
		}

		suite.Equal(podSpec, ConvertTaskConfigToPodSpec(taskConfig, jobID, instanceID))

		podSpec.Containers[0].Container = nil
		podSpec.Containers[0].Command = nil
		podSpec.Containers[0].Executor = nil
		convertedTaskConfig, err := ConvertPodSpecToTaskConfig(podSpec)
		suite.NoError(err)
		suite.Equal(taskConfig, convertedTaskConfig)
	}
}

// TestConvertTaskConfigToPodSpecOnlyLabels tests the
// conversion of task config containing only labels to pod spec
func (suite *apiConverterTestSuite) TestConvertTaskConfigToPodSpecOnlyLabels() {
	label := &peloton.Label{
		Key:   "test-key",
		Value: "test-value",
	}

	taskConfig := &task.TaskConfig{
		Labels: []*peloton.Label{label},
	}

	podSpec := &pod.PodSpec{
		Labels: []*v1alphapeloton.Label{
			{
				Key:   label.GetKey(),
				Value: label.GetValue(),
			},
		},
	}

	suite.Equal(podSpec, ConvertTaskConfigToPodSpec(taskConfig, "", 0))
}

// TestConvertPodSpecToTaskConfigNoContainers tests the conversion from
// pod spec to task config when pod spec doesn't contain any containers
func (suite *apiConverterTestSuite) TestConvertPodSpecToTaskConfigNoContainers() {
	label := &peloton.Label{
		Key:   "test-key",
		Value: "test-value",
	}

	podSpec := &pod.PodSpec{
		Constraint: &pod.Constraint{
			Type: pod.Constraint_CONSTRAINT_TYPE_LABEL,
			LabelConstraint: &pod.LabelConstraint{
				Label: &v1alphapeloton.Label{
					Key:   label.GetKey(),
					Value: label.GetValue(),
				},
			},
		},
		PreemptionPolicy: &pod.PreemptionPolicy{
			KillOnPreempt: false,
		},
	}

	convertedTaskConfig, err := ConvertPodSpecToTaskConfig(podSpec)
	suite.NoError(err)

	suite.Equal(
		task.Constraint_Type(podSpec.GetConstraint().GetType()),
		convertedTaskConfig.GetConstraint().GetType(),
	)
	suite.Equal(
		podSpec.GetConstraint().GetLabelConstraint().GetLabel().GetKey(),
		convertedTaskConfig.GetConstraint().GetLabelConstraint().GetLabel().GetKey(),
	)
	suite.Equal(
		podSpec.GetConstraint().GetLabelConstraint().GetLabel().GetValue(),
		convertedTaskConfig.GetConstraint().GetLabelConstraint().GetLabel().GetValue(),
	)
	suite.Equal(
		podSpec.GetPreemptionPolicy().GetKillOnPreempt(),
		convertedTaskConfig.GetPreemptionPolicy().GetKillOnPreempt(),
	)
}

// TestConvertLabels tests conversion from v0 peloton.Label
// array to v1alpha peloton.Label array
func (suite *apiConverterTestSuite) TestConvertLabels() {
	labels := []struct {
		Key   string
		Value string
	}{
		{
			Key:   "test-key1",
			Value: "test-value1",
		},
		{
			Key:   "test-key2",
			Value: "test-value2",
		},
	}

	var v0Labels []*peloton.Label
	for _, l := range labels {
		v0Labels = append(v0Labels, &peloton.Label{
			Key:   l.Key,
			Value: l.Value,
		})
	}

	var expectedV1AlphaLabels []*v1alphapeloton.Label
	for _, l := range labels {
		expectedV1AlphaLabels = append(expectedV1AlphaLabels, &v1alphapeloton.Label{
			Key:   l.Key,
			Value: l.Value,
		})
	}

	suite.Equal(expectedV1AlphaLabels, ConvertLabels(v0Labels))
}

// TestTaskConstraintsToPodConstraintsAndViceVersa tests conversion
// from v0 task constraints to v1alpha pod constraints and vice versa
func (suite *apiConverterTestSuite) TestConvertTaskConstraintsToPodConstraintsAndViceVersa() {
	labels := []*peloton.Label{
		{
			Key:   "test-key1",
			Value: "test-value1",
		},
		{
			Key:   "test-key2",
			Value: "test-value2",
		},
		{
			Key:   "test-key3",
			Value: "test-value3",
		},
		{
			Key:   "test-key4",
			Value: "test-value4",
		},
	}
	taskConstraints := []*task.Constraint{
		{
			// make sure the nil fields are preserved
			Type: task.Constraint_LABEL_CONSTRAINT,
			LabelConstraint: &task.LabelConstraint{
				Kind: task.LabelConstraint_HOST,
			},
		},
		{
			Type: task.Constraint_LABEL_CONSTRAINT,
			LabelConstraint: &task.LabelConstraint{
				Kind: task.LabelConstraint_HOST,
				Label: &peloton.Label{
					Key:   "hostname",
					Value: "host",
				},
			},
			AndConstraint: &task.AndConstraint{},
			OrConstraint:  &task.OrConstraint{},
		},
		{
			Type: task.Constraint_AND_CONSTRAINT,
			LabelConstraint: &task.LabelConstraint{
				Label: &peloton.Label{},
			},
			AndConstraint: &task.AndConstraint{
				Constraints: []*task.Constraint{
					{
						Type: task.Constraint_LABEL_CONSTRAINT,
						LabelConstraint: &task.LabelConstraint{
							Kind:  task.LabelConstraint_TASK,
							Label: labels[0],
						},
						AndConstraint: &task.AndConstraint{},
						OrConstraint:  &task.OrConstraint{},
					},
					{
						Type: task.Constraint_LABEL_CONSTRAINT,
						LabelConstraint: &task.LabelConstraint{
							Kind:  task.LabelConstraint_TASK,
							Label: labels[1],
						},
						AndConstraint: &task.AndConstraint{},
						OrConstraint:  &task.OrConstraint{},
					},
				},
			},
			OrConstraint: &task.OrConstraint{},
		},
		{
			Type: task.Constraint_OR_CONSTRAINT,
			LabelConstraint: &task.LabelConstraint{
				Label: &peloton.Label{},
			},
			AndConstraint: &task.AndConstraint{},
			OrConstraint: &task.OrConstraint{
				Constraints: []*task.Constraint{
					{
						Type: task.Constraint_LABEL_CONSTRAINT,
						LabelConstraint: &task.LabelConstraint{
							Kind:  task.LabelConstraint_TASK,
							Label: labels[2],
						},
						AndConstraint: &task.AndConstraint{},
						OrConstraint:  &task.OrConstraint{},
					},
					{
						Type: task.Constraint_LABEL_CONSTRAINT,
						LabelConstraint: &task.LabelConstraint{
							Kind:  task.LabelConstraint_TASK,
							Label: labels[3],
						},
						AndConstraint: &task.AndConstraint{},
						OrConstraint:  &task.OrConstraint{},
					},
				},
			},
		},
	}

	podConstraints := []*pod.Constraint{
		{
			Type: pod.Constraint_CONSTRAINT_TYPE_LABEL,
			LabelConstraint: &pod.LabelConstraint{
				Kind: pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
			},
		},
		{
			Type: pod.Constraint_CONSTRAINT_TYPE_LABEL,
			LabelConstraint: &pod.LabelConstraint{
				Kind: pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
				Label: &v1alphapeloton.Label{
					Key:   "hostname",
					Value: "host",
				},
			},
			AndConstraint: &pod.AndConstraint{},
			OrConstraint:  &pod.OrConstraint{},
		},
		{
			Type: pod.Constraint_CONSTRAINT_TYPE_AND,
			LabelConstraint: &pod.LabelConstraint{
				Label: &v1alphapeloton.Label{},
			},
			AndConstraint: &pod.AndConstraint{
				Constraints: []*pod.Constraint{
					{
						Type: pod.Constraint_CONSTRAINT_TYPE_LABEL,
						LabelConstraint: &pod.LabelConstraint{
							Kind: pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
							Label: &v1alphapeloton.Label{
								Key:   labels[0].GetKey(),
								Value: labels[0].GetValue(),
							},
						},
						AndConstraint: &pod.AndConstraint{},
						OrConstraint:  &pod.OrConstraint{},
					},
					{
						Type: pod.Constraint_CONSTRAINT_TYPE_LABEL,
						LabelConstraint: &pod.LabelConstraint{
							Kind: pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
							Label: &v1alphapeloton.Label{
								Key:   labels[1].GetKey(),
								Value: labels[1].GetValue(),
							},
						},
						AndConstraint: &pod.AndConstraint{},
						OrConstraint:  &pod.OrConstraint{},
					},
				},
			},
			OrConstraint: &pod.OrConstraint{},
		},
		{
			Type: pod.Constraint_CONSTRAINT_TYPE_OR,
			LabelConstraint: &pod.LabelConstraint{
				Label: &v1alphapeloton.Label{},
			},
			AndConstraint: &pod.AndConstraint{},
			OrConstraint: &pod.OrConstraint{
				Constraints: []*pod.Constraint{
					{
						Type: pod.Constraint_CONSTRAINT_TYPE_LABEL,
						LabelConstraint: &pod.LabelConstraint{
							Kind: pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
							Label: &v1alphapeloton.Label{
								Key:   labels[2].GetKey(),
								Value: labels[2].GetValue(),
							},
						},
						AndConstraint: &pod.AndConstraint{},
						OrConstraint:  &pod.OrConstraint{},
					},
					{
						Type: pod.Constraint_CONSTRAINT_TYPE_LABEL,
						LabelConstraint: &pod.LabelConstraint{
							Kind: pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
							Label: &v1alphapeloton.Label{
								Key:   labels[3].GetKey(),
								Value: labels[3].GetValue(),
							},
						},
						AndConstraint: &pod.AndConstraint{},
						OrConstraint:  &pod.OrConstraint{},
					},
				},
			},
		},
	}

	suite.Equal(podConstraints, ConvertTaskConstraintsToPodConstraints(taskConstraints))
	suite.Equal(taskConstraints, ConvertPodConstraintsToTaskConstraints(podConstraints))
}

// TestConvertContainerPorts tests conversion from v0 task.PortConfig array to
// v1alpha pod.PortSpec array
func (suite *apiConverterTestSuite) TestConvertContainerPorts() {
	ports := []*task.PortConfig{
		{
			Name:    "test-port1",
			Value:   5291,
			EnvName: "test-env1",
		},
		{
			Name:    "test-port2",
			Value:   5292,
			EnvName: "test-env2",
		},
	}

	for i, containerPort := range ConvertPortConfigsToPortSpecs(ports) {
		suite.Equal(ports[i].GetName(), containerPort.GetName())
		suite.Equal(ports[i].GetValue(), containerPort.GetValue())
		suite.Equal(ports[i].GetEnvName(), containerPort.GetEnvName())
	}
}

// TestConvertV0SecretsToV1Secrets tests conversion from
// v0 peloton.Secret to v1alpha peloton.Secret
func (suite *apiConverterTestSuite) TestConvertV0SecretsToV1Secrets() {
	v0Secrets := []*peloton.Secret{
		{
			Id: &peloton.SecretID{
				Value: "testsecret1",
			},
			Path: "testpath1",
			Value: &peloton.Secret_Value{
				Data: []byte("testdata1"),
			},
		},
		{
			Id: &peloton.SecretID{
				Value: "testsecret2",
			},
			Path: "testpath2",
			Value: &peloton.Secret_Value{
				Data: []byte("testdata2"),
			},
		},
	}

	for i, v1Secret := range ConvertV0SecretsToV1Secrets(v0Secrets) {
		suite.Equal(v0Secrets[i].GetId().GetValue(), v1Secret.GetSecretId().GetValue())
		suite.Equal(v0Secrets[i].GetPath(), v1Secret.GetPath())
		suite.Equal(v0Secrets[i].GetValue().GetData(), v1Secret.GetValue().GetData())
	}
}

// TestConvertV1SecretsToV0Secrets tests conversion from
// v1alpha peloton.Secret to v0 peloton.Secret
func (suite *apiConverterTestSuite) TestConvertV1SecretsToV0Secrets() {
	v1Secrets := []*v1alphapeloton.Secret{
		{
			SecretId: &v1alphapeloton.SecretID{
				Value: "testsecret1",
			},
			Path: "testpath1",
			Value: &v1alphapeloton.Secret_Value{
				Data: []byte("testdata1"),
			},
		},
		{
			SecretId: &v1alphapeloton.SecretID{
				Value: "testsecret2",
			},
			Path: "testpath2",
			Value: &v1alphapeloton.Secret_Value{
				Data: []byte("testdata2"),
			},
		},
	}

	for i, v0Secret := range ConvertV1SecretsToV0Secrets(v1Secrets) {
		suite.Equal(v1Secrets[i].GetSecretId().GetValue(), v0Secret.GetId().GetValue())
		suite.Equal(v1Secrets[i].GetPath(), v0Secret.GetPath())
		suite.Equal(v1Secrets[i].GetValue().GetData(), v0Secret.GetValue().GetData())
	}
}

// TestJobConfigToJobSpecAndViceVersa tests conversion
// from v0 JobConfig to v1alpha JobSpec
func (suite *apiConverterTestSuite) TestConvertJobConfigToJobSpec() {
	labelKey := "test-key"
	labelValue := "test-value"
	controllerCmd := "echo hello & sleep 100"
	instanceConfigMap := make(map[uint32]*task.TaskConfig)
	instanceConfigMap[0] = &task.TaskConfig{
		Command: &mesos.CommandInfo{
			Value: &controllerCmd,
		},
		Controller: true,
	}
	command := "echo hello"

	jobConfig := &job.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version:   1,
			CreatedAt: 2,
			UpdatedAt: 3,
			UpdatedBy: "peloton",
		},
		Name:        "testName",
		Type:        job.JobType_SERVICE,
		Owner:       "testOwner",
		OwningTeam:  "team123",
		LdapGroups:  []string{"peloton"},
		Description: "test Description",
		Labels: []*peloton.Label{
			{
				Key:   labelKey,
				Value: labelValue,
			},
		},
		InstanceCount: 10,
		SLA: &job.SlaConfig{
			Preemptible: true,
			Revocable:   true,
		},
		DefaultConfig: &task.TaskConfig{
			Name: "instance",
			Command: &mesos.CommandInfo{
				Value: &command,
			},
			Controller: false,
		},
		InstanceConfig: instanceConfigMap,
		RespoolID: &peloton.ResourcePoolID{
			Value: "/test/respool",
		},
	}

	jobSpec := ConvertJobConfigToJobSpec(jobConfig)

	suite.Equal(jobConfig.GetChangeLog().GetVersion(), jobSpec.GetRevision().GetVersion())
	suite.Equal(jobConfig.GetChangeLog().GetCreatedAt(), jobSpec.GetRevision().GetCreatedAt())
	suite.Equal(jobConfig.GetChangeLog().GetUpdatedAt(), jobSpec.GetRevision().GetUpdatedAt())
	suite.Equal(jobConfig.GetChangeLog().GetUpdatedBy(), jobSpec.GetRevision().GetUpdatedBy())
	suite.Equal(jobConfig.GetName(), jobSpec.GetName())
	suite.Equal(jobConfig.GetOwner(), jobSpec.GetOwner())
	suite.Equal(jobConfig.GetOwningTeam(), jobSpec.GetOwningTeam())
	suite.Equal(jobConfig.GetLdapGroups(), jobSpec.GetLdapGroups())
	suite.Equal(jobConfig.GetDescription(), jobSpec.GetDescription())

	specLabels := jobSpec.GetLabels()
	suite.Len(specLabels, len(jobConfig.GetLabels()))
	for i, l := range jobConfig.GetLabels() {
		suite.Equal(l.GetKey(), specLabels[i].GetKey())
		suite.Equal(l.GetKey(), specLabels[i].GetKey())
	}

	suite.Equal(jobConfig.GetInstanceCount(), jobSpec.GetInstanceCount())
	suite.Equal(jobConfig.GetSLA().GetPreemptible(), jobSpec.GetSla().GetPreemptible())
	suite.Equal(jobConfig.GetSLA().GetRevocable(), jobSpec.GetSla().GetRevocable())
	suite.Equal(jobConfig.GetDefaultConfig().GetName(), jobSpec.GetDefaultSpec().GetContainers()[0].GetName())
	suite.Empty(jobSpec.GetDefaultSpec().GetPodName())
	suite.Equal(jobConfig.GetDefaultConfig().GetCommand().GetValue(), jobSpec.GetDefaultSpec().GetContainers()[0].GetCommand().GetValue())
	suite.Equal(jobConfig.GetDefaultConfig().GetController(), jobSpec.GetDefaultSpec().GetController())

	suite.Len(jobSpec.GetInstanceSpec(), len(instanceConfigMap))
	for i, spec := range jobSpec.GetInstanceSpec() {
		suite.Equal(instanceConfigMap[i].GetController(), spec.GetController())
		suite.Equal(instanceConfigMap[i].GetCommand().GetValue(), spec.GetContainers()[0].GetCommand().GetValue())
	}

	suite.Equal(jobConfig.GetRespoolID().GetValue(), jobSpec.GetRespoolId().GetValue())
}

// TestConvertJobSpecToJobConfig tests conversion
// from v1alpha JobSpec to v0 JobConfig
func (suite *apiConverterTestSuite) TestConvertJobSpecToJobConfig() {
	controllerCmd := "echo hello & sleep 100"
	command := "echo hello"
	instanceSpecMap := make(map[uint32]*pod.PodSpec)
	instanceSpecMap[0] = &pod.PodSpec{
		PodName: &v1alphapeloton.PodName{},
		Containers: []*pod.ContainerSpec{
			{
				Command: &mesos.CommandInfo{
					Value: &controllerCmd,
				},
			},
		},
		Controller: true,
	}

	jobSpec := &stateless.JobSpec{
		Revision: &v1alphapeloton.Revision{
			Version:   1,
			CreatedAt: 2,
			UpdatedAt: 3,
			UpdatedBy: "peloton",
		},
		Name:        "test-name",
		Owner:       "test-owner",
		OwningTeam:  "team123",
		LdapGroups:  []string{"peloton"},
		Description: "test description",
		Labels: []*v1alphapeloton.Label{
			{
				Key:   "test-key",
				Value: "test-value",
			},
		},
		InstanceCount: 10,
		Sla: &stateless.SlaSpec{
			Preemptible: true,
			Revocable:   true,
		},
		DefaultSpec: &pod.PodSpec{
			PodName: &v1alphapeloton.PodName{
				Value: "test-name",
			},
			Containers: []*pod.ContainerSpec{
				{
					Name: "instance",
					Command: &mesos.CommandInfo{
						Value: &command,
					},
				},
			},
			Controller: false,
		},
		InstanceSpec: instanceSpecMap,
		RespoolId: &v1alphapeloton.ResourcePoolID{
			Value: "/test/respool",
		},
	}

	jobConfig, err := ConvertJobSpecToJobConfig(jobSpec)
	suite.NoError(err)

	suite.Equal(jobSpec.GetRevision().GetVersion(), jobConfig.GetChangeLog().GetVersion())
	suite.Equal(jobSpec.GetRevision().GetCreatedAt(), jobConfig.GetChangeLog().GetCreatedAt())
	suite.Equal(jobSpec.GetRevision().GetUpdatedAt(), jobConfig.GetChangeLog().GetUpdatedAt())
	suite.Equal(jobSpec.GetRevision().GetUpdatedBy(), jobConfig.GetChangeLog().GetUpdatedBy())
	suite.Equal(jobSpec.GetName(), jobConfig.GetName())
	suite.Equal(jobSpec.GetOwner(), jobConfig.GetOwner())
	suite.Equal(jobSpec.GetOwningTeam(), jobConfig.GetOwningTeam())
	suite.Equal(jobSpec.GetLdapGroups(), jobConfig.GetLdapGroups())
	suite.Equal(jobSpec.GetDescription(), jobConfig.GetDescription())
	suite.Equal(jobSpec.GetSla().GetRevocable(), jobConfig.GetDefaultConfig().GetRevocable())

	configLabels := jobConfig.GetLabels()
	suite.Len(configLabels, len(jobSpec.GetLabels()))
	for i, l := range jobSpec.GetLabels() {
		suite.Equal(l.GetKey(), configLabels[i].GetKey())
		suite.Equal(l.GetKey(), configLabels[i].GetKey())
	}

	suite.Equal(jobSpec.GetInstanceCount(), jobConfig.GetInstanceCount())
	suite.Equal(jobSpec.GetSla().GetPreemptible(), jobConfig.GetSLA().GetPreemptible())
	suite.Equal(jobSpec.GetSla().GetRevocable(), jobConfig.GetSLA().GetRevocable())
	suite.Equal(jobSpec.GetDefaultSpec().GetContainers()[0].GetName(), jobConfig.GetDefaultConfig().GetName())
	suite.Equal(jobSpec.GetDefaultSpec().GetContainers()[0].GetCommand().GetValue(), jobConfig.GetDefaultConfig().GetCommand().GetValue())
	suite.Equal(jobSpec.GetDefaultSpec().GetController(), jobConfig.GetDefaultConfig().GetController())

	suite.Len(jobConfig.GetInstanceConfig(), len(instanceSpecMap))
	for i, config := range jobConfig.GetInstanceConfig() {
		suite.Equal(instanceSpecMap[i].GetController(), config.GetController())
		suite.Equal(jobConfig.GetSLA().GetRevocable(), config.GetRevocable())
		suite.Equal(instanceSpecMap[i].GetContainers()[0].GetCommand().GetValue(), config.GetCommand().GetValue())
	}

	suite.Equal(jobSpec.GetRespoolId().GetValue(), jobConfig.GetRespoolID().GetValue())
}

func (suite *apiConverterTestSuite) TestConvertUpdateModelToWorkflowStatus() {
	suite.Nil(ConvertUpdateModelToWorkflowStatus(nil, nil))

	jobConfigVersion := uint64(3)
	prevJobConfigVersion := uint64(2)
	updateModel := &models.UpdateModel{
		Type:                 models.WorkflowType_UPDATE,
		State:                update.State_ROLLING_FORWARD,
		PrevState:            update.State_INITIALIZED,
		InstancesCurrent:     []uint32{5},
		InstancesDone:        10,
		InstancesTotal:       25,
		InstancesFailed:      3,
		JobConfigVersion:     jobConfigVersion,
		PrevJobConfigVersion: prevJobConfigVersion,
		CreationTime:         "2019-01-30T21:25:23Z",
		UpdateTime:           "2019-01-30T21:35:23Z",
	}
	runtime := &job.RuntimeInfo{
		ConfigurationVersion: _configVersion,
		WorkflowVersion:      _workflowVersion,
		DesiredStateVersion:  _desiredStateVersion,
	}

	workflowStatus := &stateless.WorkflowStatus{
		Type:                  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
		State:                 stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
		PrevState:             stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED,
		InstancesCurrent:      updateModel.GetInstancesCurrent(),
		NumInstancesCompleted: updateModel.GetInstancesDone(),
		NumInstancesRemaining: updateModel.GetInstancesTotal() - updateModel.GetInstancesDone() - updateModel.GetInstancesFailed(),
		NumInstancesFailed:    updateModel.GetInstancesFailed(),
		Version:               versionutil.GetJobEntityVersion(jobConfigVersion, _desiredStateVersion, _workflowVersion),
		PrevVersion:           versionutil.GetJobEntityVersion(prevJobConfigVersion, _desiredStateVersion, _workflowVersion),
		CreationTime:          "2019-01-30T21:25:23Z",
		UpdateTime:            "2019-01-30T21:35:23Z",
	}

	suite.Equal(workflowStatus, ConvertUpdateModelToWorkflowStatus(runtime, updateModel))
}

// TestConvertRuntimeInfoToJobStatus tests conversion from
// v0 job.RuntimeInfo and private UpdateModel to v1alpha stateless.JobStatus
func (suite *apiConverterTestSuite) TestConvertRuntimeInfoToJobStatus() {
	taskStats := make(map[string]uint32)
	taskStats["RUNNING"] = 3
	taskStats["FAILED"] = 2
	podStats := make(map[string]uint32)
	podStats["POD_STATE_RUNNING"] = 3
	podStats["POD_STATE_FAILED"] = 2
	creationTime := "now"
	entityVersion := "2-2-2"
	taskConfigStateStats := make(map[uint64]*job.RuntimeInfo_TaskStateStats)
	taskConfigStateStats[_configVersion] = &job.RuntimeInfo_TaskStateStats{
		StateStats: map[string]uint32{
			task.TaskState_RUNNING.String(): 1,
			task.TaskState_PENDING.String(): 2,
		},
	}

	runtime := &job.RuntimeInfo{
		State:        job.JobState_RUNNING,
		CreationTime: creationTime,
		Revision: &peloton.ChangeLog{
			Version:   1,
			CreatedAt: 2,
			UpdatedAt: 3,
			UpdatedBy: "peloton",
		},
		TaskStats:                       taskStats,
		GoalState:                       job.JobState_RUNNING,
		ConfigurationVersion:            _configVersion,
		WorkflowVersion:                 _workflowVersion,
		DesiredStateVersion:             _desiredStateVersion,
		TaskStatsByConfigurationVersion: taskConfigStateStats,
	}

	jobConfigVersion := uint64(3)
	prevJobConfigVersion := uint64(2)
	updateModel := &models.UpdateModel{
		Type:                 models.WorkflowType_UPDATE,
		State:                update.State_ROLLING_FORWARD,
		InstancesCurrent:     []uint32{5},
		InstancesDone:        10,
		InstancesTotal:       25,
		InstancesFailed:      3,
		JobConfigVersion:     jobConfigVersion,
		PrevJobConfigVersion: prevJobConfigVersion,
	}

	workflowStatus := &stateless.WorkflowStatus{
		Type:                  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
		State:                 stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
		InstancesCurrent:      updateModel.GetInstancesCurrent(),
		NumInstancesCompleted: updateModel.GetInstancesDone(),
		NumInstancesRemaining: updateModel.GetInstancesTotal() - updateModel.GetInstancesDone() - updateModel.GetInstancesFailed(),
		NumInstancesFailed:    updateModel.GetInstancesFailed(),
		Version:               versionutil.GetJobEntityVersion(jobConfigVersion, _desiredStateVersion, _workflowVersion),
		PrevVersion:           versionutil.GetJobEntityVersion(prevJobConfigVersion, _desiredStateVersion, _workflowVersion),
	}

	jobStatus := ConvertRuntimeInfoToJobStatus(runtime, updateModel)

	suite.Equal(stateless.JobState_JOB_STATE_RUNNING, jobStatus.State)
	suite.Equal(creationTime, jobStatus.GetCreationTime())
	suite.Equal(podStats, jobStatus.GetPodStats())
	suite.Equal(stateless.JobState_JOB_STATE_RUNNING, jobStatus.GetDesiredState())
	suite.Equal(entityVersion, jobStatus.GetVersion().GetValue())
	suite.Equal(1, len(jobStatus.GetPodStatsByConfigurationVersion()))
	stateStats := jobStatus.GetPodStatsByConfigurationVersion()[versionutil.GetPodEntityVersion(_configVersion).GetValue()].GetStateStats()
	suite.Equal(uint32(1), stateStats[pod.PodState_POD_STATE_RUNNING.String()])
	suite.Equal(uint32(2), stateStats[pod.PodState_POD_STATE_PENDING.String()])
	suite.Equal(workflowStatus, jobStatus.GetWorkflowStatus())
}

// TestConvertJobSummary tests conversion from v0 job.JobSummary
// and private UpdateModel to v1alpha stateless.JobSummary
func (suite *apiConverterTestSuite) TestConvertJobSummary() {
	taskStats := make(map[string]uint32)
	taskStats["RUNNING"] = 3
	taskStats["FAILED"] = 2
	podStats := make(map[string]uint32)
	podStats["POD_STATE_RUNNING"] = 3
	podStats["POD_STATE_FAILED"] = 2

	creationTime := "now"
	entityVersion := "2-2-2"
	podConfigStateStats := make(map[string]*stateless.JobStatus_PodStateStats)

	runtime := &job.RuntimeInfo{
		State:        job.JobState_RUNNING,
		CreationTime: creationTime,
		Revision: &peloton.ChangeLog{
			Version:   1,
			CreatedAt: 2,
			UpdatedAt: 3,
			UpdatedBy: "peloton",
		},
		TaskStats:            taskStats,
		GoalState:            job.JobState_RUNNING,
		ConfigurationVersion: _configVersion,
		WorkflowVersion:      _workflowVersion,
		DesiredStateVersion:  _desiredStateVersion,
	}

	jobConfigVersion := uint64(3)
	prevJobConfigVersion := uint64(2)
	updateModel := &models.UpdateModel{
		Type:                 models.WorkflowType_UPDATE,
		State:                update.State_ROLLING_FORWARD,
		InstancesCurrent:     []uint32{5},
		InstancesDone:        10,
		InstancesTotal:       25,
		InstancesFailed:      3,
		JobConfigVersion:     jobConfigVersion,
		PrevJobConfigVersion: prevJobConfigVersion,
	}

	workflowStatus := &stateless.WorkflowStatus{
		Type:                  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
		State:                 stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
		InstancesCurrent:      updateModel.GetInstancesCurrent(),
		NumInstancesCompleted: updateModel.GetInstancesDone(),
		NumInstancesRemaining: updateModel.GetInstancesTotal() - updateModel.GetInstancesDone() - updateModel.GetInstancesFailed(),
		NumInstancesFailed:    updateModel.GetInstancesFailed(),
		Version:               versionutil.GetJobEntityVersion(jobConfigVersion, _desiredStateVersion, _workflowVersion),
		PrevVersion:           versionutil.GetJobEntityVersion(prevJobConfigVersion, _desiredStateVersion, _workflowVersion),
	}

	jobStatus := &stateless.JobStatus{
		Revision: &v1alphapeloton.Revision{
			Version:   runtime.GetRevision().GetVersion(),
			CreatedAt: runtime.GetRevision().GetCreatedAt(),
			UpdatedAt: runtime.GetRevision().GetUpdatedAt(),
			UpdatedBy: runtime.GetRevision().GetUpdatedBy(),
		},
		State:                          stateless.JobState(runtime.GetState()),
		CreationTime:                   runtime.GetCreationTime(),
		PodStats:                       podStats,
		PodStatsByConfigurationVersion: podConfigStateStats,
		DesiredState:                   stateless.JobState(runtime.GetGoalState()),
		Version:                        &v1alphapeloton.EntityVersion{Value: entityVersion},
		WorkflowStatus:                 workflowStatus,
	}

	summary := &job.JobSummary{
		Id:            &peloton.JobID{Value: "test-id"},
		Name:          "test-name",
		OwningTeam:    "test-owning-team",
		Owner:         "test-owner",
		InstanceCount: 10,
		RespoolID: &peloton.ResourcePoolID{
			Value: "/test/respool",
		},
		Runtime: runtime,
	}

	v1alphaJobSummary := ConvertJobSummary(summary, updateModel)

	suite.Equal(summary.GetId().GetValue(), v1alphaJobSummary.GetJobId().GetValue())
	suite.Equal(summary.GetName(), v1alphaJobSummary.GetName())
	suite.Equal(summary.GetOwningTeam(), v1alphaJobSummary.GetOwningTeam())
	suite.Equal(summary.GetOwner(), v1alphaJobSummary.GetOwner())
	suite.Equal(summary.GetInstanceCount(), v1alphaJobSummary.GetInstanceCount())
	suite.Equal(summary.GetRespoolID().GetValue(), v1alphaJobSummary.GetRespoolId().GetValue())
	suite.Equal(jobStatus, v1alphaJobSummary.GetStatus())
}

// TestConvertUpdateModelToWorkflowInfo tests conversion from
// private UpdateModel to v1alpha stateless.WorkflowInfo
func (suite *apiConverterTestSuite) TestConvertUpdateModelToWorkflowInfo() {
	jobConfigVersion := uint64(3)
	prevJobConfigVersion := uint64(2)
	updateModel := &models.UpdateModel{
		Type:                 models.WorkflowType_UPDATE,
		State:                update.State_ROLLING_FORWARD,
		InstancesCurrent:     []uint32{5},
		InstancesDone:        10,
		InstancesTotal:       25,
		InstancesFailed:      3,
		JobConfigVersion:     jobConfigVersion,
		PrevJobConfigVersion: prevJobConfigVersion,
		UpdateConfig: &update.UpdateConfig{
			BatchSize:           10,
			BatchPercentage:     10,
			StopBeforeUpdate:    true,
			StartPaused:         true,
			RollbackOnFailure:   true,
			MaxFailureInstances: 2,
			MaxInstanceAttempts: 3,
		},
	}
	runtime := &job.RuntimeInfo{
		ConfigurationVersion: _configVersion,
		WorkflowVersion:      _workflowVersion,
		DesiredStateVersion:  _desiredStateVersion,
	}

	workflowStatus := &stateless.WorkflowStatus{
		Type:                  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
		State:                 stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
		InstancesCurrent:      updateModel.GetInstancesCurrent(),
		NumInstancesCompleted: updateModel.GetInstancesDone(),
		NumInstancesRemaining: updateModel.GetInstancesTotal() - updateModel.GetInstancesDone() - updateModel.GetInstancesFailed(),
		NumInstancesFailed:    updateModel.GetInstancesFailed(),
		Version:               versionutil.GetJobEntityVersion(jobConfigVersion, _desiredStateVersion, _workflowVersion),
		PrevVersion:           versionutil.GetJobEntityVersion(prevJobConfigVersion, _desiredStateVersion, _workflowVersion),
	}

	workflowInfo := ConvertUpdateModelToWorkflowInfo(runtime, updateModel, nil, nil)

	suite.Equal(workflowStatus, workflowInfo.GetStatus())
	suite.Equal(updateModel.GetUpdateConfig().GetBatchSize(), workflowInfo.GetUpdateSpec().GetBatchSize())
	suite.Equal(updateModel.GetUpdateConfig().GetRollbackOnFailure(), workflowInfo.GetUpdateSpec().GetRollbackOnFailure())
	suite.Equal(updateModel.GetUpdateConfig().GetMaxFailureInstances(), workflowInfo.GetUpdateSpec().GetMaxTolerableInstanceFailures())
	suite.Equal(updateModel.GetUpdateConfig().GetMaxInstanceAttempts(), workflowInfo.GetUpdateSpec().GetMaxInstanceRetries())
	suite.Equal(updateModel.GetUpdateConfig().GetStartPaused(), workflowInfo.GetUpdateSpec().GetStartPaused())
}

// TestConvertUpdateModelToWorkflowInfoRestart tests conversion from
// private UpdateModel to v1alpha stateless.WorkflowInfo for restart workflow type
func (suite *apiConverterTestSuite) TestConvertUpdateModelToWorkflowInfoRestart() {
	jobConfigVersion := uint64(3)
	prevJobConfigVersion := uint64(2)
	updateModel := &models.UpdateModel{
		Type:                 models.WorkflowType_RESTART,
		State:                update.State_ROLLING_FORWARD,
		InstancesUpdated:     []uint32{5, 6, 7, 10},
		InstancesDone:        10,
		InstancesTotal:       25,
		InstancesFailed:      3,
		JobConfigVersion:     jobConfigVersion,
		PrevJobConfigVersion: prevJobConfigVersion,
		UpdateConfig: &update.UpdateConfig{
			BatchSize: 10,
		},
	}
	runtime := &job.RuntimeInfo{
		ConfigurationVersion: _configVersion,
		WorkflowVersion:      _workflowVersion,
		DesiredStateVersion:  _desiredStateVersion,
	}

	workflowStatus := &stateless.WorkflowStatus{
		Type:                  stateless.WorkflowType_WORKFLOW_TYPE_RESTART,
		State:                 stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
		InstancesCurrent:      updateModel.GetInstancesCurrent(),
		NumInstancesCompleted: updateModel.GetInstancesDone(),
		NumInstancesRemaining: updateModel.GetInstancesTotal() - updateModel.GetInstancesDone() - updateModel.GetInstancesFailed(),
		NumInstancesFailed:    updateModel.GetInstancesFailed(),
		Version:               versionutil.GetJobEntityVersion(jobConfigVersion, _desiredStateVersion, _workflowVersion),
		PrevVersion:           versionutil.GetJobEntityVersion(prevJobConfigVersion, _desiredStateVersion, _workflowVersion),
	}

	workflowInfo := ConvertUpdateModelToWorkflowInfo(runtime, updateModel, nil, nil)

	restartRanges := []*pod.InstanceIDRange{
		{
			From: 5,
			To:   7,
		},
		{
			From: 10,
			To:   10,
		},
	}
	suite.Equal(workflowStatus, workflowInfo.GetStatus())
	suite.Equal(updateModel.GetUpdateConfig().GetBatchSize(), workflowInfo.GetRestartSpec().GetBatchSize())
	suite.Equal(restartRanges, workflowInfo.GetRestartSpec().GetRanges())
}

// TestConvertStatelessQuerySpecToJobQuerySpec tests conversion
// from v1alpha stateless job query spec to v0 job query spec
func (suite *apiConverterTestSuite) TestConvertStatelessQuerySpecToJobQuerySpec() {
	testOrderProperty := "/asc/property/path"
	statelessQuerySpec := &stateless.QuerySpec{
		Pagination: &v1alphaquery.PaginationSpec{
			OrderBy: []*v1alphaquery.OrderBy{
				{
					Order: v1alphaquery.OrderBy_ORDER_BY_ASC,
					Property: &v1alphaquery.PropertyPath{
						Value: testOrderProperty,
					},
				},
			},
		},
		Labels: []*v1alphapeloton.Label{
			{
				Key:   "test-key",
				Value: "test-value",
			},
		},
		JobStates: []stateless.JobState{
			stateless.JobState_JOB_STATE_RUNNING,
		},
		CreationTimeRange: &v1alphapeloton.TimeRange{
			Min: &timestamp.Timestamp{
				Seconds: 200000,
			},
			Max: &timestamp.Timestamp{
				Seconds: 400000,
			},
		},
		CompletionTimeRange: &v1alphapeloton.TimeRange{
			Min: &timestamp.Timestamp{
				Seconds: 600000,
			},
			Max: &timestamp.Timestamp{
				Seconds: 1000000,
			},
		},
		Respool: &v1alpharespool.ResourcePoolPath{
			Value: "/test/respool",
		},
	}

	jobSpec := &job.QuerySpec{
		Pagination: &query.PaginationSpec{
			OrderBy: []*query.OrderBy{
				{
					Order: query.OrderBy_ASC,
					Property: &query.PropertyPath{
						Value: testOrderProperty,
					},
				},
			},
		},
		Respool: &respool.ResourcePoolPath{
			Value: statelessQuerySpec.GetRespool().GetValue(),
		},
		CreationTimeRange: &peloton.TimeRange{
			Min: statelessQuerySpec.GetCreationTimeRange().GetMin(),
			Max: statelessQuerySpec.GetCreationTimeRange().GetMax(),
		},
		CompletionTimeRange: &peloton.TimeRange{
			Min: statelessQuerySpec.GetCompletionTimeRange().GetMin(),
			Max: statelessQuerySpec.GetCompletionTimeRange().GetMax(),
		},
	}

	for _, jobState := range statelessQuerySpec.GetJobStates() {
		jobSpec.JobStates = append(jobSpec.JobStates, job.JobState(jobState))
	}

	for _, l := range statelessQuerySpec.GetLabels() {
		jobSpec.Labels = append(jobSpec.Labels, &peloton.Label{
			Key:   l.GetKey(),
			Value: l.GetValue(),
		})
	}

	suite.Equal(jobSpec, ConvertStatelessQuerySpecToJobQuerySpec(statelessQuerySpec))
}

// TestConvertUpdateSpecToUpdateConfig tests conversion from v1alpha update spec to v0 update config
func (suite *apiConverterTestSuite) TestConvertUpdateSpecToUpdateConfig() {
	spec := &stateless.UpdateSpec{
		BatchSize:                    10,
		RollbackOnFailure:            true,
		MaxInstanceRetries:           3,
		MaxTolerableInstanceFailures: 2,
		StartPaused:                  true,
	}

	config := ConvertUpdateSpecToUpdateConfig(spec)

	suite.Equal(spec.GetBatchSize(), config.GetBatchSize())
	suite.Equal(spec.GetRollbackOnFailure(), config.GetRollbackOnFailure())
	suite.Equal(spec.GetMaxInstanceRetries(), config.GetMaxInstanceAttempts())
	suite.Equal(spec.GetMaxTolerableInstanceFailures(), config.GetMaxFailureInstances())
	suite.Equal(spec.GetStartPaused(), config.GetStartPaused())
}

// TestConvertInstanceIDListToInstanceRange tests conversion from
// list of instance ids to list of instance ranges
func (suite *apiConverterTestSuite) TestConvertInstanceIDListToInstanceRange() {
	instanceIDs := []uint32{1, 2, 3, 4, 7, 9, 10}
	instanceIDRanges := []*pod.InstanceIDRange{
		{
			From: 1,
			To:   4,
		},
		{
			From: 7,
			To:   7,
		},
		{
			From: 9,
			To:   10,
		},
	}

	suite.Equal(instanceIDRanges, util.ConvertInstanceIDListToInstanceRange(instanceIDs))
}

// TestConvertPodQuerySpecToTaskQuerySpec tests conversion
// from v1alpha pod.QuerySpec to v0 task.QuerySpec
func (suite *apiConverterTestSuite) TestConvertPodQuerySpecToTaskQuerySpec() {
	testPod := "test-pod"
	testHost := "test-host"

	podQuerySpec := &pod.QuerySpec{
		PodStates: []pod.PodState{pod.PodState_POD_STATE_RUNNING},
		Names:     []*v1alphapeloton.PodName{{Value: testPod}},
		Hosts:     []string{testHost},
	}

	taskQuerySpec := &task.QuerySpec{
		TaskStates: []task.TaskState{task.TaskState_RUNNING},
		Names:      []string{testPod},
		Hosts:      []string{testHost},
	}

	suite.Equal(taskQuerySpec, ConvertPodQuerySpecToTaskQuerySpec(podQuerySpec))
}

// TestConvertTaskInfosToPodInfos tests conversion from
// a list of v0 task info to a list of v1alpha pod info
func (suite *apiConverterTestSuite) TestConvertTaskInfosToPodInfos() {
	taskInfos := []*task.TaskInfo{
		{
			Config: &task.TaskConfig{
				Name: "peloton",
				Resource: &task.ResourceConfig{
					CpuLimit:    4,
					MemLimitMb:  200,
					DiskLimitMb: 400,
					FdLimit:     100,
					GpuLimit:    10,
				},
				RestartPolicy: &task.RestartPolicy{
					MaxFailures: 5,
				},
				Volume: &task.PersistentVolumeConfig{
					ContainerPath: "test/container/path",
					SizeMB:        100,
				},
				PreemptionPolicy: &task.PreemptionPolicy{
					Type:          task.PreemptionPolicy_TYPE_NON_PREEMPTIBLE,
					KillOnPreempt: false,
				},
				Controller:             false,
				KillGracePeriodSeconds: 5,
				Revocable:              false,
			},
			Runtime: &task.RuntimeInfo{
				State: task.TaskState_RUNNING,
				MesosTaskId: &mesos.TaskID{
					Value: &testMesosTaskID,
				},
				GoalState: task.TaskState_SUCCEEDED,
				AgentID: &mesos.AgentID{
					Value: &testAgentID,
				},
				Revision: &peloton.ChangeLog{
					Version:   1,
					CreatedAt: 2,
					UpdatedAt: 3,
					UpdatedBy: "peloton",
				},
				PrevMesosTaskId: &mesos.TaskID{
					Value: &testPrevMesosTaskID,
				},
				Healthy: task.HealthState_HEALTHY,
				DesiredMesosTaskId: &mesos.TaskID{
					Value: &testMesosTaskID,
				},
			},
		},
		{
			Config: &task.TaskConfig{
				Name: "test",
				Resource: &task.ResourceConfig{
					CpuLimit:    2,
					MemLimitMb:  100,
					DiskLimitMb: 200,
					FdLimit:     50,
					GpuLimit:    5,
				},
			},
			Runtime: &task.RuntimeInfo{
				State:     task.TaskState_INITIALIZED,
				GoalState: task.TaskState_SUCCEEDED,
			},
		},
	}

	var podInfos []*pod.PodInfo

	for _, taskInfo := range taskInfos {
		podInfo := &pod.PodInfo{
			Spec: ConvertTaskConfigToPodSpec(
				taskInfo.GetConfig(),
				taskInfo.GetJobId().GetValue(),
				taskInfo.GetInstanceId(),
			),
			Status: ConvertTaskRuntimeToPodStatus(taskInfo.GetRuntime()),
		}
		podInfos = append(podInfos, podInfo)
	}

	converted := ConvertTaskInfosToPodInfos(taskInfos)
	suite.Equal(podInfos, converted)
}

// TestConvertTerminationStatusReason verifies that all TerminationStatus
// reason enums are converted correctly from v0 to v1alpha.
func (suite *apiConverterTestSuite) TestConvertTerminationStatusReason() {
	expmap := map[task.TerminationStatus_Reason]pod.TerminationStatus_Reason{
		task.TerminationStatus_TERMINATION_STATUS_REASON_INVALID:                      pod.TerminationStatus_TERMINATION_STATUS_REASON_INVALID,
		task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST:            pod.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST,
		task.TerminationStatus_TERMINATION_STATUS_REASON_FAILED:                       pod.TerminationStatus_TERMINATION_STATUS_REASON_FAILED,
		task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE:      pod.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE,
		task.TerminationStatus_TERMINATION_STATUS_REASON_PREEMPTED_RESOURCES:          pod.TerminationStatus_TERMINATION_STATUS_REASON_PREEMPTED_RESOURCES,
		task.TerminationStatus_TERMINATION_STATUS_REASON_DEADLINE_TIMEOUT_EXCEEDED:    pod.TerminationStatus_TERMINATION_STATUS_REASON_DEADLINE_TIMEOUT_EXCEEDED,
		task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_RESTART:           pod.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_RESTART,
		task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_UPDATE:            pod.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_UPDATE,
		task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_SLA_AWARE_RESTART: pod.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_SLA_AWARE_RESTART,
	}
	// ensure that we have a test-case for every legal value of v0 reason
	suite.Equal(len(task.TerminationStatus_Reason_name), len(expmap))
	for k, v := range expmap {
		_, ok := task.TerminationStatus_Reason_name[int32(k)]
		suite.True(ok)
		podTermStatus := convertTaskTerminationStatusToPodTerminationStatus(
			&task.TerminationStatus{
				Reason: k,
			})
		suite.Equal(v, podTermStatus.GetReason())
	}
}

func (suite *apiConverterTestSuite) TestConvertV1InstanceRangeToV0() {
	from := uint32(5)
	to := uint32(10)
	v1Range := []*pod.InstanceIDRange{
		{
			From: from,
			To:   to,
		},
	}

	v0Range := ConvertV1InstanceRangeToV0InstanceRange(v1Range)
	suite.Equal(len(v0Range), 1)
	suite.Equal(v0Range[0].From, from)
	suite.Equal(v0Range[0].To, to)
}

// TestConvertTaskStatsToPodStats tests conversion
// from v0 task stats to v1alpha pod stats
func (suite *apiConverterTestSuite) TestConvertTaskStatsToPodStats() {
	taskStats := make(map[string]uint32)
	taskStats["SUCCEEDED"] = 1
	taskStats["RUNNING"] = 3
	taskStats["FAILED"] = 2
	taskStats["INITIALIZED"] = 1
	taskStats["PENDING"] = 2
	taskStats["LAUNCHING"] = 6
	taskStats["LAUNCHED"] = 15
	taskStats["STARTING"] = 7
	taskStats["LOST"] = 11
	taskStats["PREEMPTING"] = 12
	taskStats["KILLED"] = 14
	taskStats["KILLING"] = 17
	taskStats["DELETED"] = 16

	podStatePrefix := "POD_STATE_"
	podStats := ConvertTaskStatsToPodStats(taskStats)
	for k, v := range taskStats {
		suite.Equal(v, podStats[podStatePrefix+k])
	}
}

func TestAPIConverter(t *testing.T) {
	suite.Run(t, new(apiConverterTestSuite))
}
