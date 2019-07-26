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

package objects

import (
	"context"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/pkg/common"

	"github.com/gogo/protobuf/proto"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

type TaskConfigV2ObjectTestSuite struct {
	suite.Suite
	jobID *peloton.JobID
}

func (s *TaskConfigV2ObjectTestSuite) SetupTest() {
	setupTestStore()
	s.jobID = &peloton.JobID{Value: uuid.New()}
}

func TestTaskConfigV2ObjectTestSuite(t *testing.T) {
	suite.Run(t, new(TaskConfigV2ObjectTestSuite))
}

func (s *TaskConfigV2ObjectTestSuite) TestCreateGetPodSpec() {
	var configVersion uint64 = 1
	var instance0 int64 = 0
	var instance1 int64 = 1

	db := NewTaskConfigV2Ops(testStore)
	ctx := context.Background()

	taskConfig := &pbtask.TaskConfig{
		Name: "test-task",
		Resource: &pbtask.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
	}

	podSpec := &pbpod.PodSpec{
		PodName:    &v1alphapeloton.PodName{Value: "test-pod"},
		Containers: []*pbpod.ContainerSpec{{}},
	}

	s.NoError(db.Create(
		ctx,
		s.jobID,
		common.DefaultTaskConfigID,
		taskConfig,
		&models.ConfigAddOn{},
		podSpec,
		configVersion,
	))

	s.NoError(db.Create(
		ctx,
		s.jobID,
		instance0,
		taskConfig,
		&models.ConfigAddOn{},
		podSpec,
		configVersion,
	))

	// test normal get for instance0.
	spec, err := db.GetPodSpec(
		ctx,
		s.jobID,
		uint32(instance0),
		configVersion,
	)
	s.NoError(err)
	s.Equal(podSpec, spec)

	// Test get for an instance with no spec.
	// This should return default spec.
	spec, err = db.GetPodSpec(
		ctx,
		s.jobID,
		uint32(22),
		configVersion,
	)
	s.NoError(err)
	s.Equal(podSpec, spec)

	// test get from a non-existent job
	spec, err = db.GetPodSpec(
		ctx,
		&peloton.JobID{Value: uuid.New()},
		uint32(instance0),
		configVersion,
	)
	s.Error(err)
	s.Nil(spec)

	// test create and read a nil pod spec
	s.NoError(db.Create(
		ctx,
		s.jobID,
		instance1,
		taskConfig,
		&models.ConfigAddOn{},
		nil,
		configVersion,
	))

	spec, err = db.GetPodSpec(
		ctx,
		s.jobID,
		uint32(instance1),
		configVersion,
	)
	s.NoError(err)
	s.Nil(spec)

}

func (s *TaskConfigV2ObjectTestSuite) TestCreateGetTaskConfig() {
	var configVersion uint64 = 1
	var instance0 int64 = 0
	var instance1 int64 = 1

	db := NewTaskConfigV2Ops(testStore)
	ctx := context.Background()

	configAddOn := &models.ConfigAddOn{
		SystemLabels: []*peloton.Label{{Key: "k1", Value: "v1"}},
	}

	defaultConfig := &pbtask.TaskConfig{
		Name: "default",
		Resource: &pbtask.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
	}

	podSpec := &pbpod.PodSpec{
		PodName:    &v1alphapeloton.PodName{Value: "test-pod"},
		Containers: []*pbpod.ContainerSpec{{}},
	}

	// create default config
	s.NoError(db.Create(
		ctx,
		s.jobID,
		common.DefaultTaskConfigID,
		defaultConfig,
		configAddOn,
		podSpec,
		configVersion,
	))

	// create task specific config for instance 0
	instance0Config := &pbtask.TaskConfig{
		Name: "instance0",
		Resource: &pbtask.ResourceConfig{
			CpuLimit:    1.0,
			MemLimitMb:  80,
			DiskLimitMb: 150,
		},
	}
	s.NoError(db.Create(
		ctx,
		s.jobID,
		instance0,
		instance0Config,
		configAddOn,
		podSpec,
		configVersion,
	))

	// instance0 should have specific config
	config, addOn, err := db.GetTaskConfig(ctx, s.jobID, uint32(instance0), configVersion)
	s.NoError(err)
	s.Equal(config, instance0Config)
	s.Equal(addOn, configAddOn)

	// instance1 should have default config
	config, addOn, err = db.GetTaskConfig(ctx, s.jobID, uint32(instance1), configVersion)
	s.NoError(err)
	s.Equal(config, defaultConfig)
	s.Equal(addOn, configAddOn)
}

// TestGetTaskConfigLegacy tests a case where config is present in task_config
// and not in task_config_v2.
func (s *TaskConfigV2ObjectTestSuite) TestGetTaskConfigLegacy() {
	var configVersion uint64 = 1

	db := NewTaskConfigV2Ops(testStore)
	ctx := context.Background()

	configAddOn := &models.ConfigAddOn{
		SystemLabels: []*peloton.Label{{Key: "k1", Value: "v1"}},
	}

	taskConfig := &pbtask.TaskConfig{
		Name: "default",
		Resource: &pbtask.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
	}

	configBuffer, err := proto.Marshal(taskConfig)
	s.NoError(err)
	addOnBuffer, err := proto.Marshal(configAddOn)
	s.NoError(err)

	// Write config to legacy task_config table.
	obj := &TaskConfigObject{
		JobID:        s.jobID.GetValue(),
		Version:      configVersion,
		InstanceID:   0,
		Config:       configBuffer,
		ConfigAddOn:  addOnBuffer,
		CreationTime: time.Now(),
	}
	s.NoError(testStore.oClient.Create(ctx, obj))

	// read should go through and fetch data internally from task_config table.
	config, addOn, err := db.GetTaskConfig(
		ctx,
		s.jobID,
		uint32(0),
		configVersion,
	)
	s.NoError(err)
	s.Equal(config, taskConfig)
	s.Equal(addOn, configAddOn)
}
