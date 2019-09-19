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

package resmgr

import (
	"time"

	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/resmgr/common"
	"github.com/uber/peloton/pkg/resmgr/task"
)

// Config is Resource Manager specific configuration
type Config struct {
	// HTTP port which hostmgr is listening on
	HTTPPort int `yaml:"http_port"`

	// GRPC port which hostmgr is listening on
	GRPCPort int `yaml:"grpc_port"`

	// Period to run task scheduling in seconds
	TaskSchedulingPeriod time.Duration `yaml:"task_scheduling_period"`

	// Period to run entitlement calculator
	EntitlementCaculationPeriod time.Duration `yaml:"entitlement_calculation_period"`

	// Period to run task reconciliation
	TaskReconciliationPeriod time.Duration `yaml:"task_reconciliation_period"`

	// RM Task Config
	RmTaskConfig *task.Config `yaml:"task"`

	// Config for task preemption
	PreemptionConfig *common.PreemptionConfig `yaml:"preemption"`

	// Period to run host drainer
	HostDrainerPeriod time.Duration `yaml:"host_drainer_period"`

	// This flag will enable/disable host scorer service
	EnableHostScorer bool `yaml:"enable_host_scorer"`

	// HostManagerAPIVersion is the API version that the Resource Manager
	// should use to talk to Host Manager.
	HostManagerAPIVersion api.Version `yaml:"hostmgr_api_version"`

	// UseHostPool is the config switch to use host pool in Resource manager
	UseHostPool bool `yaml:"use_host_pool"`
}
