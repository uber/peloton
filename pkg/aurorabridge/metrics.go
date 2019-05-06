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

package aurorabridge

import (
	"github.com/uber-go/tally"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
)

const (
	ProcedureAbortJobUpdate         = "auroraschedulermanager__abortjobupdate"
	ProcedureGetConfigSummary       = "readonlyscheduler__getconfigsummary"
	ProcedureGetJobSummary          = "readonlyscheduler__getjobsummary"
	ProcedureGetJobUpdateDetails    = "readonlyscheduler__getjobupdatedetails"
	ProcedureGetJobUpdateDiff       = "readonlyscheduler__getjobupdatediff"
	ProcedureGetJobUpdateSummaries  = "readonlyscheduler__getjobupdatesummaries"
	ProcedureGetJobs                = "readonlyscheduler__getjobs"
	ProcedureGetTasksWithoutConfigs = "readonlyscheduler__gettaskswithoutconfigs"
	ProcedureGetTierConfigs         = "readonlyscheduler__gettierconfigs"
	ProcedureKillTasks              = "auroraschedulermanager__killtasks"
	ProcedurePauseJobUpdate         = "auroraschedulermanager__pausejobupdate"
	ProcedurePulseJobUpdate         = "auroraschedulermanager__pulsejobupdate"
	ProcedureResumeJobUpdate        = "auroraschedulermanager__resumejobupdate"
	ProcedureRollbackJobUpdate      = "auroraschedulermanager__rollbackjobupdate"
	ProcedureStartJobUpdate         = "auroraschedulermanager__startjobupdate"
)

var _procedures = []string{
	ProcedureAbortJobUpdate,
	ProcedureGetConfigSummary,
	ProcedureGetJobSummary,
	ProcedureGetJobUpdateDetails,
	ProcedureGetJobUpdateDiff,
	ProcedureGetJobUpdateSummaries,
	ProcedureGetJobs,
	ProcedureGetTasksWithoutConfigs,
	ProcedureGetTierConfigs,
	ProcedureKillTasks,
	ProcedurePauseJobUpdate,
	ProcedurePulseJobUpdate,
	ProcedureResumeJobUpdate,
	ProcedureRollbackJobUpdate,
	ProcedureStartJobUpdate,
}

var _responseCodeToText = map[api.ResponseCode]string{
	api.ResponseCodeInvalidRequest:   "invalid-request",
	api.ResponseCodeOk:               "ok",
	api.ResponseCodeError:            "error",
	api.ResponseCodeWarning:          "warning",
	api.ResponseCodeAuthFailed:       "auth-failed",
	api.ResponseCodeJobUpdatingError: "job-updating-error",
	api.ResponseCodeErrorTransient:   "error-transient",
}

type ResponseCodeMetrics struct {
	ResponseCodes map[api.ResponseCode]tally.Counter
}

type ResponseCodeLatencyMetrics struct {
	ResponseCodes map[api.ResponseCode]tally.Timer
}

type PerProcedureMetrics struct {
	ResponseCode        *ResponseCodeMetrics
	ResponseCodeLatency *ResponseCodeLatencyMetrics
}

// Metrics is the struct containing all metrics relevant for aurora api parrity
type Metrics struct {
	Procedures map[string]*PerProcedureMetrics
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	m := &Metrics{
		Procedures: map[string]*PerProcedureMetrics{},
	}
	for _, procedure := range _procedures {
		responseCodeMetrics := &ResponseCodeMetrics{
			ResponseCodes: make(map[api.ResponseCode]tally.Counter),
		}
		responseCodeLatencyMetrics := &ResponseCodeLatencyMetrics{
			ResponseCodes: make(map[api.ResponseCode]tally.Timer),
		}
		for _, responseCode := range api.ResponseCode_Values() {
			responseCodeText, exists := _responseCodeToText[responseCode]
			if !exists {
				responseCodeText = "unknown-error"
			}
			tag := map[string]string{
				"procedure":    procedure,
				"responsecode": responseCodeText,
			}
			responseCodeMetrics.ResponseCodes[responseCode] = scope.Tagged(tag).Counter("calls")
			responseCodeLatencyMetrics.ResponseCodes[responseCode] = scope.Tagged(tag).Timer("call_latency")
		}
		m.Procedures[procedure] = &PerProcedureMetrics{
			ResponseCode:        responseCodeMetrics,
			ResponseCodeLatency: responseCodeLatencyMetrics,
		}
	}
	return m
}
