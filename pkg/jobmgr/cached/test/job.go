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

package test

import (
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"

	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"

	"github.com/golang/mock/gomock"
)

// NewMockJobConfig create a MockJobConfig from pbjob.JobConfig for unit test
func NewMockJobConfig(ctrl *gomock.Controller, config *pbjob.JobConfig) *cachedmocks.MockJobConfigCache {
	mockJobConfig := cachedmocks.NewMockJobConfigCache(ctrl)
	mockJobConfig.EXPECT().GetRespoolID().Return(config.RespoolID).AnyTimes()
	mockJobConfig.EXPECT().GetSLA().Return(config.GetSLA()).AnyTimes()
	mockJobConfig.EXPECT().GetRespoolID().Return(config.GetRespoolID()).AnyTimes()
	mockJobConfig.EXPECT().GetChangeLog().Return(config.GetChangeLog()).AnyTimes()
	mockJobConfig.EXPECT().GetInstanceCount().Return(config.GetInstanceCount()).AnyTimes()
	mockJobConfig.EXPECT().GetType().Return(config.GetType()).AnyTimes()
	return mockJobConfig
}
