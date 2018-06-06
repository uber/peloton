package test

import (
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/job"

	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"

	"github.com/golang/mock/gomock"
)

// NewMockJobConfig create a MockJobConfig from pbjob.JobConfig for unit test
func NewMockJobConfig(ctrl *gomock.Controller, config *pbjob.JobConfig) *cachedmocks.MockJobConfig {
	mockJobConfig := cachedmocks.NewMockJobConfig(ctrl)
	mockJobConfig.EXPECT().GetRespoolID().Return(config.RespoolID).AnyTimes()
	mockJobConfig.EXPECT().GetSLA().Return(config.GetSla()).AnyTimes()
	mockJobConfig.EXPECT().GetRespoolID().Return(config.GetRespoolID()).AnyTimes()
	mockJobConfig.EXPECT().GetChangeLog().Return(config.GetChangeLog()).AnyTimes()
	mockJobConfig.EXPECT().GetInstanceCount().Return(config.GetInstanceCount()).AnyTimes()
	mockJobConfig.EXPECT().GetType().Return(config.GetType()).AnyTimes()
	return mockJobConfig
}
