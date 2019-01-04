package test

import (
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"

	cachedmocks "github.com/uber/peloton/jobmgr/cached/mocks"

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
