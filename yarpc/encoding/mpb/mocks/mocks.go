// Code generated by MockGen. DO NOT EDIT.
// Source: code.uber.internal/infra/peloton/yarpc/encoding/mpb (interfaces: SchedulerClient,MasterOperatorClient)

package mocks

import (
	reflect "reflect"

	v1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	master "code.uber.internal/infra/peloton/.gen/mesos/v1/master"
	scheduler "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"
	gomock "github.com/golang/mock/gomock"
)

// MockSchedulerClient is a mock of SchedulerClient interface
type MockSchedulerClient struct {
	ctrl     *gomock.Controller
	recorder *MockSchedulerClientMockRecorder
}

// MockSchedulerClientMockRecorder is the mock recorder for MockSchedulerClient
type MockSchedulerClientMockRecorder struct {
	mock *MockSchedulerClient
}

// NewMockSchedulerClient creates a new mock instance
func NewMockSchedulerClient(ctrl *gomock.Controller) *MockSchedulerClient {
	mock := &MockSchedulerClient{ctrl: ctrl}
	mock.recorder = &MockSchedulerClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (_m *MockSchedulerClient) EXPECT() *MockSchedulerClientMockRecorder {
	return _m.recorder
}

// Call mocks base method
func (_m *MockSchedulerClient) Call(_param0 string, _param1 *scheduler.Call) error {
	ret := _m.ctrl.Call(_m, "Call", _param0, _param1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Call indicates an expected call of Call
func (_mr *MockSchedulerClientMockRecorder) Call(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "Call", reflect.TypeOf((*MockSchedulerClient)(nil).Call), arg0, arg1)
}

// MockMasterOperatorClient is a mock of MasterOperatorClient interface
type MockMasterOperatorClient struct {
	ctrl     *gomock.Controller
	recorder *MockMasterOperatorClientMockRecorder
}

// MockMasterOperatorClientMockRecorder is the mock recorder for MockMasterOperatorClient
type MockMasterOperatorClientMockRecorder struct {
	mock *MockMasterOperatorClient
}

// NewMockMasterOperatorClient creates a new mock instance
func NewMockMasterOperatorClient(ctrl *gomock.Controller) *MockMasterOperatorClient {
	mock := &MockMasterOperatorClient{ctrl: ctrl}
	mock.recorder = &MockMasterOperatorClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (_m *MockMasterOperatorClient) EXPECT() *MockMasterOperatorClientMockRecorder {
	return _m.recorder
}

// Agents mocks base method
func (_m *MockMasterOperatorClient) Agents() (*master.Response_GetAgents, error) {
	ret := _m.ctrl.Call(_m, "Agents")
	ret0, _ := ret[0].(*master.Response_GetAgents)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Agents indicates an expected call of Agents
func (_mr *MockMasterOperatorClientMockRecorder) Agents() *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "Agents", reflect.TypeOf((*MockMasterOperatorClient)(nil).Agents))
}

// AllocatedResources mocks base method
func (_m *MockMasterOperatorClient) AllocatedResources(_param0 string) ([]*v1.Resource, error) {
	ret := _m.ctrl.Call(_m, "AllocatedResources", _param0)
	ret0, _ := ret[0].([]*v1.Resource)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AllocatedResources indicates an expected call of AllocatedResources
func (_mr *MockMasterOperatorClientMockRecorder) AllocatedResources(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCallWithMethodType(_mr.mock, "AllocatedResources", reflect.TypeOf((*MockMasterOperatorClient)(nil).AllocatedResources), arg0)
}