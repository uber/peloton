package client_test

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	agent "code.uber.internal/infra/peloton/.gen/mesos/v1/agent"
	executor "code.uber.internal/infra/peloton/.gen/mesos/v1/executor"
	client "code.uber.internal/infra/peloton/executor/mesos-client"
	mocks "code.uber.internal/infra/peloton/executor/mesos-client/mocks"
)

func TestSubscribeResponseCode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	handler := mocks.NewMockEventHandler(ctrl)
	handler.EXPECT().Connected()
	handler.EXPECT().Disconnected()

	doer := mocks.NewMockHTTPDoer(ctrl)
	doer.EXPECT().Do(gomock.Any()).Return(&http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(strings.NewReader("0\n")),
	}, nil)

	client := client.New("", handler)
	client.SubscribeClient = doer

	callType := executor.Call_SUBSCRIBE
	call := &executor.Call{Type: &callType}

	code, err := client.ExecutorCall(call)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)

	doer.EXPECT().Do(gomock.Any()).Return(&http.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       ioutil.NopCloser(strings.NewReader("Internal Server Error\n")),
	}, nil)

	code, err = client.ExecutorCall(call)
	require.Error(t, err)
	require.Equal(t, http.StatusInternalServerError, code)
}

func TestUpdateResponseCode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := client.New("", nil)
	doer := mocks.NewMockHTTPDoer(ctrl)
	client.OutboundClient = doer

	doer.EXPECT().Do(gomock.Any()).Return(&http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(strings.NewReader("OK")),
	}, nil)

	callType := executor.Call_UPDATE
	call := &executor.Call{Type: &callType}

	code, err := client.ExecutorCall(call)
	require.Error(t, err)
	require.Equal(t, http.StatusOK, code)

	doer.EXPECT().Do(gomock.Any()).Return(&http.Response{
		StatusCode: http.StatusAccepted,
		Body:       ioutil.NopCloser(strings.NewReader("OK\n")),
	}, nil)
	code, err = client.ExecutorCall(call)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, code)
}

func TestLaunchResponseCode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := client.New("", nil)
	doer := mocks.NewMockHTTPDoer(ctrl)
	client.OutboundClient = doer

	doer.EXPECT().Do(gomock.Any()).Return(&http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(strings.NewReader("OK")),
	}, nil)

	callType := agent.Call_LAUNCH_NESTED_CONTAINER
	call := &agent.Call{Type: &callType}

	code, err := client.AgentCall(call)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)

	doer.EXPECT().Do(gomock.Any()).Return(&http.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       ioutil.NopCloser(strings.NewReader("Internal Server Error\n")),
	}, nil)
	code, err = client.AgentCall(call)
	require.Error(t, err)
	require.Equal(t, http.StatusInternalServerError, code)
}

func TestKillResponsecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := client.New("", nil)
	doer := mocks.NewMockHTTPDoer(ctrl)
	client.OutboundClient = doer

	doer.EXPECT().Do(gomock.Any()).Return(&http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(strings.NewReader("OK")),
	}, nil)

	callType := agent.Call_KILL_NESTED_CONTAINER
	call := &agent.Call{Type: &callType}

	code, err := client.AgentCall(call)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)

	doer.EXPECT().Do(gomock.Any()).Return(&http.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       ioutil.NopCloser(strings.NewReader("Internal Server Error\n")),
	}, nil)
	code, err = client.AgentCall(call)
	require.Error(t, err)
	require.Equal(t, http.StatusInternalServerError, code)
}

func TestWaitResponseCode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := client.New("", nil)
	doer := mocks.NewMockHTTPDoer(ctrl)
	client.OutboundClient = doer

	doer.EXPECT().Do(gomock.Any()).Return(&http.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       ioutil.NopCloser(strings.NewReader("Internal Server Error\n")),
	}, nil)

	callType := agent.Call_WAIT_NESTED_CONTAINER
	call := &agent.Call{Type: &callType}

	code, err := client.AgentCall(call)
	require.Error(t, err)
	require.Equal(t, 500, code)

	codewanted := int32(10)
	agentresp := &agent.Response{
		WaitNestedContainer: &agent.Response_WaitNestedContainer{
			ExitStatus: &codewanted,
		},
	}
	protostr, err := proto.Marshal(agentresp)
	require.NoError(t, err)
	doer.EXPECT().Do(gomock.Any()).Return(&http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(bytes.NewReader(protostr)),
	}, nil)

	code, err = client.AgentCall(call)
	require.NoError(t, err)
	require.Equal(t, 10, code)
}
