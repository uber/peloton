package client_test

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	executor "code.uber.internal/infra/peloton/.gen/mesos/v1/executor"
	client "code.uber.internal/infra/peloton/executor/mesos-client"
	mocks "code.uber.internal/infra/peloton/executor/mesos-client/mocks"
)

func TestReadNothing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	handler := mocks.NewMockEventHandler(ctrl)

	reader, writer := io.Pipe()
	closer := ioutil.NopCloser(reader)
	go func() {
		writer.Close()
	}()
	err := client.DispatchEvents(closer, client.JSONUnmarshal, handler)
	require.Equal(t, io.EOF, errors.Cause(err))
}

func TestReadEmptyChunk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	handler := mocks.NewMockEventHandler(ctrl)

	reader, writer := io.Pipe()
	closer := ioutil.NopCloser(reader)
	go func() {
		writer.Write([]byte("0\n"))
		writer.Close()
	}()
	err := client.DispatchEvents(closer, client.JSONUnmarshal, handler)
	require.NoError(t, err)
}

func TestReadEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	handler := mocks.NewMockEventHandler(ctrl)

	reader, writer := io.Pipe()
	closer := ioutil.NopCloser(reader)
	go func() {
		writer.Write([]byte("2\n{}0\n"))
		writer.Close()
	}()

	handler.EXPECT().HandleEvent(&executor.Event{})

	err := client.DispatchEvents(closer, client.JSONUnmarshal, handler)
	require.Nil(t, err)
}

func TestDisconnectOnEOF(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	handler := mocks.NewMockEventHandler(ctrl)

	reader, writer := io.Pipe()
	closer := ioutil.NopCloser(reader)
	go func() {
		writer.Write([]byte("0\n"))
		writer.Close()
	}()

	err := client.DispatchEvents(closer, client.JSONUnmarshal, handler)
	require.NoError(t, err)
}
