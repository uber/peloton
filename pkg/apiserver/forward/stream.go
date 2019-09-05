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

package forward

import (
	"context"
	"io"

	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/yarpcerrors"
)

var _ transport.StreamHandler = (*streamForward)(nil)

// streamForward implements a StreamHandler.
type streamForward struct {
	// outbound is the stream outbound to forward streaming requests to.
	outbound transport.StreamOutbound
	// overrideService is the service name to override the original service name.
	overrideService string
}

// NewStreamForward returns a new stream forwarding handler.
func NewStreamForward(
	outbound transport.StreamOutbound,
	overrideService string,
) *streamForward {
	return &streamForward{
		outbound:        outbound,
		overrideService: overrideService,
	}
}

// Handle implements HandleStream function of StreamHandler to forward streaming
// request and response between server and client.
// In case if either request or response is not a stream type,
// the corresponding streaming will just block on receiving message, and terminates
// when the connection terminates.
func (sf *streamForward) HandleStream(serverStream *transport.ServerStream) error {
	// Create new outbound context with cancel func.
	ctx, cancel := context.WithCancel(serverStream.Context())
	defer cancel()

	// Create a new client stream connection.
	clientStream, err := sf.outbound.CallStream(
		ctx, sf.preprocess(serverStream.Request()))
	if err != nil {
		return err
	}

	// Create error channels between server and client bidirectionally.
	serverToClient := sf.serverToClient(serverStream, clientStream)
	clientToServer := sf.clientToServer(clientStream, serverStream)

	// Handle stop sending from both server and client side.
	for i := 0; i < 2; i++ {
		select {
		case err := <-serverToClient:
			if err == io.EOF {
				_ = clientStream.Close(context.Background())
				break
			} else {
				return err
			}
		case err := <-clientToServer:
			if err != io.EOF {
				return err
			}
			return nil
		}
	}

	return yarpcerrors.InternalErrorf(
		"stream request forwarding should never reach here")
}

// clientToServer forwards message from client stream to server stream.
func (sf *streamForward) clientToServer(
	src *transport.ClientStream,
	dest *transport.ServerStream,
) chan error {
	errChan := make(chan error, 1)

	go func() {
		for {
			// Passing in a new context is ok here since the implementation
			// of ReceiveMessage and SendMessage underneath don't respect
			// the context passed in anyway.
			msg, err := src.ReceiveMessage(context.Background())
			if err != nil {
				errChan <- err
				break
			}

			err = dest.SendMessage(context.Background(), msg)
			if err != nil {
				errChan <- err
				break
			}
		}
	}()

	return errChan
}

// serverToClient forwards message from server stream to client stream.
func (sf *streamForward) serverToClient(
	src *transport.ServerStream,
	dest *transport.ClientStream,
) chan error {
	errChan := make(chan error, 1)

	go func() {
		for {
			// Passing in a new context is ok here since the implementation
			// of ReceiveMessage and SendMessage underneath don't respect
			// the context passed in anyway.
			msg, err := src.ReceiveMessage(context.Background())
			if err != nil {
				errChan <- err
				break
			}

			err = dest.SendMessage(context.Background(), msg)
			if err != nil {
				errChan <- err
				break
			}
		}
	}()

	return errChan
}

// preprocess overrides streaming request service if overrideService is provided.
func (sf *streamForward) preprocess(req *transport.StreamRequest) *transport.StreamRequest {
	if sf.overrideService != "" {
		req.Meta.Service = sf.overrideService
	}
	return req
}
