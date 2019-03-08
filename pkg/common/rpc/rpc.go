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

package rpc

import (
	"fmt"
	"net"
	nethttp "net/http"

	"github.com/uber/peloton/pkg/common"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/grpc"
	"go.uber.org/yarpc/transport/http"

	log "github.com/sirupsen/logrus"
)

const (
	// MaxRecvMsgSize is the largest acceptable RPC message size.
	MaxRecvMsgSize = 256 * 1024 * 1024 // 256MB
)

// NewTransport returns a new transport, using the default transport layer.
func NewTransport() *grpc.Transport {
	return grpc.NewTransport(
		grpc.ClientMaxRecvMsgSize(MaxRecvMsgSize),
		grpc.ServerMaxRecvMsgSize(MaxRecvMsgSize),
	)
}

// NewAuroraBridgeInbounds creates both HTTP and gRPC inbounds for the given ports
func NewAuroraBridgeInbounds(
	httpPort int,
	grpcPort int,
	mux *nethttp.ServeMux) []transport.Inbound {

	// Create both HTTP and gRPC transport
	ht := http.NewTransport()
	gt := NewTransport()

	gl, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.WithError(err).Fatal("failed to listen to gRPC port")
	}

	inbounds := []transport.Inbound{
		ht.NewInbound(
			fmt.Sprintf(":%d", httpPort),
			http.Mux("/api", mux),
		),
		gt.NewInbound(gl),
	}

	return inbounds
}

// NewInbounds creates both HTTP and gRPC inbounds for the given ports
func NewInbounds(
	httpPort int,
	grpcPort int,
	mux *nethttp.ServeMux) []transport.Inbound {

	// Create both HTTP and gRPC transport
	ht := http.NewTransport()
	gt := NewTransport()

	gl, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.WithError(err).Fatal("failed to listen to gRPC port")
	}

	inbounds := []transport.Inbound{
		ht.NewInbound(
			fmt.Sprintf(":%d", httpPort),
			http.Mux(common.PelotonEndpointPath, mux),
		),
		gt.NewInbound(gl),
	}
	return inbounds
}
