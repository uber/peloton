package rpc

import (
	"fmt"
	"net"
	nethttp "net/http"

	"code.uber.internal/infra/peloton/common"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/http"
	"go.uber.org/yarpc/transport/x/grpc"

	log "github.com/sirupsen/logrus"
)

// NewInbounds creates both HTTP and gRPC inbounds for the given ports
func NewInbounds(
	httpPort int,
	grpcPort int,
	mux *nethttp.ServeMux) []transport.Inbound {

	// Create both HTTP and gRPC transport
	ht := http.NewTransport()
	gt := grpc.NewTransport()

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
