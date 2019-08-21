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

package apiproxy

import (
	"fmt"
	"reflect"

	pb_v0_host_svc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"
	"github.com/uber/peloton/pkg/apiproxy/forward"
	"github.com/uber/peloton/pkg/common"

	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/encoding/protobuf"
)

const (
	_procedureNameTemplate = "%s::%s"
)

var (
	// _encodingTypes contains a list of encoding types to build procedures with.
	_encodingTypes []transport.Encoding
)

// init constructs required Peloton YARPC clients for the package.
func init() {
	_encodingTypes = []transport.Encoding{
		protobuf.Encoding,
		protobuf.JSONEncoding,
	}
}

// BuildHostServiceProcedures builds forwarding procedures for
// all available procedures in HostServiceYARPCClient with given outbound.
// TODO: Construct all yarpc servers as a slice of interface and further consolidating
//  procedures building functions.
func BuildHostServiceProcedures(outbound transport.UnaryOutbound) []transport.Procedure {
	var procedures []transport.Procedure

	ct := reflect.TypeOf((*pb_v0_host_svc.HostServiceYARPCServer)(nil)).Elem()
	for i := 0; i < ct.NumMethod(); i++ {
		for _, encoding := range _encodingTypes {
			name := createProcedureName(
				common.RPCPelotonV0HostServiceName, ct.Method(i).Name)
			p := buildProcedure(name, common.PelotonHostManager, outbound, encoding)
			procedures = append(procedures, p)
		}
	}
	return procedures
}

// buildProcedure builds procedure with given procedure name and outbound.
func buildProcedure(
	procedureName, overrideService string,
	outbound transport.UnaryOutbound,
	encoding transport.Encoding,
) transport.Procedure {
	return transport.Procedure{
		Name: procedureName,
		HandlerSpec: transport.NewUnaryHandlerSpec(
			forward.NewUnaryForward(outbound, overrideService),
		),
		Encoding: encoding,
	}
}

// createProcedureName creates a full procedure name with given service and method.
func createProcedureName(service, method string) string {
	return fmt.Sprintf(_procedureNameTemplate, service, method)
}
