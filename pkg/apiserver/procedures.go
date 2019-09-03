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

package apiserver

import (
	pbv0hostsvc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"
	pbv0jobmgr "github.com/uber/peloton/.gen/peloton/api/v0/job"
	pbv0jobsvc "github.com/uber/peloton/.gen/peloton/api/v0/job/svc"
	pbv0resmgr "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	pbv0respoolsvc "github.com/uber/peloton/.gen/peloton/api/v0/respool/svc"
	pbv0taskmgr "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbv0tasksvc "github.com/uber/peloton/.gen/peloton/api/v0/task/svc"
	pbv0updatesvc "github.com/uber/peloton/.gen/peloton/api/v0/update/svc"
	pbv0volumesvc "github.com/uber/peloton/.gen/peloton/api/v0/volume/svc"
	pbv1alphaadminsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/admin/svc"
	pbv1alphahostsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/host/svc"
	pbv1alphajobstatelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	pbv1alphapodsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	pbv1alpharespoolsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/respool/svc"
	pbv1alphawatchsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/watch/svc"
	pbprivateeventstreamsvc "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/eventstreamsvc"
	pbprivatehostsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	pbprivatehostmgrsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc"
	pbprivatejobmgrsvc "github.com/uber/peloton/.gen/peloton/private/jobmgrsvc"
	pbprivatetaskqueue "github.com/uber/peloton/.gen/peloton/private/resmgr/taskqueue"
	pbprivateresmgrsvc "github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/apiserver/forward"
	"github.com/uber/peloton/pkg/common"

	"go.uber.org/yarpc/api/transport"
)

// BuildJobManagerProcedures builds forwarding procedures for services that rely
// on Job Manager. The outbounds must connect to the Job Manager leader.
func BuildJobManagerProcedures(
	outbounds transport.Outbounds,
) []transport.Procedure {
	procedures :=
		pbv0jobmgr.BuildJobManagerYARPCProcedures(nil)
	procedures = append(
		procedures,
		pbv0jobsvc.BuildJobServiceYARPCProcedures(nil)...,
	)
	procedures = append(
		procedures,
		pbv1alphajobstatelesssvc.BuildJobServiceYARPCProcedures(nil)...,
	)
	procedures = append(
		procedures,
		pbprivatejobmgrsvc.BuildJobManagerServiceYARPCProcedures(nil)...,
	)
	procedures = append(
		procedures,
		pbv1alphapodsvc.BuildPodServiceYARPCProcedures(nil)...,
	)
	procedures = append(
		procedures,
		pbv0taskmgr.BuildTaskManagerYARPCProcedures(nil)...,
	)
	procedures = append(
		procedures,
		pbv0tasksvc.BuildTaskServiceYARPCProcedures(nil)...,
	)
	procedures = append(
		procedures,
		pbv0updatesvc.BuildUpdateServiceYARPCProcedures(nil)...,
	)
	procedures = append(
		procedures,
		pbv0volumesvc.BuildVolumeServiceYARPCProcedures(nil)...,
	)
	procedures = append(
		procedures,
		pbv1alphaadminsvc.BuildAdminServiceYARPCProcedures(nil)...,
	)
	procedures = append(
		procedures,
		pbv1alphawatchsvc.BuildWatchServiceYARPCProcedures(nil)...,
	)

	return convertProcedures(
		procedures,
		common.PelotonJobManager,
		outbounds,
	)
}

// BuildHostManagerProcedures builds forwarding procedures for services handled
// by Host Manager. The outbounds must connect to the Host Manager leader.
func BuildHostManagerProcedures(
	outbounds transport.Outbounds,
) []transport.Procedure {
	procedures :=
		pbv0hostsvc.BuildHostServiceYARPCProcedures(nil)
	procedures = append(
		procedures,
		pbv1alphahostsvc.BuildHostServiceYARPCProcedures(nil)...,
	)
	// Private Event Stream Service doesn't actually accept calls from api
	// server. It inspects RPC caller's service name and only accepts from
	// peloton-jobmgr, peloton-resmgr. This is okay because we do not actually
	// expect these daemons to contact each other through the api server.
	procedures = append(
		procedures,
		pbprivateeventstreamsvc.BuildEventStreamServiceYARPCProcedures(nil)...,
	)
	procedures = append(
		procedures,
		pbprivatehostsvc.BuildInternalHostServiceYARPCProcedures(nil)...,
	)
	procedures = append(
		procedures,
		pbprivatehostmgrsvc.BuildHostManagerServiceYARPCProcedures(nil)...,
	)

	return convertProcedures(
		procedures,
		common.PelotonHostManager,
		outbounds,
	)
}

// BuildResourceManagerProcedures builds forwarding procedures for services
// handled by Resource Manager. The outbounds must connect to the Resource
// Manager leader.
func BuildResourceManagerProcedures(
	outbounds transport.Outbounds,
) []transport.Procedure {
	procedures :=
		pbv0resmgr.BuildResourceManagerYARPCProcedures(nil)
	procedures = append(
		procedures,
		pbprivateresmgrsvc.BuildResourceManagerServiceYARPCProcedures(nil)...,
	)
	procedures = append(
		procedures,
		pbv0respoolsvc.BuildResourcePoolServiceYARPCProcedures(nil)...,
	)
	procedures = append(
		procedures,
		pbv1alpharespoolsvc.BuildResourcePoolServiceYARPCProcedures(nil)...,
	)
	procedures = append(
		procedures,
		pbprivatetaskqueue.BuildTaskQueueYARPCProcedures(nil)...,
	)

	return convertProcedures(
		procedures,
		common.PelotonResourceManager,
		outbounds,
	)
}

func convertProcedures(
	procedures []transport.Procedure,
	pelotonApplication string,
	outbounds transport.Outbounds,
) []transport.Procedure {
	convertedProcedures := make([]transport.Procedure, 0, len(procedures))
	for _, p := range procedures {
		handlerSpec := transport.NewUnaryHandlerSpec(
			forward.NewUnaryForward(outbounds.Unary, pelotonApplication),
		)
		if p.HandlerSpec.Type() == transport.Streaming {
			handlerSpec = transport.NewStreamHandlerSpec(
				forward.NewStreamForward(outbounds.Stream, pelotonApplication),
			)
		}
		cp := transport.Procedure{
			Name:        p.Name,
			HandlerSpec: handlerSpec,
			Encoding:    p.Encoding,
		}
		convertedProcedures = append(convertedProcedures, cp)
	}
	return convertedProcedures
}
