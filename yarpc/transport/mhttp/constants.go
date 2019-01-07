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

package mhttp

const (
	// ApplicationHeaderPrefix is the prefix added to application headers over
	// the wire.
	ApplicationHeaderPrefix = "Rpc-Header-"

	// BaggageHeaderPrefix is the prefix added to context headers over the wire.
	BaggageHeaderPrefix = "Context-"

	// TODO(abg): Allow customizing header prefixes

	// CallerHeader is the HTTP header used to indiate the service doing the calling
	CallerHeader = "Rpc-Caller"

	// EncodingHeader is the HTTP header used to specify the name of the
	// encoding.
	EncodingHeader = "Rpc-Encoding"

	// TTLMSHeader is the HTTP header used to indicate the ttl in ms
	TTLMSHeader = BaggageHeaderPrefix + "TTL-MS"

	// ProcedureHeader is the HTTP header used to indicate the procedure
	ProcedureHeader = "Rpc-Procedure"

	// ServiceHeader is the HTTP header used to indicate the service
	ServiceHeader = "Rpc-Service"
)
