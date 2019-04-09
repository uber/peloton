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

package common

import "go.uber.org/yarpc/yarpcerrors"

/*
 * IMPORTANT: errors in this file are treated as part of API and user can have
 * dependency on it. Any modification on this file will be an API change.
 */

// UnexpectedVersionError is used when an operation fails because existing version
// is different from the version of object passed in
var UnexpectedVersionError = yarpcerrors.AbortedErrorf("operation aborted due to unexpected version")

// InvalidEntityVersionError is used when the entity version provided is different
// from the entity version passed in
var InvalidEntityVersionError = yarpcerrors.AbortedErrorf("unexpected entity version")
