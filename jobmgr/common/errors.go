package common

import "go.uber.org/yarpc/yarpcerrors"

// UnexpectedVersionError is used when an operation fails because existing version
// is different from the version of object passed in
var UnexpectedVersionError = yarpcerrors.AbortedErrorf("operation aborted due to unexpected version")
