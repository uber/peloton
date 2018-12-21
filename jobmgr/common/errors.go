package common

import "go.uber.org/yarpc/yarpcerrors"

// UnexpectedVersionError is used when an operation fails because existing version
// is different from the version of object passed in
var UnexpectedVersionError = yarpcerrors.AbortedErrorf("operation aborted due to unexpected version")

// InvalidEntityVersionError is used when the entity version provided is different
// from the entity version passed in
var InvalidEntityVersionError = yarpcerrors.InvalidArgumentErrorf("unexpected entity version")
