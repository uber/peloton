#!/usr/bin/env bash
set -e
set -x
cd "$(dirname "$0")"

# Set up the Git repo and submodules
git init
git submodule add gitolite@code.uber.internal:go-build || \
   git submodule update --init  || \
   echo "go-build is already set up"
GOBUILD=`pwd`/go-build

# We currently use godep, and want to force using Godeps/_workspace
export GO15VENDOREXPERIMENT=0

# Get the latest go-common and save to Godeps
TMPGOPATH=`mktemp -d`
mkdir -p $TMPGOPATH
export GOPATH=$TMPGOPATH
mkdir -p $GOPATH/src/code.uber.internal
pushd $GOPATH/src/code.uber.internal
git clone gitolite@code.uber.internal:go-common go-common.git
pushd $GOPATH/src/code.uber.internal/go-common.git
$GOBUILD/godep restore -v
popd
popd

# Godeps needs all imports to be valid, so generate thrift code
make thriftc

# Godeps needs to be in GOPATH to work, so create symlinks
TARGET_PREFIX=$GOPATH/src/code.uber.internal/infra
TARGET_DIR=$TARGET_PREFIX/peloton_agent
rm -rf $TARGET_DIR
mkdir -p $TARGET_PREFIX
ln -s `pwd` $TARGET_DIR
cd $TARGET_DIR
$GOBUILD/godep save ./...
