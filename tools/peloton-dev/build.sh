#!/usr/bin/env bash
# This script builds a docker image with all the dependencies required for peloton

set -euxo pipefail

# Constants
DEFAULT_DIST=jessie
PROJECT_NAME=peloton
PROJECT_ROOT=code.uber.internal/infra
TAG=test-peloton
workspace=${1:-}
dist=${2:-$DEFAULT_DIST}
GOPATH=${workspace}/.tmp/.goroot

# This is a workaround for downloading the dependencies via glide outside the container since we don't have ssh
# keys inside the container
setup_gopath() {
    mkdir -p .tmp/.goroot/src/${PROJECT_ROOT}
    chmod -R 777 .tmp
    ln -s ${workspace} .tmp/.goroot/src/${PROJECT_ROOT}/${PROJECT_NAME}
    export GOPATH
}

install_dependencies() {
    cd ${GOPATH}/src/${PROJECT_ROOT}/peloton && glide install
}

# Builds the docker container
build_docker() {
    docker build -t ${TAG} -f ${workspace}/tools/peloton-dev/${dist}/Dockerfile .
}

usage() { echo "Usage: $0 <workspace directory> [<distribution>]" 1>&2; exit 1; }

main() {
    if [[ -z "$workspace" ]]; then
        usage
    fi
    setup_gopath
    install_dependencies
    build_docker
}

main
