#!/usr/bin/env bash
# This script builds a docker container with all the dependencies required for peloton

set -euxo pipefail

# Constants
PROJECT_NAME=peloton
PROJECT_ROOT=code.uber.internal/infra
TAG=test-peloton

# This is a workaround for downloading the dependencies via glide outside the container since we don't have ssh
# keys inside the container
setup_gopath() {
    mkdir -p .tmp/.goroot/src/${PROJECT_ROOT}
    ln -s ${workspace} .tmp/.goroot/src/${PROJECT_ROOT}/${PROJECT_NAME}
    GOPATH=${workspace}/.tmp/.goroot
    export GOPATH
}

install_dependencies() {
    cd ${GOPATH}/src/${PROJECT_ROOT}/peloton && glide install
}

# Builds the docker container
build_docker() {
    docker build -t ${TAG} -f ${workspace}/docker/peloton/Dockerfile .
}

main() {
    setup_gopath
    install_dependencies
    build_docker
}

workspace=${1:-}
if [[ -z "$workspace" ]]; then
    echo "no workspace specified!"
    exit 1
fi

main
