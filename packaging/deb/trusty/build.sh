#!/usr/bin/env bash

set -euxo pipefail

source build-common.sh

main() {
    echo "start building ubuntu-trusty package"
    install_dependencies
    build_peloton
    create_installation
    package
}

install_dependencies() {
  export GOPATH=/workspace
  install_protoc
}

main
