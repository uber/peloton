#!/usr/bin/env bash

set -euxo pipefail

source build-common.sh

main() {
    echo "start building debian-jessie package"
    install_dependencies
    build_peloton
    create_installation
    package
}

install_dependencies() {
  export GOPATH=/workspace
  install_golang
  install_protoc
}

main
