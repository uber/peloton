#!/usr/bin/env bash

set -euxo pipefail

source /build-common.sh

main() {
    echo "start building ubuntu-trusty package"
    install_dependencies
    build_peloton
    create_installation
    trusty_install
    package
}

trusty_install(){
  mkdir -p $INSTALL_DIR/etc/init
  cp -R $SRC_DIR/tools/packaging/peloton-release/deb/trusty/*upstart $INSTALL_DIR/etc/init/
}

install_dependencies() {
  install_protoc
}

main
