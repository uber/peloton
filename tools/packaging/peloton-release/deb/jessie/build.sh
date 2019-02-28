#!/usr/bin/env bash

set -euxo pipefail

source /build-common.sh

main() {
    echo "start building debian-jessie package"
    install_dependencies
    build_peloton
    create_installation
    jessie_install
    package
}

jessie_install(){
  # install systemd unit files
  mkdir -p $INSTALL_DIR/lib/systemd/system
  cp -R $SRC_DIR/tools/packaging/peloton-release/deb/jessie/*service $INSTALL_DIR/lib/systemd/system/
}

install_dependencies() {
  install_protoc
}

main
