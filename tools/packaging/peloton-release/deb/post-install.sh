#!/usr/bin/env bash

set -euxo pipefail

BIN_DIR="/usr/bin/peloton"
ETC_PELOTON="/etc/peloton"
INSTALL_DIR="/peloton-install"

main() {
  create_symlinks
}

create_symlinks() {
  mkdir -p $ETC_PELOTON
  ln -sf $INSTALL_DIR/bin $BIN_DIR
  ln -sf $INSTALL_DIR/config $ETC_PELOTON/config
}

main
