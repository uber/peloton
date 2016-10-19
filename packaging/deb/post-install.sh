#!/usr/bin/env bash

set -euxo pipefail

PELOTON_BIN_DIR="/usr/bin/peloton"
ETC_PELOTON="/etc/peloton"
INSTALL_DIR="/peloton-install"

main() {
  create_symlinks
}

create_symlinks() {
  mkdir -p $ETC_PELOTON
  ln -sf $INSTALL_DIR/bin $PELOTON_BIN_DIR
  ln -sf $INSTALL_DIR/config $ETC_PELOTON/config
}

main
