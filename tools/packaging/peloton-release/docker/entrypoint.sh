#!/usr/bin/env bash

set -euxo pipefail

# Identify which binary to run by component name
/usr/bin/peloton/peloton-$PELOTON_COMPONENT \
    -c $PELOTON_CONFIG_DIR/base.yaml   \
    -c $PELOTON_CONFIG_DIR/$PELOTON_ENVIRONMENT.yaml
