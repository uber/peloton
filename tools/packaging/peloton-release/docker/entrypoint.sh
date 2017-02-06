#!/usr/bin/env bash

set -euxo pipefail

# Identify which app binary to run by name
/usr/bin/peloton/peloton-$PELOTON_APP \
    -c $PELOTON_CONFIG_DIR/base.yaml   \
    -c $PELOTON_CONFIG_DIR/$PELOTON_ENVIRONMENT.yaml
