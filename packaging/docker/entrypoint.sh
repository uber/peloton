#!/usr/bin/env bash

set -euxo pipefail

/usr/bin/peloton/peloton-master -c $UBER_CONFIG_DIR/base.yaml \
                                -c $UBER_CONFIG_DIR/$UBER_ENVIRONMENT.yaml
