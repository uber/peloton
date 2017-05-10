#!/bin/bash
set -ex

if [[ -z "${APP}" ]]; then
  echo 'APP is not set'
  exit 1
fi

app="${APP}"
env="${ENVIRONMENT:-development}"
cfgdir="${CONFIG_DIR:-/etc/peloton}"
dir="${BUILD_DIR:-/go/src/code.uber.internal/infra/peloton}"
# make sure to cd into the BUILD_DIR, because aurora chainses us by changing into
# the sandbox
cd "${dir}"

if [[ $app == "client" ]] ; then
  exec peloton "$@"
else
  exec "peloton-${app}" \
    -c "${cfgdir}/${app}/base.yaml" \
    -c "${cfgdir}/${app}/${env}.yaml" \
    "$@"
fi
