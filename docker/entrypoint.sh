#!/bin/bash
set -ex

if [[ -z "${APP}" ]]; then
  echo 'APP is not set'
  exit 1
fi

app="${APP}"
env="${ENVIRONMENT:-development}"
cfgdir="${CONFIG_DIR:-/etc/peloton}"
secretcfgdir="${SECRET_CONFIG_DIR}"
dir="${BUILD_DIR:-/go/src/github.com/uber/peloton}"
# make sure to cd into the BUILD_DIR, because aurora chainses us by changing into
# the sandbox
cd "${dir}"

if [[ ! -z ${PRODUCTION_CONFIG} ]]; then
    echo "${PRODUCTION_CONFIG}" | base64 --decode > "${cfgdir}/${app}/production.yaml"
fi

if [[ $app == "client" ]] ; then
    exec peloton "$@"
elif [ ! -z "$secretcfgdir" ]; then
    exec "peloton-${app}" \
        -c "${cfgdir}/${app}/base.yaml" \
        -c "${cfgdir}/${app}/${env}.yaml" \
        -c "/langley/udocker/peloton/current/secrets.yaml" \
        "$@"
else
    exec "peloton-${app}" \
        -c "${cfgdir}/${app}/base.yaml" \
        -c "${cfgdir}/${app}/${env}.yaml" \
        "$@"
fi
