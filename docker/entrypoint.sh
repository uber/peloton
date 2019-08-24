#!/bin/bash
set -ex

if [[ -z "${APP}" ]]; then
  echo 'APP is not set'
  exit 1
fi

app="${APP}"
apptype="${APP_TYPE:-$APP}"
env="${ENVIRONMENT:-development}"
cfgdir="${CONFIG_DIR:-/etc/peloton}"
secretcfgdir="${SECRET_CONFIG_DIR}"
dir="${BUILD_DIR:-/go/src/github.com/uber/peloton}"
# make sure to cd into the BUILD_DIR, because aurora chainses us by changing into
# the sandbox
cd "${dir}"

if [[ ! -z ${PRODUCTION_CONFIG} ]]; then
    echo "${PRODUCTION_CONFIG}" | base64 --decode > "${cfgdir}/${apptype}/production.yaml"
fi

if [[ $app == "client" ]] ; then
    exec peloton "$@"
elif [[ $app == "migratedb" ]] ; then
    exec migratedb \
        -c "${cfgdir}/${apptype}/base.yaml" \
        -c "${cfgdir}/${apptype}/${env}.yaml" \
	    "$@"
elif [ ! -z "$secretcfgdir" ]; then
    exec "peloton-${app}" \
        -c "${cfgdir}/${apptype}/base.yaml" \
        -c "${cfgdir}/${apptype}/${env}.yaml" \
        -c "/langley/udocker/peloton/current/secrets.yaml" \
        "$@"
else
    exec "peloton-${app}" \
        -c "${cfgdir}/${apptype}/base.yaml" \
        -c "${cfgdir}/${apptype}/${env}.yaml" \
        "$@"
fi
