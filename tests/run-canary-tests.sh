#!/bin/bash

set -exo pipefail

# make sure GOPATH is setup if missing. this is necessary to support building on
# uber ubuild machines without a valid gopath setup
# TODO(gabe): remove me when we no longer need a functioning GO env to build the docker
# containers.
if [[ -z ${GOPATH+x} ]] ; then
	workspace="$(pwd -P)/workspace"
	rm -rf "${workspace}" || :
	goDirPath="${workspace}/src/$(make project-name)"
  mkdir -p "$(dirname "$goDirPath")"
  if [ ! -e "$goDirPath" ]; then
    ln -sfv "$(dirname $workspace)" "$goDirPath"
  elif [ ! -L "$goDirPath" ]; then
    echo >&2 "error: $goDirPath already exists but is unexpectedly not a symlink"
    exit 1
  fi
	export GOPATH="$workspace"
fi

# Short-term fix for T1671015 (chunyang.shen)
mkdir -p "$GOPATH/bin"
export GOBIN="$GOPATH/bin"
export PATH=$PATH:$GOBIN

make install

. env/bin/activate

pip install -r tests/requirements.txt

make pygens

# Allow python path override so we can test any local changes in python client
if [[ -z "${PYTHONPATH}" ]]; then
  PYTHONPATH=$(pwd)
fi

export PYTHONPATH

PATH=$PATH:$(pwd)/bin CLUSTER="${CLUSTER}" ELECTION_ZK_SERVERS="${ZOOKEEPER}" FAILFAST="${FAILFAST}" JOB_TYPE=SERVICE pytest -vrsx --count=3 --durations=0 --random-order-seed="$(((RANDOM % 1000) + 1))" tests/integration/canary_test --junit-xml=integration-test-report.xml

deactivate
