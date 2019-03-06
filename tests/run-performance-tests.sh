#!/bin/bash

set -exo pipefail

cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

root_dir=$(dirname "$cur_dir")

pushd $root_dir

# set GOPATH
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

# install dependency
make vcluster

. env/bin/activate

if [[ -z "${PELOTON_IMAGE}" ]]; then
  echo "Docker image for Peloton not set in environment variable PELOTON_IMAGE"
  exit 1
fi
if [[ -z "${ZOOKEEPER}" ]]; then
  echo "Zookeeper location not set in environment variable ZOOKEEPER"
  exit 1
fi

# get the current version
if [[ -z "${VERSION}" ]]; then
  echo "No Version specified"
  VERSION=`make version`
fi

# get the current commit hash and use it as the name for vCluster
if [[ -z "${COMMIT_HASH}" ]]; then
  echo "No Version specified"
  COMMIT_HASH=`make commit-hash`
fi

echo $VERSION
echo $COMMIT_HASH

# vCluster name is the first 12 character of the commit
vcluster_name="v${COMMIT_HASH:0:12}"
VCLUSTER_ARGS="-z ${ZOOKEEPER} -p /PelotonPerformance -n ${vcluster_name}"
NUM_SLAVES=1000

# start vCluster
tools/vcluster/main.py \
  ${VCLUSTER_ARGS} \
  setup \
  -s ${NUM_SLAVES} \
  -i ${PELOTON_IMAGE}

# run benchmark. Make a note of the return code instead of failing this script
# so that vcluster teardown is run even if the benchmark fails.
RC=0
tests/performance/multi_benchmark.py -i ".${vcluster_name}" -o "PERF_TEST" || RC=$?

# teardown vCluster
tools/vcluster/main.py ${VCLUSTER_ARGS} teardown

deactivate

popd

exit $RC
