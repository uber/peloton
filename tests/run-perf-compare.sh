#!/bin/bash

set -exo pipefail
[[ $(uname) == Darwin || -n $JENKINS_HOME ]] && docker_cmd='docker' || docker_cmd='sudo docker'

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

if [[ -z "${PELOTON_IMAGE_CURRENT}" ]]; then
  echo "Docker image for current Peloton version not set in environment variable PELOTON_IMAGE_CURRENT"
  exit 1
fi
if [[ -z "${PELOTON_IMAGE_BASELINE}" ]]; then
  echo "Docker image for baseline Peloton version not set in environment variable PELOTON_IMAGE_BASELINE"
  exit 1
fi
if [[ -z "${ZOOKEEPER}" ]]; then
  echo "Zookeeper location not set in environment variable ZOOKEEPER"
  exit 1
fi

if [[ -z "${COMMIT_HASH}" ]]; then
  echo "No Version specified"
  COMMIT_HASH=`make commit-hash`
fi


# Run current version
vcluster_name="v${COMMIT_HASH:12:12}"
VCLUSTER_ARGS="-z ${ZOOKEEPER} -p /DefaultResPool -n ${vcluster_name}"
NUM_SLAVES=1000

tools/vcluster/main.py \
  ${VCLUSTER_ARGS} \
  setup \
  -s ${NUM_SLAVES} \
  -i ${PELOTON_IMAGE_CURRENT}
# Make a note of the return code instead of failing this script
# so that vcluster teardown is run even if the benchmark fails.
RC=0
tests/performance/multi_benchmark.py \
  -i "CONF_${vcluster_name}" \
  -o "PERF_CURRENT" || RC=$?
tools/vcluster/main.py ${VCLUSTER_ARGS} teardown

if [[ $RC -ne 0 ]]; then
  echo "Benchmark failed for current version, aborting"
  exit $RC
fi

# Run base version
vcluster_name="v${COMMIT_HASH:0:12}"
VCLUSTER_ARGS="-z ${ZOOKEEPER} -p /DefaultResPool -n ${vcluster_name}"

tools/vcluster/main.py \
  ${VCLUSTER_ARGS} \
  setup \
  -s ${NUM_SLAVES} \
  -i ${PELOTON_IMAGE_BASELINE}
RC=0
tests/performance/multi_benchmark.py \
  -i "CONF_${vcluster_name}" \
  -o "PERF_BASE" || RC=$?
tools/vcluster/main.py ${VCLUSTER_ARGS} teardown

if [[ $RC -ne 0 ]]; then
  echo "Benchmark failed for base version, aborting"
  exit $RC
fi

# Compare performance
tests/performance/perf_compare.py -f1 "PERF_BASE" -f2 "PERF_CURRENT"

deactivate

popd
