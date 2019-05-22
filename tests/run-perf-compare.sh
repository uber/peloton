#!/usr/bin/env bash
set -exo pipefail
# Perform performance comparison between two commits
#
# Depends on the following environment variables to be set
# 1. PELOTON_IMAGE_BASELINE : The baseline image of the form
#                             $docker-registry:$image:$version
# 2. PELOTON_IMAGE_CURRENT  : The image to compare the baseline to of the form
#                             $docker-registry:$image:$version
# 3. PELOTON_CLUSTER_CONFIG : The location of the config files for peloton apps.
#                             The directory structure at $PELOTON_CLUSTER_CONFIG
#                             is assumed to have the following structure
#                             |-- $PELOTON_CLUSTER_CONFIG
#                                └── config
#                                    ├── archiver
#                                    │   └── production.yaml
#                                    ├── aurorabridge
#                                    │   └── production.yaml
#                                    ├── executor
#                                    │   └── production.yaml
#                                    ├── hostmgr
#                                    │   └── production.yaml
#                                    ├── jobmgr
#                                    │   └── production.yaml
#                                    ├── placement
#                                    │   └── production.yaml
#                                    └── resmgr
#                                        └── production.yaml
# 4. ZOOKEEPER : The zookeeper of the cluster where to the perf tests will be run.
#
# Usage:
#      export PELOTON_IMAGE_BASELINE=...
#      export PELOTON_IMAGE_CURRENT=...
#      export PELOTON_CLUSTER_CONFIG=...
#      export ZOOKEEPER=...
#      ./run-perf-compare.sh
#

# validate
if [[ -z "${PELOTON_IMAGE_CURRENT}" ]]; then
  echo "Docker image for current Peloton version not set in environment variable PELOTON_IMAGE_CURRENT"
  exit 1
fi
if [[ -z "${PELOTON_IMAGE_BASELINE}" ]]; then
  echo "Docker image for baseline Peloton version not set in environment variable PELOTON_IMAGE_BASELINE"
  exit 1
fi
if [[ -z "${PELOTON_CLUSTER_CONFIG}" ]]; then
  echo "peloton cluster config location not set in environment variable PELOTON_CLUSTER_CONFIG"
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

[[ $(uname) == Darwin || -n $JENKINS_HOME ]] && docker_cmd='docker' || docker_cmd='sudo docker'

cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
root_dir=$(dirname "$cur_dir")

pushd ${root_dir}

# set GOPATH
if [[ -z ${GOPATH+x} ]] ; then
	workspace="$(pwd -P)/workspace"
	rm -rf "${workspace}" || :
	goDirPath="${workspace}/src/$(make project-name)"
  mkdir -p "$(dirname "$goDirPath")"
  if [[ ! -e "$goDirPath" ]]; then
    ln -sfv "$(dirname $workspace)" "$goDirPath"
  elif [[ ! -L "$goDirPath" ]]; then
    echo >&2 "error: $goDirPath already exists but is unexpectedly not a symlink"
    exit 1
  fi
	export GOPATH="$workspace"
fi

# install dependency
make vcluster

. env/bin/activate

# Number of mesos agents to spin up
num_agents=1000

run_perf_test()
{
  vcluster_name="$1"
  vcluster_args="$2"
  image="$3"
  outfile="$4"

  tools/vcluster/main.py \
  ${vcluster_args} \
  setup \
  -s ${num_agents} \
  -i ${image} \
  -c ${PELOTON_CLUSTER_CONFIG}

  # Make a note of the return code instead of failing this script
  # so that vcluster teardown is run even if the benchmark fails.
  rc=0
  tests/performance/multi_benchmark.py \
  ${MINI_TEST_SUITE} -i "CONF_${vcluster_name}" \
  -o ${outfile} || rc=$?
  tools/vcluster/main.py ${vcluster_args} teardown

  if [[ ${rc} -ne 0 ]]; then
    echo "Benchmarking failed, aborting"
    cleanup ${rc}
  fi
}

cleanup() {
    deactivate
    popd
    exit $1
}

# Run current version
vcluster_name="v${COMMIT_HASH:12:12}"
vcluster_args="-z ${ZOOKEEPER} -p /PelotonPerformance -n ${vcluster_name} --auth-type ${PELTON_AUTH_TYPE} --auth-config-file ${PELTON_AUTH_CONFIG_FILE}"
run_perf_test "${vcluster_name}" "${vcluster_args}" "${PELOTON_IMAGE_CURRENT}" "PERF_CURRENT"

# Run base version
vcluster_name="v${COMMIT_HASH:0:12}"
vcluster_args="-z ${ZOOKEEPER} -p /PelotonPerformance -n ${vcluster_name} --auth-type ${PELTON_AUTH_TYPE} --auth-config-file ${PELTON_AUTH_CONFIG_FILE}"
run_perf_test "${vcluster_name}" "${vcluster_args}" "${PELOTON_IMAGE_BASELINE}" "PERF_BASE"

# Compare performance
tests/performance/perf_compare.py -f1 "PERF_BASE" -f2 "PERF_CURRENT"

cleanup 0
