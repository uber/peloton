#!/bin/bash

set -exo pipefail
[[ $(uname) == Darwin || -n $JENKINS_HOME ]] && docker_cmd='docker' || docker_cmd='sudo docker'

cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
root_dir=$(dirname "$cur_dir")

registry="docker-registry.pit-irn-1.uberatc.net"
image="vendor/peloton"

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


if [[ -z "${COMMIT_HASH}" ]]; then
  echo "No Version specified"
  COMMIT_HASH=`make commit-hash`
fi

# Run current version
vcluster_name="v${COMMIT_HASH:12:12}"
tools/vcluster/main.py -z zookeeper-mesos-preprod01.pit-irn-1.uberatc.net -p /DefaultResPool -n ${vcluster_name} setup -s 1000 -v ${CURRENT_VERSION}
tests/performance/multi_benchmark.py  -i "CONF_${vcluster_name}" -o "PERF_${CURRENT_VERSION}_CURRENT"
tools/vcluster/main.py -z zookeeper-mesos-preprod01.pit-irn-1.uberatc.net -p /DefaultResPool -n ${vcluster_name} teardown

# Run base version
vcluster_name="v${COMMIT_HASH:0:12}"
tools/vcluster/main.py -z zookeeper-mesos-preprod01.pit-irn-1.uberatc.net -p /DefaultResPool -n ${vcluster_name} setup -s 1000 -v ${BASE_VERSION}
tests/performance/multi_benchmark.py -i "CONF_${vcluster_name}" -o "PERF_${BASE_VERSION}_BASE"
tools/vcluster/main.py -z zookeeper-mesos-preprod01.pit-irn-1.uberatc.net -p /DefaultResPool -n ${vcluster_name} teardown

# Compare performance
tests/performance/perf_compare.py -f1 "PERF_${BASE_VERSION}_BASE" -f2 "PERF_${CURRENT_VERSION}_CURRENT"

deactivate

popd
