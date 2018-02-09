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

# start vCluster
tools/vcluster/main.py -z zookeeper-mesos-preprod01.pit-irn-1.uberatc.net -p /DefaultResPool -n $COMMIT_HASH setup -s 1000 -v $VERSION

# run benchmark
tests/performance/multi_benchmark.py

# teardown vCluster
tools/vcluster/main.py -z zookeeper-mesos-preprod01.pit-irn-1.uberatc.net -p /DefaultResPool -n $COMMIT_HASH teardown

deactivate

popd
