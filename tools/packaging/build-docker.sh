#!/usr/bin/env bash
# NOTE: this script is intended to *only* build the docker container
# $1 is the container name you want to build, including version (from make version)
set -exo pipefail
[[ $(uname) == Darwin || -n $JENKINS_HOME ]] && docker_cmd='docker' || docker_cmd='sudo docker'

image_name="${1:-peloton}"

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


# TODO(gabe) always do a glide install on the host before building an image
make install
# Clean-up stale pip.conf
rm -f ./docker/pip.conf
rm -rf env
${docker_cmd} build --build-arg GIT_REPO=. --no-cache -t "${image_name}" .
echo "Built ${image_name}"

