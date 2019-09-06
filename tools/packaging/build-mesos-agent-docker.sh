#!/usr/bin/env bash

set -exo pipefail
[[ $(uname) == Darwin || -n $JENKINS_HOME ]] && docker_cmd='docker' || docker_cmd='sudo docker'

image_name="${1:-peloton-mesos-agent}"

${docker_cmd} build --build-arg GIT_REPO=. -f Dockerfile.mesos-agent -t "${image_name}" .
echo "Built ${image_name}"
