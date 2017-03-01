#!/usr/bin/env bash
# NOTE: this script is intended to *only* build the docker container
# $1 is the container name you want to build, including version (from make version)
set -euxo pipefail
[[ $(uname) == Darwin || -n $JENKINS_HOME ]] && docker_cmd='docker' || docker_cmd='sudo docker'

image_name="${1:-peloton}"
# TODO(gabe) always do a glide install on the host before building an image
make install
"${docker_cmd}" build -t "${image_name}" .
echo "Built ${image_name}"

