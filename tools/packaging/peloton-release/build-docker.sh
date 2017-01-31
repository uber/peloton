#!/usr/bin/env bash

set -euxo pipefail

[[ $(uname) == Darwin ]] && docker_cmd='docker' || docker_cmd='sudo docker'

# bump me up !
TAG=default-$(date +%s)
BRANCH=master
PRODUCTION=""
ATG=""
# TODO: add support for other distributions as needed
DISTRIBUTION="jessie"
DOCKERFILE_PATH=docker/$DISTRIBUTION
DOCKER_REGISTRY_URL="docker-registry01-sjc1:5055/test/infra/peloton"
ATG_DOCKER_REGISTRY_URL="docker.int.uberatc.com/uber-usi/infra/peloton"

while [ $# -gt 0 ] ; do
    case "$1" in
         "--tag" )
            TAG="$2"
            shift 2 ;;
         "--branch" )
            BRANCH="$2"
            shift 2 ;;
         "--prod" )
            PRODUCTION="$2"
            shift 2 ;;
         "--atg" )
            PRODUCTION="$2"
            ATG="$2"
            DOCKER_REGISTRY_URL=$ATG_DOCKER_REGISTRY_URL
            shift 2 ;;
         "--registry" )
            DOCKER_REGISTRY_URL="$2"
            shift 2 ;;
         * )
            shift 2 ;;
    esac
done

./build-debian.sh --package-name peloton.deb --branch $BRANCH
mv -f build/peloton.deb $DOCKERFILE_PATH
cp docker/entrypoint.sh $DOCKERFILE_PATH
$docker_cmd build -t infra/peloton:$TAG $DOCKERFILE_PATH
rm -rf build $DOCKERFILE_PATH/peloton.deb $DOCKERFILE_PATH/entrypoint.sh

if [ -n "$PRODUCTION" ]; then
   if [ -n "$ATG" ]; then
      $docker_cmd login docker.int.uberatc.com
   fi
   $docker_cmd tag infra/peloton:$TAG $DOCKER_REGISTRY_URL:$TAG
   $docker_cmd push $DOCKER_REGISTRY_URL:$TAG
   echo 'The image can now be pulled from docker registry at '${DOCKER_REGISTRY_URL}':'$TAG
   $docker_cmd rmi -f infra/peloton:$TAG
   $docker_cmd rmi -f $DOCKER_REGISTRY_URL:$TAG
else
   echo 'The image is available locally by tag infra/peloton:'$TAG
fi
