#!/bin/bash
set -eo pipefail

[[ $(uname) == Darwin ]] && docker_cmd='docker' || docker_cmd='sudo docker'

DOCKER_REGISTRY_URL="docker-registry01-sjc1:5055/uber-usi/infra/peloton-mysql5.7.17"
ATG_DOCKER_REGISTRY_URL="docker.int.uberatc.com/uber-usi/infra/peloton-mysql5.7.17"
TAG="latest-test"

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

$docker_cmd build -t $DOCKER_REGISTRY_URL:$TAG .

if [ -n "$PRODUCTION" ]; then
   if [ -n "$ATG" ]; then
      $docker_cmd login docker.int.uberatc.com
   fi
   $docker_cmd push $DOCKER_REGISTRY_URL:$TAG
   echo 'The image can now be pulled from docker registry at '${DOCKER_REGISTRY_URL}':'$TAG
   $docker_cmd rmi -f $DOCKER_REGISTRY_URL:$TAG
else
   echo 'The image is available locally by tag '$DOCKER_REGISTRY_URL:$TAG
fi
