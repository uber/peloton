#!/usr/bin/env bash

set -euxo pipefail

# bump me up !
TAG=default-$(date +%s)
BRANCH=master
PRODUCTION=""
# TODO: add support for other distributions as needed
DISTRIBUTION="jessie"
DOCKERFILE_PATH=docker/$DISTRIBUTION
DOCKER_REGISTRY_URL="docker-registry01-sjc1:5055/test"

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
sudo docker build -t infra/peloton:$TAG $DOCKERFILE_PATH
rm -rf build $DOCKERFILE_PATH/peloton.deb $DOCKERFILE_PATH/entrypoint.sh

if [ -n "$PRODUCTION" ]; then
   sudo docker tag infra/peloton:$TAG $DOCKER_REGISTRY_URL/peloton:$TAG
   sudo docker push $DOCKER_REGISTRY_URL/peloton:$TAG
   echo 'The image can now be pulled from docker registry at '${DOCKER_REGISTRY_URL}'/peloton:'$TAG
   sudo docker rmi -f infra/peloton:$TAG
   sudo docker rmi -f $DOCKER_REGISTRY_URL/peloton:$TAG
else
   echo 'The image is available locally by tag infra/peloton:'$TAG
fi
