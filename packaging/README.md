Packaging for > Pelton
===========================

This repository maintains configuration and tooling for building binary distributions and docker images of Peloton. Building
docker image is highly recommended.

## Building a debian binary

Binaries are built within Docker containers that provide the appropriate build environment for the target platform.
You will need to have a working Docker installation before proceeding.

To generate a debian package build from any machine(i.e. laptop, production host), follow below steps:
1. cd $PATH_TO_PELOTON/builder
2.  ./build-artifact.sh --build BUILD_NUMBER [optional : [--branch BRANCH_NAME] [--package_name PACKAGE_NAME] [--distribution DISTRIBUTION_NAME]]
Once build is done, a .deb file with name pattern 'peloton-{release ver}-{build number}-3_amd64.deb' will be generated
under the "build" directory


## Installing peloton debian package

simply run dpkg -i $PATH_TO_BINARY/peloton-{release}-{build}-3_amd64.deb on dev server or any host, then peloton will be
installed under /peloton-install directory


Running peloton on dev server

export PELOTON_HOME=/peloton-install

export UBER_CONFIG_DIR=$PELOTON_HOME/config/master

export UBER_ENVIRONMENT=development

all executables can be run from $PELOTON_HOME/bin


## Building a docker image

A docker image can be built by leveraging the debian package build script, then get pushed to internal docker registry.

To generate a build from any machine(i.e. laptop, production host), follow below steps:

1. git clone gitolite@code.uber.internal:infra/peloton
2. cd peloton/builder
3. ./docker-build.sh --tag [optional tag, but highly recommended to specify one][optional : [--branch BRANCH_NAME]
[--prod [Optional flag, specify 1 to push to uber docker registry, tunnel is required if running from non prod host]]]

When building locally, the "docker push" part will be skipped.


## Running peloton container locally

On the host where you want to run peloton-master from, run the following command:

DOCKER_IMAGE_URL=infra/peloton:$TAG

Leader :

sudo docker run --net host -d -i --name peloton-leader $DOCKER_IMAGE_URL

Follower :

sudo docker run --net host -d -i --name peloton-follower -e ROLE=follower $DOCKER_IMAGE_URL

## Running peloton container in Prod

For prod, use DOCKER_IMAGE_URL=localhost:15055/test/peloton:$TAG with '-e UBER_ENVIRONMENT=production', refer to runbook
for more details
