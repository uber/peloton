Packaging for > Pelton
===========================

This repository maintains configuration and tooling for building binary distributions and docker images of Peloton. Building
docker image is highly recommended.

## Building a debian binary

Binaries are built within Docker containers that provide the appropriate build environment for the target platform.
You will need to have a working Docker installation before proceeding.

To generate a debian package build from any machine(i.e. laptop, production host), follow below steps:
1. cd $PATH_TO_PELOTON/packaging
2.  ./build-debian.sh --build BUILD_NUMBER [optional : [--branch BRANCH_NAME] [--package_name PACKAGE_NAME] [--distribution DISTRIBUTION_NAME]]
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
2. cd peloton/packaging
3. ./build-docker.sh --tag [optional tag, but highly recommended to specify one][optional : [--branch BRANCH_NAME]
[--prod [Optional flag, specify 1 to push to uber docker registry, tunnel is required if running from non prod host]]
[--registry [Optional flag, the docker registry url to tag and push the docker image]]]

When building locally, the "docker push" part will be skipped.


## Running peloton container locally

On the host where you want to run peloton-master from, run the following command:

DOCKER_IMAGE_URL=infra/peloton:$TAG

First Peloton master:

sudo docker run --net host -d -i --name peloton1 $DOCKER_IMAGE_URL

Second Peloton master:

sudo docker run --net host -d -i --name peloton2 -e MASTER_PORT=5290 $DOCKER_IMAGE_URL

Repeat command to launch more peloton masters.


## Running peloton container in Prod/ATC

For prod/atc, make sure mysql container is running first, then run peloton container with '-e UBER_ENVIRONMENT=production',
plus '-e MESOS_ZK_PATH ${full mesos zk path}', '-e ELECTION_ZK_SERVERS=${dns of zk ensemble}', '-e DB_HOST=${db host name}'
and with the target docker image url, i.e docker-registry01-sjc1:5055/vendor/peloton:23 ,
refer to [Runbook](https://code.uberinternal.com/w/runbooks/peloton/operations/#docker-run-based) for more details.

Example command to run peloton master in sjc1-devel01:

sudo docker run --net host -d -i --name peloton -e UBER_ENVIRONMENT=production \
                           -e MESOS_ZK_PATH=zk://zookeeper-mesos-devel01-sjc1.uber.internal:2181/mesos \
                           -e DB_HOST=compute74-sjc1 \
                           -e ELECTION_ZK_SERVERS=zookeeper-mesos-devel01-sjc1.uber.internal \
                           docker-registry01-sjc1:5055/vendor/peloton:29

Repeat the same command on a different host to run another peloton master.


## Jenkins build job

### Docker

URL: https://ci-infra.uberinternal.com/job/peloton-build/

If no tag is specified, which is recommended for building from master, the build number will be used. When build is done,
search the bottom of [build log](https://ci-infra.uberinternal.com/job/peloton-build/label=apache-mesos/lastBuild/console)
for 'The image can now be pulled from docker registry at' for the docker image url

### Debian

URL: https://ci-infra.uberinternal.com/job/peloton/

Both jessie and trusty are supported via 'DISTRIBUTION' flag. jessie debian will be pushed to uber's internal jessie apt repo,
whereas trusty build artifact can be downloaded from 'https://ci-infra.uberinternal.com/job/peloton/ws/label/apache-mesos/build/'
since there is no trusty repo support at Uber yet.
