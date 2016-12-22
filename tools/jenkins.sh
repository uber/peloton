#!/usr/bin/env bash
# This runs the jenkins tests inside a peloton docker container

set -euxo pipefail

# Constants
DOCKER=docker
DOCKER_TAG=test-peloton
DOCKER_CMD="/usr/bin/make jenkins"
DOCKER_GOPATH=/home/goroot/src/code.uber.internal/infra/peloton
DOCKER_NET_ARGS="--net=host"

# Builds the peloton docker container with all the dependencies
build_peloton_container() {
    ${workspace}/tools/peloton-dev/build.sh $1
}

# Starts mysql in a separate container as a peer to the peloton container
run_mysql_container(){
    ${workspace}/docker/run_test_mysql.sh
}


# Starts cassandra in a separate container as a peer to the peloton container
run_cassandra_container(){
    ${workspace}/docker/run_test_cassandra.sh
}

# Runs `make jenkins` inside the peloton container
run_jenkins() {
    # mounts the workspace inside the container's GOPATH
    docker_mount=$1:${DOCKER_GOPATH}
    ${DOCKER} run -t ${DOCKER_NET_ARGS} -v ${docker_mount} ${DOCKER_TAG} ${DOCKER_CMD}
}

usage() { echo "Usage: $0 [-w <workspace directory>]" 1>&2; exit 1; }

main() {
    workspace=""
    while getopts ":w:" o; do
        case "${o}" in
            w)
                workspace=${OPTARG}
                ;;
            *)
                usage
                ;;
        esac
    done
    shift $((OPTIND-1))

    if [ -z "${workspace}" ]; then
        usage
    fi

    build_peloton_container ${workspace}
    run_mysql_container
    run_cassandra_container
    run_jenkins ${workspace}
}

main "$@"
