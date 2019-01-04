#!/usr/bin/env bash
# This runs the jenkins tests inside a peloton docker container
set -eo pipefail

# Constants
[[ $(uname) == Darwin || -n $JENKINS_HOME ]] && docker_cmd='docker' || docker_cmd='sudo docker'
package_path=github.com/uber/peloton

# Builds the peloton docker container with all the dependencies
build_peloton_container() {
    image="$1"
    echo "Building docker container $image"
    make -f "${workspace}/Makefile" docker IMAGE=$image
}

# This is a workaround for downloading the dependencies via glide outside the container since we don't have ssh
# keys inside the container
setup_gopath() {
  mkdir -p ".tmp/.goroot/src/${package_path}"
  chmod -R 777 .tmp
  ln -s "${workspace}" ".tmp/.goroot/src/${package_path}"
  export GOPATH="${workspace}/.tmp/.goroot"
}

# launch the test containers necessary to run integration/unit tests
run_test_containers(){
    make -f "${workspace}/Makefile" test-containers
}

# Runs `make jenkins` inside the peloton container
run_jenkins_in_container() {
    image="$1"
    echo "Running 'make jenkins' in ${image}"
    ${docker_cmd} run --rm -t \
      -v "${workspace}:/go/src/${package_path}" \
      --entrypoint make \
      --net=host \
      "${image}" jenkins
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

    setup_gopath
    build_peloton_container test-peloton
    run_test_containers
    run_jenkins_in_container test-peloton
}

main "$@"
