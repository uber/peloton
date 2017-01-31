#!/usr/bin/env bash

set -euxo pipefail

[[ $(uname) == Darwin ]] && docker_cmd='docker' || docker_cmd='sudo docker'

PELOTON_REPO="gitolite@code.uber.internal:infra/peloton.git"
SRC_DIR=peloton
RELEASE_VERSION="1.0"
BUILD_DIR="build"
BUILDER_PATH="deb"
BUILDER_COMMON_FILE="$BUILDER_PATH/build-common.sh"
POST_INSTALL_SCRIPT="$BUILDER_PATH/post-install.sh"
DEFAULT_DISTRIBUTION="jessie"

main() {
    local builder_dir="$BUILDER_PATH/$DEFAULT_DISTRIBUTION"
    local build_iteration="1"
    local package_name=
    local repo="$PELOTON_REPO"
    local no_cache=false
    local release_version="$RELEASE_VERSION"
    local build_branch="origin/master"
    while [ $# -gt 0 ] ; do
        case "$1" in
            "--distribution" )
                builder_dir="$BUILDER_PATH/$2"
                shift 2 ;;
            "--build" )
                build_iteration="$2"
                shift 2 ;;
            "--branch" )
                build_branch="$2"
                shift 2 ;;
            "--package-name" )
                package_name="$2"
                shift 2 ;;
            "--no-cache" )
                no_cache=true
                shift 1 ;;
            * )
                echo "illegal flag: $1"
                usage
                exit 1 ;;
        esac
    done

    fetch_peloton "$repo" "$build_branch" "$builder_dir"

    if ! [ "$package_name" ] ; then
        local dist=`basename $builder_dir`
        package_name="peloton-${release_version}-"
        package_name+="${build_iteration}_${dist}.deb"
    fi

    local image_name="peloton-$release_version-$build_iteration"
    local container_name="${image_name}-run"

    # Dockerfile doesn't like file or symlink from other directories, so build-common.sh has to be moved into builder dir
    cp $BUILDER_COMMON_FILE $builder_dir
    cp $POST_INSTALL_SCRIPT $builder_dir
    if $no_cache ; then
        $docker_cmd build --no-cache -t "$image_name" "$builder_dir"
    else
        $docker_cmd build -t "$image_name" "$builder_dir"
    fi

    $docker_cmd run \
        --name "$container_name" \
        -e "BUILD_ITERATION=$build_iteration" \
        -e "SRC_DIR=/$SRC_DIR" \
        -t "$image_name" /build.sh

    rm -rf $BUILD_DIR
    mkdir -p $BUILD_DIR
    $docker_cmd cp "$container_name:/pkg.deb" $BUILD_DIR/"$package_name"
    $docker_cmd rm -f "$container_name"
    $docker_cmd rmi -f "$image_name"

    echo "Produced artifacts $BUILD_DIR/$package_name"
    # clean up temporarily generated files
    rm -rf "$builder_dir/$SRC_DIR"
    rm -f "$builder_dir/build-common.sh"
    rm -f "$builder_dir/post-install.sh"
}


fetch_peloton() {
    local repo_url="$1"
    local value="$2"
    local src_dir="$3"/"$SRC_DIR"

    rm -rf $src_dir
    mkdir -p workspace/src
    export GOPATH=$PWD/workspace
    git clone "$repo_url" "$src_dir"
    ( cd "$src_dir" && git checkout -f "$value" )

    cd $src_dir
    # run glide install to pull all dependencies into 'vendor' folder
    glide install
    cd -
    rm -rf workspace
}


usage() {
    echo "[--build BUILD] [--branch BRANCH_NAME] [--distribution DISTRIBUTION] [--package_name PACKAGE_NAME]"
}


if [ $# -eq 0 ] ; then
    usage
    exit 1
fi

case "$1" in
    -h|--help ) usage ;;
    * ) main "$@" ;;
esac