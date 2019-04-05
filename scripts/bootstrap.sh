#!/bin/bash

set -euxo pipefail

PROTOC_VERSION=3.5.1

function install_protoc_dev_server {
    wget https://github.com/google/protobuf/releases/download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-linux-x86_64.zip
    unzip -d protoc protoc-$PROTOC_VERSION-linux-x86_64.zip
    sudo cp protoc/bin/protoc /usr/bin
    sudo chmod 755 /usr/bin/protoc
    sudo cp -R protoc/include/* /usr/local/include/
    sudo chmod -R 755 /usr/local/include
    rm -r protoc
    rm protoc-$PROTOC_VERSION-linux-x86_64.zip

    # install protoc-gen-go plugin
    go get -u github.com/golang/protobuf/{proto,protoc-gen-go}
}

function install_protoc_mac {
    brew install protobuf
    go get -u -v github.com/golang/protobuf/proto
    go get -u -v github.com/golang/protobuf/protoc-gen-go
}


if [ "$(uname)" == "Darwin" ]; then
    install_protoc_mac
else
    install_protoc_dev_server
fi
