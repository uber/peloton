#!/bin/bash

set -euxo pipefail

PROTOC_VERSION=3.0.2

function install_protoc_dev_server {
    wget https://github.com/google/protobuf/releases/download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-linux-x86_64.zip
    unzip -d protoc protoc-$PROTOC_VERSION-linux-x86_64.zip
    sudo cp protoc/bin/protoc /usr/bin
    sudo chmod 755 /usr/bin/protoc
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

# Until T663752 is resolved, use an older version of docker-py which
# is compatible with docker 1.9.1 (jenkins box) and 1.12.x (Laptop).
pip install docker-py==1.7.2
