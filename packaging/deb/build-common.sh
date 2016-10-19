INSTALL_DIR=/peloton-install
SRC_DIR="${SRC_DIR:-/peloton}"
PROTOC_VERSION="3.0.2"
POST_INSTALL_FILE="${POST_INSTALL_FILE:-/post-install.sh}"

install_golang() {
    echo 'start installing golang 1.6'
    curl -O https://storage.googleapis.com/golang/go1.6.linux-amd64.tar.gz
    tar -xvf go1.6.linux-amd64.tar.gz
    mv go /usr/local
    export GOROOT=/usr/local/go
    export PATH=$PATH:$GOROOT/bin
}

install_protoc () {
    echo 'start installing protoc'
    wget https://github.com/google/protobuf/releases/download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-linux-x86_64.zip
    unzip -d protoc protoc-$PROTOC_VERSION-linux-x86_64.zip
    cp protoc/bin/protoc /usr/bin
    chmod 755 /usr/bin/protoc
    rm -r protoc
    rm protoc-$PROTOC_VERSION-linux-x86_64.zip

    # install protoc-gen-go plugin
    go get -u github.com/golang/protobuf/{proto,protoc-gen-go}
    cp $GOPATH/bin/protoc-gen-go /usr/bin
    chmod 755 /usr/bin/protoc-gen-go
}

build_peloton() {
    echo 'start building peloton'
    mkdir -p $GOPATH/src/code.uber.internal/infra/peloton
    cp -R $SRC_DIR/vendor/* $GOPATH/src
    cp -R $SRC_DIR $GOPATH/src/code.uber.internal/infra/
    cd $GOPATH/src/code.uber.internal/infra/peloton
    make
}

create_installation() {
    mkdir -p "$INSTALL_DIR"
    cp -R $GOPATH/src/code.uber.internal/infra/peloton/* $INSTALL_DIR
}


package() {(
    local opts=(
        -s dir
        -n peloton
        --iteration "$BUILD_ITERATION"
        --description
"Peloton is Uber's meta-framework for managing, scheduling and upgrading jobs on Mesos clusters.
 It has a few unique design priciples that differentiates itself from other Mesos meta-frameworks"
        --url=https://code.uberinternal.com/w/repo/infra/peloton/
        --license Uber
        -a amd64
        --category misc
        --vendor "Uber Technologies"
        -m compute@uber.com
        --prefix=/$INSTALL_DIR
        -t deb
        -p /pkg.deb
        --after-install $POST_INSTALL_FILE
    )

    cd "$INSTALL_DIR"
    ls
    fpm "${opts[@]}" -- .
)}
