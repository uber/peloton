FROM debian:jessie

WORKDIR /

RUN apt-get -yqq update && DEBIAN_FRONTEND=noninteractive apt-get -yqq install \
        dpkg-dev \
        curl \
        git \
        wget \
        make \
        unzip \
        ruby-dev \
        rubygems

RUN gem install fpm

# Install Go
ENV GO_VERSION 1.7.3
RUN curl -fsSL "https://golang.org/dl/go${GO_VERSION}.linux-amd64.tar.gz" \
| tar -xzC /usr/local

# Setup GOPATH inside the container
ENV GOPATH=/home/goroot
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

# Install go tools
ENV GO_TOOLS_COMMIT 823804e1ae08dbb14eb807afc7db9993bc9e3cc3
RUN git clone https://github.com/golang/tools.git $GOPATH/src/golang.org/x/tools \
	&& (cd $GOPATH/src/golang.org/x/tools && git checkout -q $GO_TOOLS_COMMIT)

# Grab Go's lint tool
ENV GO_LINT_COMMIT 32a87160691b3c96046c0c678fe57c5bef761456
RUN git clone https://github.com/golang/lint.git $GOPATH/src/github.com/golang/lint \
	&& (cd $GOPATH/src/github.com/golang/lint && git checkout -q $GO_LINT_COMMIT) \
	&& go install -v github.com/golang/lint/golint

# Install protoc
ENV PROTOC_VERSION 3.0.2
RUN wget https://github.com/google/protobuf/releases/download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-linux-x86_64.zip \
    && unzip -d protoc protoc-$PROTOC_VERSION-linux-x86_64.zip \
    && rm protoc-$PROTOC_VERSION-linux-x86_64.zip \
    && cp protoc/bin/protoc /usr/local/bin \
    && rm -rf protoc \
    && go get -u github.com/golang/protobuf/proto \
    && go get -u github.com/golang/protobuf/protoc-gen-go

# Change the workdir to the project root inside the the GOPATH
WORKDIR /home/goroot/src/code.uber.internal/infra/peloton
