ARG GOLANG_VERSION=1.11.4

FROM golang:$GOLANG_VERSION

ENV CONFIG_DIR /etc/peloton
ENV ENVIRONMENT development
ENV PROTOC_VERSION 3.5.1
ENV BUILD_DIR /go/src/github.com/uber/peloton
ENV PATH $BUILD_DIR/bin:$PATH

# NOTE: python-dev is required for peloton to be launched with Aurora
RUN apt-get --allow-unauthenticated -yqq update \
  && DEBIAN_FRONTEND=noninteractive apt-get --allow-unauthenticated -yqq install \
  unzip \
  curl \
  vim \
  gdb \
  util-linux \
  python-dev \
  python-setuptools \
  python-pip

# Install protoc
RUN wget https://github.com/google/protobuf/releases/download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-linux-x86_64.zip \
  && unzip -d protoc protoc-$PROTOC_VERSION-linux-x86_64.zip \
  && rm protoc-$PROTOC_VERSION-linux-x86_64.zip \
  && cp protoc/bin/protoc /usr/local/bin \
  && cp -rf protoc/include/* /usr/local/include/. \
  && rm -rf protoc \
  && go get -u github.com/golang/protobuf/proto \
  && go get -u github.com/golang/protobuf/protoc-gen-go

ARG GIT_REPO=git-repo

ADD $GIT_REPO /go/src/github.com/uber/peloton
WORKDIR /go/src/github.com/uber/peloton

# setup config environment with default configurations
RUN mkdir /etc/peloton
COPY $GIT_REPO/config/ /etc/peloton/
COPY $GIT_REPO/docker/entrypoint.sh /bin/entrypoint.sh

# TODO(gabe): reenable me when we have no more closed source dependencies, and
# readd vendor/ to .dockerignore. For now, this relies on having an updated
# vendor/ directory on the host prior to performing the docker build...

# install glide and do glide install
# RUN make install

RUN make

RUN ( echo "Built Peloton" && peloton-jobmgr --version ) >&2 && cp ./bin/* /usr/bin/

ENTRYPOINT ["/bin/entrypoint.sh"]

EXPOSE 5290 5291 5292 5293 5297 5090
