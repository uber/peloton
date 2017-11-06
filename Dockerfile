FROM golang:1.8

ENV CONFIG_DIR /etc/peloton
ENV ENVIRONMENT development
ENV PROTOC_VERSION 3.0.2
ENV BUILD_DIR /go/src/code.uber.internal/infra/peloton
ENV PATH $BUILD_DIR/bin:$PATH

# NOTE: python-dev is required for peloton to be launched with Aurora
RUN apt-get -yqq update && DEBIAN_FRONTEND=noninteractive apt-get -yqq install \
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
    && rm -rf protoc \
    && go get -u github.com/golang/protobuf/proto \
    && go get -u github.com/golang/protobuf/protoc-gen-go

# TODO(gabe) update this path when we get a public namespace
COPY . /go/src/code.uber.internal/infra/peloton
WORKDIR /go/src/code.uber.internal/infra/peloton
# Copy pip.conf to be able to install internal packages
RUN mkdir /root/.pip
COPY ./docker/pip.conf /root/.pip/pip.conf

# TODO(gabe): reenable me when we have no more closed source dependencies, and
# readd vendor/ to .dockerignore. For now, this relies on having an updated
# vendor/ directory on the host prior to performing the docker build...

# install glide and do glide install
# RUN make install

RUN make

# setup config environment with default configurations
RUN mkdir /etc/peloton
COPY ./docker/default-config/ /etc/peloton/
COPY ./docker/entrypoint.sh /bin/entrypoint.sh

RUN ( echo "Built Peloton" && peloton-jobmgr --version ) >&2 && cp ./bin/* /usr/bin/

ENTRYPOINT ["/bin/entrypoint.sh"]

EXPOSE 5290 5291 5292 5293
