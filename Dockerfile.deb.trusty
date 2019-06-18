FROM ubuntu:trusty

VOLUME /output

RUN apt-get -yqq update && DEBIAN_FRONTEND=noninteractive apt-get -yqq install \
        dpkg-dev \
        lsb-release \
        curl \
        wget \
        make \
        unzip \
        git \
        ruby-dev \
        rubygems-integration \
        software-properties-common \
        python-dev \
        python-pip

RUN gem install fpm
RUN pip install virtualenv

RUN mkdir -p /gocode/src/github.com/uber/peloton
WORKDIR /gocode/src/github.com/uber/peloton

RUN curl -o go1.11.4.linux-amd64.tar.gz https://dl.google.com/go/go1.11.4.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.11.4.linux-amd64.tar.gz

ENV PATH="${PATH}:/usr/local/go/bin"
ENV GOROOT=/usr/local/go
ENV GOBIN=/usr/local/go/bin
ENV GOPATH=/gocode
ENV SRC_DIR=/gocode/src/github.com/uber/peloton

RUN curl https://glide.sh/get | sh

ADD . /gocode/src/github.com/uber/peloton

RUN rm -rf vendor && glide cc && glide install

ADD tools/packaging/peloton-release/deb/trusty/build.sh /build.sh
ADD tools/packaging/peloton-release/deb/build-common.sh /build-common.sh
ENTRYPOINT ["/build.sh"]
