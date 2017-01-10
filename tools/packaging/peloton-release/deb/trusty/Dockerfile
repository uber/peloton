FROM ubuntu:trusty

WORKDIR /

RUN apt-get -yqq update && DEBIAN_FRONTEND=noninteractive apt-get -yqq install \
        dpkg-dev \
        curl \
        git \
        wget \
        make \
        unzip \
        ruby-dev \
        rubygems-integration \
        software-properties-common

RUN gem install fpm

RUN add-apt-repository ppa:ubuntu-lxc/lxd-stable -y \
    && apt-get update \
    && apt-get install -y golang

ADD peloton /peloton
ADD build.sh /build.sh
ADD build-common.sh /build-common.sh
ADD post-install.sh /post-install.sh
