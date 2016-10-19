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

ADD peloton /peloton
ADD build.sh /build.sh
ADD build-common.sh /build-common.sh
ADD post-install.sh /post-install.sh
