FROM debian:jessie

MAINTAINER Uber Compute Platform <compute@uber.com>


RUN apt-get -yqq update
RUN apt-get -yqq install \
                    curl \
              python-dev \
                     vim \
                     gdb \
              util-linux

ADD peloton.deb /peloton.deb
ADD entrypoint.sh /entrypoint.sh
RUN dpkg -i peloton.deb

ENV PELOTON_CONFIG_DIR  /etc/peloton/config/master
ENV PELOTON_ENVIRONMENT development
# TODO: remove this once we have split master into 4 components
ENV PELOTON_COMPONENT        master

# workdir is required until T581823 is fixed.
WORKDIR /peloton-install
ENTRYPOINT ["/entrypoint.sh"]
EXPOSE 5289 5290
