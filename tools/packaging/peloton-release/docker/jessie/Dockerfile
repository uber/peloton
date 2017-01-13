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

ENV UBER_CONFIG_DIR  /etc/peloton/config/master
ENV UBER_ENVIRONMENT development

# workdir is required until T581823 is fixed.
WORKDIR /peloton-install
ENTRYPOINT ["/entrypoint.sh"]
EXPOSE 5289 5290
