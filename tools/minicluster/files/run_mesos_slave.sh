#!/bin/bash

# Remove slave metadata to ensure slave start does not pick up old state.
rm -rf /var/lib/mesos/meta/slaves/latest

# Install python2.7 to be used by thermos executor
mkdir -p /usr/local/lib/python2.7 \
    && mv /usr/lib/python2.7/site-packages /usr/local/lib/python2.7/dist-package \
    && ln -s /usr/local/lib/python2.7/dist-packages /usr/lib/python2.7/site-packages
apt-get -yqq update \
    && DEBIAN_FRONTEND=noninteractive apt-get -yqq install python2.7

# Install thermos executor
mkdir -p /usr/share/aurora/bin
cp /files/thermos_executor_0.19.1.pex /usr/share/aurora/bin/thermos_executor.pex

mesos-slave
