#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -o errexit
set -o nounset
set -o verbose

readonly MESOS_VERSION=1.2.0

function remove_unused {
  rm -f /home/vagrant/VBoxGuestAdditions.iso
}

function install_base_packages {
  apt-get -y install \
      bison \
      curl \
      git \
      jq \
      unzip \
      python-dev \
      zookeeperd
}

function install_golang {
  add-apt-repository ppa:ubuntu-lxc/lxd-stable -y
  apt-get update
  apt-get install -y golang

}

function install_protoc {
    wget https://github.com/google/protobuf/releases/download/v3.0.2/protoc-3.0.2-linux-x86_64.zip
    unzip -d protoc protoc-3.0.2-linux-x86_64.zip
    cp protoc/bin/protoc /usr/bin
    chmod 755 /usr/bin/protoc
    rm -r protoc
    rm protoc-3.0.2-linux-x86_64.zip

    # install protoc-gen-go plugin
    export GOPATH=/workspace
    go get -u github.com/golang/protobuf/{proto,protoc-gen-go}
}

function install_docker {
  # Instructions from https://docs.docker.com/engine/installation/linux/ubuntulinux/
  apt-get install -y apt-transport-https ca-certificates
  apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 \
    --recv-keys 58118E89F3A912897C070ADBF76221572C52609D
  echo 'deb https://apt.dockerproject.org/repo ubuntu-xenial main' \
    > /etc/apt/sources.list.d/docker.list
  apt-get update
  apt-get -y install \
    linux-image-extra-$(uname -r) \
    apparmor \
    docker-engine
}

function install_mesos {
  apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF
  DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
  CODENAME=$(lsb_release -cs)
  echo "deb http://repos.mesosphere.io/${DISTRO} ${CODENAME} main" \
      > /etc/apt/sources.list.d/mesosphere.list
  apt-get update
  apt-get -y install mesos=${MESOS_VERSION}*
}

function compact_box {
  apt-get clean

  # By design, this will fail as it writes until the disk is full.
  dd if=/dev/zero of=/junk bs=1M || true
  rm -f /junk
  sync
}

remove_unused
install_base_packages
install_golang
install_protoc
install_docker
install_mesos
compact_box

