#!/bin/bash -ex

PELOTON_HOME=/workspace/src/github.com/uber/peloton

function install_mesos_config {
  modprobe overlay || true
  modprobe aufs || true
  # Assign mesos command line arguments.
  cp $PELOTON_HOME/vagrant/mesos_config/etc_mesos-slave/* /etc/mesos-slave
  cp $PELOTON_HOME/vagrant/mesos_config/etc_mesos-master/* /etc/mesos-master
  systemctl stop mesos-master || true
  systemctl stop mesos-slave  || true
  # Remove slave metadata to ensure slave start does not pick up old state.
  rm -rf /var/lib/mesos/meta/slaves/latest
  systemctl start mesos-master
  systemctl start mesos-slave
}

function setup_workspace {
  # setup GOPATH and workspace
  cat >> /home/vagrant/.bashrc <<EOF

export GOPATH=/workspace
export PATH=$PATH:/workspace/bin
export PELOTON_HOME=/workspace/src/github.com/uber/peloton
export UBER_CONFIG_DIR=$PELOTON_HOME/config/master
export UBER_ENVIRONMENT=development

EOF

  # fix permission for /workspace
  sudo chown -R vagrant:vagrant /workspace

}

install_mesos_config
setup_workspace
