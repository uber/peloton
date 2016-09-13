#!/bin/bash -ex

PELOTON_HOME=/workspace/src/code.uber.internal/infra/peloton

function install_mesos_config {
  # Assign mesos command line arguments.
  cp $PELOTON_HOME/vagrant/mesos_config/etc_mesos-slave/* /etc/mesos-slave
  cp $PELOTON_HOME/vagrant/mesos_config/etc_mesos-master/* /etc/mesos-master
  stop mesos-master || true
  stop mesos-slave || true
  # Remove slave metadata to ensure slave start does not pick up old state.
  rm -rf /var/lib/mesos/meta/slaves/latest
  start mesos-master
  start mesos-slave
}

function setup_workspace {
  # setup GOPATH and workspace
  cat >> /home/vagrant/.bashrc <<EOF

export GOPATH=/workspace
export PATH=$PATH:/workspace/bin
export PELOTON_HOME=/workspace/src/code.uber.internal/infra/peloton
export UBER_CONFIG_DIR=$PELOTON_HOME/config/master
export UBER_ENVIRONMENT=development

EOF

  # fix permission for /workspace
  sudo chown -R vagrant:vagrant /workspace

}

install_mesos_config
setup_workspace
