import time
import subprocess
import os
from retry import retry
import json

from peloton_helper import (
    PelotonClientHelper,
    create_respool_for_new_peloton,
)
from color_print import (
    print_okblue,
    print_okgreen,
)

from modules import (
    Zookeeper,
    MesosMaster,
    MesosSlave,
    Peloton,
)

DEFAULT_PELOTON_NUM_LOG_FILES = 10


@retry(tries=3, delay=10)
def cassandra_operation(config, keyspace, create=True):
    """
    rtype: bool
    """
    cli = config.get('cassandra').get('cli')
    host = config.get('cassandra').get('host')
    port = config.get('cassandra').get('port', '9042')
    migrate_cli = config.get('cassandra').get('migrate_cli') \
        % os.environ['GOPATH']
    migrate_cmd = config.get('cassandra').get('cmd').get('migrate').format(
        host=host,
        port=port,
        keyspace=keyspace,
    )
    path = config.get('cassandra').get('path')

    version = config.get('cassandra').get('version')
    if create:
        # Create DB
        create_script = config.get('cassandra').get('cmd').get(
            'create').format(keyspace=keyspace)
        cmd_create = [cli, version, "-e", create_script, host, port]
        print_okgreen(
            "Step: creating Cassandra keyspace {keyspace} on "
            "{host}:{port}".format(
                host=host,
                port=port,
                keyspace=keyspace,
            )
        )
        return_create = subprocess.call(cmd_create)
        time.sleep(5)

        # Migrate DB
        cmd_migrate = [migrate_cli,
                       '--url', migrate_cmd,
                       '--path', path,
                       'up']
        print_okgreen(
            "Step: migrating Cassandra %s from %s" % (
                migrate_cmd, path
            )
        )

        return_migrate = subprocess.call(cmd_migrate)
        if return_create != 0 | return_migrate != 0:
            raise Exception("Cassandra DB migration failed")
    else:
        cmd = config.get('cassandra').get('cmd').get(
            'drop').format(keyspace=keyspace)
        print_okgreen(
            "Step: drop Cassandra keyspace {keyspace} "
            "on {host}:{port}".format(
                host=host,
                port=port,
                keyspace=keyspace,
            )
        )
        if subprocess.call([cli, version, "-e", cmd, host, port]) != 0:
            raise Exception("Cassandra DB drop failed")


class VCluster(object):

    APP_ORDER = [
        'hostmgr',
        'resmgr',
        'placement',
        'placement_stateless',
        'jobmgr']

    def __init__(self, config, label_name, zk_server, respool_path):
        """
        param config: vcluster configuration
        param label_name: label of the virtual cluster
        param zk_server: DNS address of the physical zookeeper server
        param respool_path: the path of the resource pool

        type config: dict
        type label_name: str
        type zk_server: str
        type respool: str
        """
        self.config = config
        self.label_name = label_name
        self.zk_server = zk_server
        self.respool_path = respool_path

        self.peloton_helper = PelotonClientHelper(zk_server, respool_path)

        self.zookeeper = Zookeeper(self.label_name, self.config,
                                   self.peloton_helper)
        self.mesos_master = MesosMaster(self.label_name, self.config,
                                        self.peloton_helper)
        self.mesos_slave = MesosSlave(self.label_name, self.config,
                                      self.peloton_helper)

        # Optionally includes Peloton apps
        self.peloton = Peloton(self.label_name, self.config,
                               self.peloton_helper)

        self.virtual_zookeeper = ''

        # vcluster is the config can be loaded for launching benchmark test
        # it can be dump into a file
        # the config filename starts with 'CONF_' and the label name
        self.vcluster_config = {
            'label_name': self.label_name,
            'config': self.config,
            'base_zk_server': self.zk_server,
            'base_respool_path': self.respool_path,
            'job_info': {}
        }
        self.config_name = 'CONF_' + label_name

    def output_vcluster_data(self):
        # write the vcluster data into a json file
        with open(self.config_name, 'w') as outfile:
            json.dump(self.vcluster_config, outfile)

    def start_mesos(self, agent_num):
        """
        param agent_num: Mesos-agent number of the virtual cluster
        type agent_num: int

        return host, port: zookeeper host, zookeeper port
        rtype host: str
        """
        # setup the VCluster on a physical Peloton cluster
        # create zookeeper
        print_okgreen('Step: creating virtual zookeeper with 1 instance')
        zookeeper_count = int(self.config.get('zookeeper').get(
            'instance_count'))
        self.vcluster_config['job_info']['zookeeper'] = (
            self.zookeeper.setup({}, zookeeper_count))

        # Get zookeeper tasks info
        host, port = self.zookeeper.get_host_port()
        if not host and port:
            raise Exception("Zookeeper launch failed")

        self.virtual_zookeeper = '%s:%s' % (host, port)

        zk_address = 'zk://%s/mesos' % self.virtual_zookeeper
        print_okgreen('Zookeeper created successfully: %s' % zk_address)

        # create mesos master
        self.start_mesos_master(self.virtual_zookeeper)

        # create mesos slaves
        self.start_mesos_slave(self.virtual_zookeeper, agent_num)

        self.vcluster_config.update({
            "Zookeeper": '%s:%s' % (host, port),
            "Mesos Slave Number": agent_num,
        })
        return host, port

    def start_peloton(self, virtual_zookeeper, agent_num, version=None,
                      skip_respool=False):
        """
        type zk_host: str
        type zk_port: str
        """
        # DB Migration
        cassandra_operation(self.config, keyspace=self.label_name,
                            create=True)
        host = self.config.get('cassandra').get('host')
        time.sleep(10)
        print_okblue("DB migration finished")

        # Setup Peloton
        print_okgreen('Step: Create Peloton, version: %s' % version)
        num_logs = self.config.get('peloton').get(
            'num_log_files', DEFAULT_PELOTON_NUM_LOG_FILES)

        for app in self.APP_ORDER:
            print_okblue('Creating peloton application: %s' % app)
            dynamic_env_master = {
                'APP': app,
                'ENVIRONMENT': 'production',
                'ELECTION_ZK_SERVERS': virtual_zookeeper,
                'MESOS_ZK_PATH': 'zk://%s/mesos' % virtual_zookeeper,
                'CASSANDRA_STORE': self.label_name,
                'CASSANDRA_HOSTS': host,
                'CASSANDRA_PORT': self.config.get('cassandra').get(
                    'port', '9042'),
                'CONTAINER_LOGGER_LOGROTATE_STDERR_OPTIONS':
                'rotate %s' % num_logs,
            }
            mesos_slave_config = self.config.get('mesos-slave', {})
            mesos_work_dir = [
                kv['value']
                for kv in mesos_slave_config.get('static_env', [])
                if kv.get('name') == 'MESOS_WORK_DIR']
            if mesos_work_dir:
                dynamic_env_master['MESOS_AGENT_WORK_DIR'] = mesos_work_dir[0]

            if app == 'hostmgr':
                dynamic_env_master['SCARCE_RESOURCE_TYPES'] = ','.join(
                    self.config.get('peloton').get(app).get(
                        'scarce_resource_types'))
                dynamic_env_master['SLACK_RESOURCE_TYPES'] = ','.join(
                    self.config.get('peloton').get(app).get(
                        'slack_resource_types'))
                dynamic_env_master['ENABLE_REVOCABLE_RESOURCES'] = \
                    str(self.config.get('peloton').get(app).get(
                        'enable_revocable_resources'))
            if app == "placement_stateless":
                dynamic_env_master['APP'] = 'placement'
                dynamic_env_master['TASK_TYPE'] = 'STATELESS'

            peloton_app_count = int(
                self.config.get('peloton').get(app).get('instance_count'))
            self.vcluster_config['job_info'][app] = (
                self.peloton.setup(
                    dynamic_env_master, peloton_app_count,
                    self.label_name + '_' + 'peloton-' + app,
                    version
                ))

        self.vcluster_config.update({
            'Peloton Version': version,
        })

        # create a default resource pool
        if not skip_respool:
            create_respool_for_new_peloton(
                self.config,
                zk_server=virtual_zookeeper,
                agent_num=agent_num,
            )

    def start_all(self, agent_num, peloton_version, skip_respool=False):
        """
        type agent_num: int
        """
        try:
            host, port = self.start_mesos(agent_num)
            virtual_zookeeper = '%s:%s' % (host, port)
            self.start_peloton(virtual_zookeeper, agent_num, peloton_version,
                               skip_respool=skip_respool)
            self.output_vcluster_data()
        except Exception as e:
            print 'Failed to create/configure vcluster: %s' % e
            self.teardown()
            raise

    def start_mesos_master(self, virtual_zookeeper):
        zk_address = 'zk://%s/mesos' % virtual_zookeeper
        print_okgreen('Step: creating virtual Mesos-master with 3 instance')
        dynamic_env_master = {
            self.config.get('mesos-master').get('dynamic_env'): zk_address,
        }
        mesos_count = int(
            self.config.get('mesos-master').get('instance_count'))
        self.vcluster_config['job_info']['mesos-master'] = (
            self.mesos_master.setup(dynamic_env_master, mesos_count))
        print_okgreen('Mesos-master created successfully.')

    def start_mesos_slave(self, virtual_zookeeper, agent_num):
        # create mesos slaves
        zk_address = 'zk://%s/mesos' % virtual_zookeeper
        print_okgreen(
            'Step: creating virtual Mesos-slave with %s instance' % agent_num
        )
        dynamic_env_slave = {
            self.config.get('mesos-slave').get('dynamic_env'): zk_address,
        }
        self.vcluster_config['job_info']['mesos-slave'] = (
            self.mesos_slave.setup(dynamic_env_slave, agent_num))
        print_okgreen('Mesos-slave created successfully.')

    def teardown_slave(self, remove=False):
        self.mesos_slave.teardown(remove=remove)

    def teardown_peloton(self, remove=False):
        print_okgreen('Step: stopping all peloton applications')
        for app in reversed(self.APP_ORDER):
            print_okblue('Stopping peloton application: %s' % app)
            self.peloton.teardown(self.label_name + '_' + 'peloton-' + app,
                                  remove=remove)

        print_okgreen('Step: drop the cassandra keyspace')
        cassandra_operation(self.config, keyspace=self.label_name,
                            create=False)

        try:
            os.remove(self.config_name)
        except OSError:
            pass

    def teardown(self, remove=False):
        self.teardown_peloton(remove=remove)

        print_okgreen('Step: stopping all virtual Mesos-slaves')
        self.teardown_slave(remove=remove)

        print_okgreen('Step: stopping all virtual Mesos-master')
        self.mesos_master.teardown(remove=remove)

        print_okgreen('Step: stopping all virtual Zookeeper')
        self.zookeeper.teardown(remove=remove)

    def get_vitual_zookeeper(self):
        if self.virtual_zookeeper:
            return self.virtual_zookeeper
        host, port = self.zookeeper.get_host_port()

        return '%s:%s' % (host, port)

    def get_mesos_master(self):
        zk_server = self.get_vitual_zookeeper()
        host, port = self.mesos_master.find_leader(zk_server)
        return '%s:%s' % (host, port)


def vcluster_from_conf(conf_file):
    with open(conf_file, 'r') as infile:
        conf = json.load(infile)
        vc = VCluster(conf["config"],
                      conf["label_name"],
                      conf["base_zk_server"],
                      conf["base_respool_path"])
        vc.virtual_zookeeper = conf["Zookeeper"]
        vc.vcluster_config['job_info'].update(conf['job_info'])
        return vc
