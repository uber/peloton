import os
import yaml

from peloton_client.pbgen.mesos.v1 import mesos_pb2 as mesos
from peloton_client.pbgen.peloton.api.task import task_pb2 as task


def load_config():
    config_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'config.yaml')
    with open(config_file, 'r') as f:
        config = yaml.load(f)
    return config


config = load_config()


# create task config to launch Docker container
def create_task_config(module, dynamic_env):
    """
    param module: the module name
    type module: str
    param dynamic_env: dict of dynamic environment variables
    type dynamic_env: dict

    rtype: task.TaskConfig
    """
    resource_config = config.get(module).get('resource')
    ports = config.get(module).get('ports')
    ports_config = []
    for port in ports:
        ports_config.append(
            task.PortConfig(
                name=port,
                envName=port,
            )
        )

    mesos_parameters = []
    start_cmd = config.get(module).get('start_command')
    static_envs = config.get(module).get('static_env')
    fetch_files = config.get(module).get('fetch_files', [])

    for static_env in static_envs:
        mesos_parameters.append(
            mesos.Parameter(
                key='env',
                value=static_env['name'] + '=' + static_env['value'],
            )
        )

    for dyn_env_name, dyn_env_value in dynamic_env.iteritems():
        mesos_parameters.append(
            mesos.Parameter(
                key='env',
                value=dyn_env_name + '=' + dyn_env_value,
            )
        )

    return task.TaskConfig(
        resource=task.ResourceConfig(**resource_config),
        ports=ports_config,
        container=mesos.ContainerInfo(
            type='DOCKER',
            docker=mesos.ContainerInfo.DockerInfo(
                image=config.get(module).get('image'),
                parameters=mesos_parameters,
                privileged=True,
            ),
        ),
        command=mesos.CommandInfo(
            uris=[mesos.CommandInfo.URI(
                value=fetch_file['source'],
                output_file=fetch_file['name'],
            ) for fetch_file in fetch_files],
            shell=True,
            value=start_cmd,
        ),

    )


# create task config to launch Mesos Containerizer container
def create_mesos_task_config(module, dynamic_env, docker_image=None):
    resource_config = config.get(module).get('resource')
    ports = config.get(module).get('ports')
    ports_config = []
    for port in ports:
        ports_config.append(
            task.PortConfig(
                name=port,
                envName=port,
            )
        )

    environments = []
    start_cmd = config.get(module).get('start_command')
    static_envs = config.get(module).get('static_env')
    fetch_files = config.get(module).get('fetch_files', [])

    for static_env in static_envs:
        environments.append(
            mesos.Environment.Variable(
                name=static_env['name'],
                value=static_env['value'],
            )
        )

    for dyn_env_name, dyn_env_value in dynamic_env.iteritems():
        environments.append(
            mesos.Environment.Variable(
                name=dyn_env_name,
                value=dyn_env_value,
            )
        )

    if not docker_image:
        docker_image = config.get(module).get('image')

    return task.TaskConfig(
        resource=task.ResourceConfig(**resource_config),
        ports=ports_config,
        container=mesos.ContainerInfo(
            type='MESOS',
            mesos=mesos.ContainerInfo.MesosInfo(
                image=mesos.Image(
                    type='DOCKER',
                    docker=mesos.Image.Docker(
                        name=docker_image,

                    )
                )
            ),
        ),
        command=mesos.CommandInfo(
            uris=[mesos.CommandInfo.URI(
                value=fetch_file['source'],
                output_file=fetch_file['name'],
            ) for fetch_file in fetch_files],
            shell=True,
            value=start_cmd,
            environment=mesos.Environment(
                variables=environments
            )
        ),

    )
