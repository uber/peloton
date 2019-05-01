import os
import yaml

from peloton_client.pbgen.mesos.v1 import mesos_pb2 as mesos
from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task
from peloton_client.pbgen.peloton.api.v0.respool import respool_pb2 as respool
from peloton_client.pbgen.peloton.api.v0 import peloton_pb2 as peloton


def load_config(config_file):
    with open(config_file, "r") as f:
        config = yaml.load(f)
    return config


# create task config to launch Mesos Containerizer container
def create_mesos_task_config(
    config, module, dynamic_env, version=None, image_path=None
):
    resource_config = config.get(module).get("resource")
    ports = config.get(module).get("ports", [])
    ports_config = []
    for port in ports:
        ports_config.append(task.PortConfig(name=port, envName=port))

    environments = []
    start_cmd = config.get(module).get("start_command", "")
    static_envs = config.get(module).get("static_env", [])
    fetch_files = config.get(module).get("fetch_files", [])

    for static_env in static_envs:
        environments.append(
            mesos.Environment.Variable(
                name=static_env["name"], value=static_env["value"]
            )
        )

    for dyn_env_name, dyn_env_value in dynamic_env.iteritems():
        environments.append(
            mesos.Environment.Variable(name=dyn_env_name, value=dyn_env_value)
        )

    if image_path:
        docker_image = image_path
    else:
        docker_registry = config.get(module).get("docker_registry")
        image_path = config.get(module).get("image_path")
        if not version:
            version = config.get(module).get("version")

        docker_image = (
            os.path.join(docker_registry, image_path) + ":" + version
        )

    return task.TaskConfig(
        resource=task.ResourceConfig(**resource_config),
        ports=ports_config,
        container=mesos.ContainerInfo(
            type="MESOS",
            mesos=mesos.ContainerInfo.MesosInfo(
                image=mesos.Image(
                    type="DOCKER", docker=mesos.Image.Docker(name=docker_image)
                )
            ),
        ),
        command=mesos.CommandInfo(
            uris=[
                mesos.CommandInfo.URI(
                    value=fetch_file["source"],
                    output_file=fetch_file["name"],
                    cache=bool(fetch_file.get("cache", False)),
                    executable=bool(fetch_file.get("executable", False)),
                )
                for fetch_file in fetch_files
            ],
            shell=True,
            value=start_cmd,
            environment=mesos.Environment(variables=environments),
        ),
    )


def create_pool_config(name, cpu, memory, disk):
    """
    type name: string
    type cpu: float
    type memory: float
    type disk: float
    rtype: respool.ResourcePoolConfig
    """
    return respool.ResourcePoolConfig(
        name=name,
        resources=[
            respool.ResourceConfig(
                kind="cpu", reservation=cpu, limit=cpu, share=1
            ),
            respool.ResourceConfig(
                kind="memory", reservation=memory, limit=memory, share=1
            ),
            respool.ResourceConfig(
                kind="disk", reservation=disk, limit=disk, share=1
            ),
        ],
        parent=peloton.ResourcePoolID(value="root"),
    )
