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

# Disable checkstyle for this entire file as it is a pystachio schema.
# checkstyle: noqa

from pystachio import Choice

from aurora.schema.thermos.schema import *



# TODO(wickman) Bind {{mesos.instance}} to %shard_id%
class MesosContext(Struct):
  # The instance id (i.e. replica id, shard id) in the context of a task
  instance    = Required(Integer)
  hostname    = Required(String)


class UpdateConfig(Struct):
  batch_size                  = Default(Integer, 1)
  watch_secs                  = Default(Integer, 45)
  max_per_shard_failures      = Default(Integer, 0)
  max_total_failures          = Default(Integer, 0)
  rollback_on_failure         = Default(Boolean, True)
  wait_for_batch_completion   = Default(Boolean, False)
  pulse_interval_secs         = Integer


class HttpHealthChecker(Struct):
  endpoint                 = Default(String, '/health')
  expected_response        = Default(String, 'ok')
  expected_response_code   = Default(Integer, 0)


class ShellHealthChecker(Struct):
  shell_command            = Required(String)


class HealthCheckerConfig(Struct):
  http                     = HttpHealthChecker
  shell                    = ShellHealthChecker


DefaultHealthChecker      = HealthCheckerConfig(http=HttpHealthChecker())


class HealthCheckConfig(Struct):
  health_checker            = Default(HealthCheckerConfig, DefaultHealthChecker)
  initial_interval_secs     = Default(Float, 15.0)
  interval_secs             = Default(Float, 10.0)
  max_consecutive_failures  = Default(Integer, 0)
  min_consecutive_successes = Default(Integer, 1)
  timeout_secs              = Default(Float, 1.0)


class HttpLifecycleConfig(Struct):
  # Named port to POST shutdown endpoints
  port = Default(String, 'health')

  # Endpoint to hit to indicate that a task should gracefully shutdown.
  graceful_shutdown_endpoint = Default(String, '/quitquitquit')

  # Endpoint to hit to give a task it's final warning before being killed.
  shutdown_endpoint = Default(String, '/abortabortabort')


class LifecycleConfig(Struct):
  http = HttpLifecycleConfig


DisableLifecycle = LifecycleConfig()
DefaultLifecycleConfig = LifecycleConfig(http = HttpLifecycleConfig())


class Announcer(Struct):
  primary_port = Default(String, 'http')

  # Portmap can either alias two ports together, e.g.
  #   aurora <= http
  # Or it can be used to alias static ports to endpoints, e.g.
  #   http <= 80
  #   https <= 443
  #   aurora <= https
  portmap      = Default(Map(String, String), {
    'aurora': '{{primary_port}}'
  })

  # Root of tree where annoucements are stored. If specified, this overrides the
  # default path (executor must be started with --announcer-allow-custom-serverset-path for
  # this setting to take effect)
  zk_path      = String


# The executorConfig populated inside of TaskConfig.
class MesosTaskInstance(Struct):
  task                = Required(Task)
  instance            = Required(Integer)
  role                = Required(String)
  announce            = Announcer
  environment         = Required(String)
  health_check_config = Default(HealthCheckConfig, HealthCheckConfig())
  lifecycle           = LifecycleConfig


class Parameter(Struct):
  name = Required(String)
  value = Required(String)


class Docker(Struct):
  image = Required(String)
  parameters = Default(List(Parameter), [])


class AppcImage(Struct):
  name = Required(String)
  image_id = Required(String)


class DockerImage(Struct):
  name = Required(String)
  tag = Required(String)

Mode = Enum('RO', 'RW')

class Volume(Struct):
  container_path = Required(String)
  host_path = Required(String)
  mode = Required(Mode)

class Mesos(Struct):
  image = Choice([AppcImage, DockerImage])
  volumes = Default(List(Volume), [])


class Container(Struct):
  docker = Docker


class MesosJob(Struct):
  name          = Default(String, '{{task.name}}')
  role          = Required(String)
  contact       = String
  cluster       = Required(String)
  environment   = Required(String)
  instances     = Default(Integer, 1)
  task          = Required(Task)
  announce      = Announcer
  tier          = String

  cron_schedule = String
  cron_collision_policy = Default(String, "KILL_EXISTING")

  update_config = Default(UpdateConfig, UpdateConfig())

  constraints                = Map(String, String)
  service                    = Default(Boolean, False)
  max_task_failures          = Default(Integer, 1)
  production                 = Default(Boolean, False)
  priority                   = Default(Integer, 0)
  health_check_config        = Default(HealthCheckConfig, HealthCheckConfig())
  # TODO(wickman) Make Default(Any, LifecycleConfig()) once pystachio #17 is addressed.
  lifecycle                  = Default(LifecycleConfig, DefaultLifecycleConfig)
  task_links                 = Map(String, String)  # Unsupported.  See AURORA-739

  enable_hooks = Default(Boolean, False)  # enable client API hooks; from env python-list 'hooks'

  # Specifying a `Container` with a `docker` property for Docker jobs is deprecated, instead just
  # specify the value of the container property to be a `Docker` container directly.
  container = Choice([Container, Docker, Mesos])


Job = MesosJob
Service = Job(service = True)
