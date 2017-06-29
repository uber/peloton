CREATE TABLE IF NOT EXISTS job_config (
  job_id        uuid,
  version       bigint,
  creation_time timestamp,
  config        blob,
  PRIMARY KEY (job_id, version)
);

CREATE TABLE IF NOT EXISTS job_runtime (
  job_id          uuid,
  state           text,
  update_time     timestamp,
  runtime_info    blob,
  PRIMARY KEY (job_id)
);

CREATE TABLE IF NOT EXISTS task_config (
  job_id          uuid,
  version         int,
  instance_id     int,
  creation_time   timestamp,
  config          blob,
  PRIMARY KEY (job_id, version, instance_id)
);

CREATE TABLE IF NOT EXISTS task_runtime (
  job_id          uuid,
  instance_id     int,
  update_time     timestamp,
  state           text,
  runtime_info    blob,
  PRIMARY KEY (job_id, instance_id)
);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_task_by_state AS
    SELECT job_id, instance_id, state FROM task_runtime
    WHERE job_id is not NULL and instance_id is not NULL and state is not NULL
    PRIMARY KEY ((job_id, state), instance_id);

/*
  JobState to job materialized view
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_job_by_state AS
    SELECT job_id, state, update_time FROM job_runtime
    WHERE job_id is not NULL and state is not NULL and update_time is not NULL
    PRIMARY KEY (state, job_id);

/*
  frameworks table persists the framework id and mesos stream id for peloton
 */
CREATE TABLE IF NOT EXISTS frameworks (
  framework_name  text,
  framework_id    text,
  mesos_stream_id text,
  update_time     timestamp,
  update_host     text,
  PRIMARY KEY (framework_name)
);

/*
  task_state_changes table persists the task state change for peloton
 */
CREATE TABLE IF NOT EXISTS task_state_changes (
  job_id      uuid,
  instance_id int,
  events      list<text>,
  PRIMARY KEY (job_id, instance_id)
);

/*
  respool table persists resource pools information for peloton
 */
CREATE TABLE IF NOT EXISTS respools (
  respool_id      text,
  respool_config  text,
  owner           text,
  creation_time   timestamp,
  update_time     timestamp,
  PRIMARY KEY (respool_id)
);

/*
  owner to respool materialized view
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_respools_by_owner AS
    SELECT respool_id, respool_config, owner, creation_time, update_time FROM respools
    WHERE owner is not NULL and respool_id is not NULL and creation_time is not NULL and update_time is not NULL and respool_config is not NULL
    PRIMARY KEY (owner, respool_id);

/*
  persistent_volumes table persists volumes info for peloton.
 */
CREATE TABLE IF NOT EXISTS persistent_volumes (
  volume_id       text,
  state           text,
  goal_state      text,
  job_id          text,
  instance_id     int,
  hostname        text,
  container_path  text,
  size_mb         int,
  creation_time   timestamp,
  update_time     timestamp,
  PRIMARY KEY (volume_id)
);

CREATE TABLE IF NOT EXISTS upgrades (
  upgrade_id  uuid,
  options     text,
  state       int,
  progress    int,
  instances   set<int>,
  job_id      uuid,
  job_config  text,
  PRIMARY KEY (upgrade_id)
);

/*
  job_index table merges the jobs info and runtime info. It would be used for supporting all
  queries.
 */
CREATE TABLE IF NOT EXISTS job_index (
  job_id          uuid,
  config          text,
  runtime_info    text,
  state           text,
  respool_id      text,
  owner           text,
  labels          text,
  creation_time   timestamp,
  update_time     timestamp,
  completion_time timestamp,
  start_time      timestamp,
  PRIMARY KEY (job_id)
);

/*
  create lucene index in cassandra store for job_index table
 */
CREATE CUSTOM INDEX IF NOT EXISTS job_index_lucene ON job_index ()
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
   'refresh_seconds': '10',
   'schema': '{
      fields: {
         owner: {type: "string"},
         labels: {type: "text", analyzer: "english"},
         config:{type: "text", analyzer: "english"},
         creation_time: {type: "date", pattern: "yyyy/MM/dd"},
         respool_id: {type: "string"},
         state: {type: "string"},
         update_time: {type: "date", pattern: "yyyy/MM/dd"},
         start_time: {type:"date", pattern: "yyyy/MM/dd"},
         completion_time: {type: "date", pattern: "yyyy/MM/dd"}
      }
   }'
};
