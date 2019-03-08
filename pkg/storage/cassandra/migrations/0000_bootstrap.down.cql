DROP index if exists job_index_lucene;

DROP TABLE IF EXISTS job_index;

DROP TABLE IF EXISTS upgrades;
DROP TABLE IF EXISTS persistent_volumes;
DROP MATERIALIZED VIEW IF EXISTS mv_respools_by_owner;
DROP TABLE IF EXISTS respools;
DROP TABLE IF EXISTS task_state_changes;
DROP TABLE IF EXISTS frameworks;

DROP MATERIALIZED VIEW IF EXISTS mv_task_by_state;

DROP MATERIALIZED VIEW IF EXISTS mv_job_by_state;

DROP TABLE IF EXISTS job_config;
DROP TABLE IF EXISTS job_runtime;
DROP TABLE IF EXISTS task_config;
DROP TABLE IF EXISTS task_runtime;
