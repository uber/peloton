DROP TABLE IF EXISTS upgrades;

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
