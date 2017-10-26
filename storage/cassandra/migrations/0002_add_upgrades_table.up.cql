DROP TABLE IF EXISTS upgrades;

CREATE TABLE IF NOT EXISTS upgrades (
  upgrade_id          uuid,
  options             blob,
  status              blob,
  from_config_version bigint,
  to_config_version   bigint,
  state               int,
  PRIMARY KEY (upgrade_id)
);
