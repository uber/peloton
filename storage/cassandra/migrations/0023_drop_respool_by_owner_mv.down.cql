/*
  owner to respool materialized view
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_respools_by_owner AS
    SELECT respool_id, respool_config, owner, creation_time, update_time FROM respools
    WHERE owner is not NULL and respool_id is not NULL and creation_time is not NULL and update_time is not NULL and respool_config is not NULL
    PRIMARY KEY (owner, respool_id);
