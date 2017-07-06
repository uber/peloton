/*
  owner to respool materialized view
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_respools_by_owner AS
    SELECT ID, ResourcePoolConfig, Owner, CreateTime, UpdateTime FROM respools
    WHERE Owner is not NULL and ID is not NULL and CreateTime is not NULL and UpdateTime is not NULL and ResourcePoolConfig is not NULL
    PRIMARY KEY (Owner, ID);
