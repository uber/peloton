/*
  respool table persists resource pools information for peloton
 */
CREATE TABLE IF NOT EXISTS respools (
  ID                     text,
  ResourcePoolConfig     text,
  Owner                  text,
  CreateTime             timestamp,
  UpdateTime             timestamp,
  PRIMARY KEY (ID)
);
