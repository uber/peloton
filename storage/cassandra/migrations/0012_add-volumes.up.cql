/*
  persistent_volumes table persists volumes info for peloton.
 */
CREATE TABLE IF NOT EXISTS persistent_volumes (
  ID                text,
  State             text,
  GoalState         text,
  JobID             text,
  InstanceID        int,
  Hostname          text,
  ContainerPath     text,
  SizeMb            int,
  CreateTime        timestamp,
  UpdateTime        timestamp,
  PRIMARY KEY (ID)
);
