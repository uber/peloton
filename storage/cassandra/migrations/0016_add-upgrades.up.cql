CREATE TABLE IF NOT EXISTS upgrades (
  ID             text,
  Options        text,
  State          int,
  Progress       int,
  Instances      set<int>,
  JobID          text,
  JobConfig      text,
  PRIMARY KEY (ID)
);
