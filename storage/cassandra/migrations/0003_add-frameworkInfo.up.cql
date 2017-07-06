/*
  frameworks table persists the framework id and mesos stream id for peloton
 */
CREATE TABLE IF NOT EXISTS frameworks (
  FrameworkName  text,
  FrameworkID    text,
  MesosStreamID  text,
  UpdateTime     timestamp,
  UpdateHost     text,
  PRIMARY KEY (FrameworkName)
);

