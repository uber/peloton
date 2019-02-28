/*
  secret_info table persists the secret info for peloton.
 */
CREATE TABLE IF NOT EXISTS secret_info (
  secret_id         uuid,
  job_id            uuid,
  path              text,
  data              text,
  creation_time     timestamp,
  /* 
    We may decide on different formats to store secrets in the future
    They may be encrypted, may have a preamble, may just contain a reference
    Version number will help interpret the secret
   */
  version           bigint,
  valid             boolean,
  PRIMARY KEY (secret_id, valid)
);
