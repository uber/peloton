CREATE CUSTOM INDEX IF NOT EXISTS job_index_lucene ON job_index ()
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
   'refresh_seconds': '10',
   'schema': '{
      fields: {
         owner: {type: "string"},
         labels: {type: "text", analyzer: "english"},
         config:{type: "text", analyzer: "english"},
         creation_time: {type: "date", pattern: "yyyy/MM/dd"},
         respool_id: {type: "string"},
         state: {type: "string"},
         update_time: {type: "date", pattern: "yyyy/MM/dd"},
         start_time: {type:"date", pattern: "yyyy/MM/dd"},
         completion_time: {type: "date", pattern: "yyyy/MM/dd"}
      }
   }'
};

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
