/*
  create lucene index in cassandra store for jobs table
 */
CREATE CUSTOM INDEX jobs_index ON jobs ()
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
   'refresh_seconds': '10',
   'schema': '{
      fields: {
         owner: {type: "string"},
         labels: {type: "text", analyzer: "english"},
         jobconfig:{type: "text", analyzer: "english"},
         createtime: {type: "date", pattern: "yyyy/MM/dd"}
      }
   }'
};
