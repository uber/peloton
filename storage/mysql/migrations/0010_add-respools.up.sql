/*
  respools table persists resource pools information for peloton
 */
CREATE TABLE respools (
  `added_id`        int(11) NOT NULL AUTO_INCREMENT,
  `row_key`         VARCHAR(64) NOT NULL PRIMARY KEY,
  `col_key`         VARCHAR(64) NOT NULL,
  `ref_key`         INTEGER NOT NULL,
  `body`            JSON NOT NULL,
  `created_by`      VARCHAR(64) NOT NULL ,
  `create_time`     DATETIME DEFAULT CURRENT_TIMESTAMP,
  `update_time`     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `owning_team`     VARCHAR(64) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(body, '$.owningTeam'))),
  `flags`           INTEGER DEFAULT 0,
   KEY `added_id` (`added_id`)
);
