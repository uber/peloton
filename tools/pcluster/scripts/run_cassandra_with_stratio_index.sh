#!/bin/bash
set -euxo pipefail

# 1. install curl
apt-get update && apt-get install --yes curl

# 2. Download the cassandra-lucene-index-plugin jar into cassandra lib directory
cd /usr/share/cassandra/lib
curl -LO http://search.maven.org/remotecontent?filepath=com/stratio/cassandra/cassandra-lucene-index-plugin/3.9.0/cassandra-lucene-index-plugin-3.9.0.jar

# 3. Start c*
/docker-entrypoint.sh cassandra -f
