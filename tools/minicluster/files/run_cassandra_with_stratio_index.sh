#!/bin/bash
set -euxo pipefail

# 1. cp mounted cassandra-lucene-index-plugin jar into cassandra lib directory
cp /files/cassandra-lucene-index-plugin-3.0.14.0.jar /usr/share/cassandra/lib

# 2. Start c*
/docker-entrypoint.sh cassandra -f
