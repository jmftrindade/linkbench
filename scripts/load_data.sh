#!/usr/bin/env bash

# NOTE: needs to be run as superuser.

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

# NOTE: /mnt/tmpfs/neo4j_data needs to be mounted first, obvs.
db_path="/mnt/tmpfs/neo4j_data"
src_path="/tmp/neo4j_data"

function refresh() {
  echo "Cleaning previous run..."
  rm -rf $db_path
  echo "Creating fresh copy..."
  cp -r $src_path $db_path
}

systemctl stop neo4j
refresh
systemctl start neo4j
sleep 10
