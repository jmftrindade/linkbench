#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

db_path="/mnt/ram/neo4j"
src_path="/home/ec2-user/neo4j"

function refresh() {
  echo "Cleaning previous run..."
  rm -rf $db_path
  echo "Creating fresh copy..."
  cp -r $src_path $db_path
}

$HOME/neo4j-community-3.1.2/bin/neo4j stop
refresh
$HOME/neo4j-community-3.1.2/bin/neo4j start
