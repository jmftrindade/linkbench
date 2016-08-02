#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"
datapath=$1

for num_threads in 1 2 4 8 16 32 64 128 256; do
  cp -r $datapath ~/neo4j
  $sbin/../bin/linkbench -c $sbin/../config/LinkConfigNeo4j.properties -r -L $sbin/../neo4j.t${num_threads}.log -Drequesters=${num_threads}
  rm -r ~/neo4j
done
