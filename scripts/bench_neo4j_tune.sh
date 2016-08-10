#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"
datapath=$1

for num_threads in 1 2 4 8; do
  for pc in 1g 5g 10g 15g 20g; do
    cp -r $datapath ~/neo4j
    $sbin/../bin/linkbench -c $sbin/../config/LinkConfigNeo4j.properties -r -L $sbin/../neo4j.t${num_threads}.p${pc}.log -Drequesters=${num_threads} -Dpage_cache_size=$pc
    rm -r ~/neo4j
  done
done
