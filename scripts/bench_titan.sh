#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

sudo /etc/init.d/cassandra stop

for num_threads in 1 2 4 8 16 32 64 128 256; do
  sudo cp -r /raid0/cassandra/data/medium /raid0/cassandra/data/test
  sudo /etc/init.d/cassandra start
  sleep 120
  nodetool enablethrift
  $sbin/../bin/linkbench -c $sbin/../config/LinkConfigTitan.properties -r -L $sbin/../titan.t${num_threads}.log -Drequesters=${num_threads}
  nodetool drain
  sudo /etc/init.d/cassandra stop
  sudo rm -rf /raid0/cassandra/commitlog/* /raid0/cassandra/data/test
done
