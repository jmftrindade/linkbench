#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

echo "Draining Cassandra"
nodetool drain
echo "Stopping Cassandra"
sudo /etc/init.d/cassandra stop
echo "Deleting previous state and commitlogs"
sudo rm -rf /raid0/cassandra/commitlog/* /raid0/cassandra/data/small

for num_threads in 1 2 4 8 16 32 64 128; do
  echo "Copying fresh state"
  sudo cp -r /raid0/cassandra/data/test /raid0/cassandra/data/small
  echo "Adding user cassanra as user for the new state"
  sudo chown cassandra:cassandra -R /raid0/cassandra/data/small
  echo "Starting Cassandra"
  sudo /etc/init.d/cassandra start
  echo "Sleeping"
  sleep 120
  echo "Enabling thrift"
  nodetool enablethrift
  echo "Starting benchmark"
  $sbin/../bin/linkbench -c $sbin/../config/LinkConfigTitan.properties -r -L $sbin/../titan.t${num_threads}.log -Drequesters=${num_threads}
  echo "Benchmark finished; draining Cassandra"
  nodetool drain
  echo "Stopping Cassandra"
  sudo /etc/init.d/cassandra stop
  echo "Removing previous state and commitlogs"
  sudo rm -rf /raid0/cassandra/commitlog/* /raid0/cassandra/data/small
done
