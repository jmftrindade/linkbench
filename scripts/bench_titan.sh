#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

function setup() {
  $sbin/run_on_nodes.sh $sbin/setup.sh
}

echo "Copying benchmark directory"
$sbin/copy-dir $sbin/../

dataset=$1

for num_threads in 64; do
  setup $dataset
  echo "Setup complete"
  sleep 1
  $sbin/bench_on_nodes.sh $dataset $num_threads
done
