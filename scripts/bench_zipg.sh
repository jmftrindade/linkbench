#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"
dataset=$1

for num_threads in 1 2 4 6 16; do
  /home/ec2-user/succinct-graph/sbin/setup.sh $dataset
  $sbin/../bin/linkbench -c $sbin/../config/LinkConfigSuccinct.properties -r -L $sbin/../zipg.t${num_threads}.log -Drequesters=${num_threads}
done
