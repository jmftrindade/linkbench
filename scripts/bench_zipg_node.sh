#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

node_id=$1
servername=$2
dataset=$3
num_threads=$4

if [ "$dataset" = "small" ]; then
  numnodes=32290001
elif [ "$dataset" = "medium" ]; then
  numnodes=403625000
elif [ "$dataset" = "large" ]; then
  numnodes=1026822000
else
  echo "Invalid dataset $dataset"
  exit -1
fi

node_max_id=$(($numnodes + $node_id * 10000000 + 1))

echo "Executing benchmark on node $node_id with $num_threads threads for the $dataset dataset [node_max_id=$node_max_id]"
$sbin/../bin/linkbench -c $sbin/../config/LinkConfigSuccinct.properties -r -L $sbin/../zipg.t${num_threads}.log -Drequesters=${num_threads} -Dhostname=$servername -Dnodeidoffset=$node_max_id
