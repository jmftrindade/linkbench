#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

query_types=(
addlink
deletelink
updatelink
countlink
getlink
getlinklist
getnode
addnode
updatenode
deletenode
)

for num_threads in 1 2 4 8 16 32; do
  tail -n1 $sbin/results/linkbench/gs.t${num_threads}.qfb.log | awk -F' = ' '{ print $2 }' >> $sbin/results/fb.txt
done
