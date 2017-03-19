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

for tail_scheme in "read-stalled" "write-stalled"; do
  for query_type in ${query_types[@]}; do
    for num_threads in 1 2 4 8 16 32; do
      tail -n1 $sbin/$tail_scheme/gs.t${num_threads}.q${query_type}.log | awk -F' = ' '{ print $2 }' >> $sbin/$tail_scheme/$query_type.txt
    done
  done
  for num_threads in 1 2 4 8 16 32; do
    tail -n1 $sbin/$tail_scheme/gs.t${num_threads}.qfb.log | awk -F' = ' '{ print $2 }' >> $sbin/$tail_scheme/fb.txt
  done
done
