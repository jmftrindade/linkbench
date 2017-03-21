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

for tail_scheme in "true" "false"; do
  for query_type in ${query_types[@]}; do
    for num_threads in 1 2 4 8 16 32; do
      tail -n1 $sbin/results/neo4j.t${num_threads}.p1g.q${query_type}.o${tail_scheme}.log | awk -F' = ' '{ print $2 }' >> $sbin/results/$query_type-o${tail_scheme}.txt
    done
  done
  for num_threads in 1 2 4 8 16 32; do
    tail -n1 $sbin/results/neo4j.t${num_threads}.p1g.qfb.o${tail_scheme}.log | awk -F' = ' '{ print $2 }' >> $sbin/results/$query_type-o${tail_scheme}.txt
  done
done
