#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

server=${1:-"localhost"}

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

function qopts() {
  query=$1
  QOPTS_=""
  for query_type in ${query_types[@]}; do
    if [ "$query" = "$query_type" ]; then
      QOPTS_="$QOPTS_ -D$query_type=100"
    else
      QOPTS_="$QOPTS_ -D$query_type=0"
    fi
  done
  echo $QOPTS_
}

for query_type in ${query_types[@]}; do
  QOPTS=`qopts $query_type`
  $sbin/bench_neo4j.sh $server $QOPTS
done

$sbin/bench_neo4j.sh $server
