#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

server=${1:-"localhost"}

echo "Server=$server"

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

read_only=(
countlink
getlink
getlinklist
getnode
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

function start_gs() {
  # By default disable strict CLIENT key checking
  if [ "$SSH_OPTS" = "" ]; then
    SSH_OPTS="-o StrictHostKeyChecking=no"
  fi

  ssh $SSH_OPTS "$server" $HOME/monolog/sbin/stop_gs.sh 2>&1 | sed "s/^/$server: /"
  ssh $SSH_OPTS "$server" $HOME/monolog/sbin/start_gs.sh 2>&1 | sed "s/^/$server: /"
  ssh $SSH_OPTS "$server" $HOME/linkbench/scripts/load.sh 2>&1 | sed "s/^/$server: /"
}

for query_type in ${query_types[@]}; do
  QOPTS=`qopts $query_type`
  for num_threads in 1 2 4 8 16 32; do
    echo "Benchmarking query_type=$query_type with num_threads=$num_threads"
    start_gs
    echo "Started GS"
    sleep 1
    $sbin/bench_gs_node.sh $server $num_threads $QOPTS
  done
done
