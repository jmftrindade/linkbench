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

queries=(
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
  tail_scheme=$1

  # By default disable strict CLIENT key checking
  if [ "$SSH_OPTS" = "" ]; then
    SSH_OPTS="-o StrictHostKeyChecking=no"
  fi

  ssh $SSH_OPTS "$server" $HOME/monolog/sbin/stop_gs.sh 2>&1 | sed "s/^/$server: /"
  sleep 5
  ssh $SSH_OPTS "$server" $HOME/monolog/sbin/start_gs.sh $tail_scheme 2>&1 | sed "s/^/$server: /"
}

for tail_scheme in "read-stalled" "write-stalled"; do
  for query_type in ${queries[@]}; do
    QOPTS=`qopts $query_type`
    for num_threads in 32; do
      echo "Starting GS"
      start_gs $tail_scheme
      sleep 1
      echo "Benchmarking query_type=$query_type with num_threads=$num_threads"
      $sbin/bench_gs_node.sh $server $num_threads $tail_scheme $QOPTS
    done
  done

  # Linkbench wokload
  for num_threads in 64 128; do
    echo "Starting GS"
    start_gs $tail_scheme
    sleep 1
    echo "Benchmarking linkbench workload with num_threads=$num_threads"
    $sbin/bench_gs_node.sh $server $num_threads $tail_scheme
  done
done
