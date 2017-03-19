#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

server=${1:-"localhost"}

function setup() {
  # By default disable strict CLIENT key checking
  if [ "$SSH_OPTS" = "" ]; then
    SSH_OPTS="-o StrictHostKeyChecking=no"
  fi

  ssh $SSH_OPTS "$server" $HOME/monolog/sbin/stop_gs.sh 2>&1 | sed "s/^/$server: /"
  sleep 5
  ssh $SSH_OPTS "$server" $HOME/monolog/sbin/start_gs.sh 2>&1 | sed "s/^/$server: /"
}

for num_threads in 1 2 4 8 16 32; do
  setup
  $sbin/bench_gs_node.sh $server $num_threads
done
