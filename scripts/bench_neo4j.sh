#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

server=$1
shift

QOPTS=""
if [ "$#" = "0" ]; then
  query_type="fb"
elif [ "$#" != "10" ]; then
  echo "Must provide exactly 10 query opts: provided $#"
  exit 0
else
  for opt in $@; do
    perc=$(echo $opt | sed 's/\-D\(.*\)=\(.*\)/\2/')
    qtyp=$(echo $opt | sed 's/\-D\(.*\)=\(.*\)/\1/')
    if [ "$perc" = "100" ]; then
      query_type=$qtyp
    fi
    QOPTS="$QOPTS $opt"
  done
fi

echo "Query opts=\'$QOPTS\'"
echo "Query type=\'$query_type\'"

function load() {
  host=$1
  ssh $host $sbin/load_data.sh
}

conf=$sbin/../config/LinkConfigNeo4j.properties
for num_threads in 1 2 4 8 16 32; do
  load $server
  output=$sbin/../results/neo4j.t${num_threads}.q${query_type}.txt
  $sbin/../bin/linkbench -c $conf -r -L $output -Drequesters=${num_threads} -Dserver=$server $QOPTS
done
