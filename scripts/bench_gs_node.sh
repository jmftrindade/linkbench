#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

servername=$1
shift
num_threads=$1
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

echo "Executing benchmark on node $node_id with $num_threads threads"
cmd="$sbin/../bin/linkbench -c $sbin/../config/LinkConfigMonolog.properties -l -r -L $sbin/../gs.t${num_threads}.q${query_type}.log -Drequesters=${num_threads} -Dhostname=$servername $QOPTS"
echo $cmd
$cmd
