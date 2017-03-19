#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

servername=$1
shift
num_threads=$1
shift
tail_scheme=$1
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

echo "Executing benchmark on node $servername with $num_threads threads"
ssh $SSH_OPTS "$servername" $HOME/linkbench/scripts/load.sh $num_threads $query_type $tail_scheme 2>&1 | sed "s/^/$servername: /"
mkdir -p $sbin/../$tail_scheme
cmd="$sbin/../bin/linkbench -c $sbin/../config/LinkConfigMonolog.properties -r -L $sbin/../$tail_scheme/gs.t${num_threads}.q${query_type}.log -Drequesters=${num_threads} -Dhostname=$servername $QOPTS"
echo $cmd
$cmd
