#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"
dataset=$1

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

db_path="/mnt2/neo4j/$dataset"
page_cache=`du ${db_path}/*store.db* | awk '{ x += $1 } END { printf("%.0f\n", x*1.2/(1024*1024)) }'`
pc=$(($page_cache>230?230:$page_cache))g

function cache() {
  echo "Caching data at $db_path"
  find $db_path/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
  echo "Caching complete"
}

cache

echo "pc=$pc"
for num_threads in 1 16; do
  for tuned in "true" "false"; do
    $sbin/../bin/linkbench -c $sbin/../config/LinkConfigNeo4j.properties -r -L $sbin/../neo4j.t${num_threads}.p${pc}.q${query_type}.o${tuned}.log -Drequesters=${num_threads} -Dpage_cache_size=$pc -Ddb_path=$db_path -Dtuned=$tuned $QOPTS
  done
done
