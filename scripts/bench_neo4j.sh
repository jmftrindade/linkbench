#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

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

db_path="/mnt/ram/neo4j"
src_path="/home/ec2-user/neo4j"
page_cache=`du ${src_path}/*store.db* | awk '{ x += $1 } END { printf("%.0f\n", x*1.2/(1024*1024)) }'`
pc=$(($page_cache>230?230:$page_cache))g

function cache() {
  echo "Caching data at $db_path"
  find $db_path/ -name "*store.db*" -type f -exec dd if={} of=/dev/null bs=1M 2>/dev/null \;
  echo "Caching complete"
}

#cache

function refresh() {
  echo "Cleaning previous run..."
  rm -rf $db_path
  echo "Creating fresh copy..."
  cp -r $src_path $db_path
}

echo "pc=$pc"
for num_threads in 1 2 4 8 16 32; do
  for tuned in "true" "false"; do
    refresh
    $sbin/../bin/linkbench -c $sbin/../config/LinkConfigNeo4j.properties -r -L $sbin/../neo4j.t${num_threads}.p${pc}.q${query_type}.o${tuned}.log -Drequesters=${num_threads} -Dpage_cache_size=$pc -Ddb_path=$db_path -Dtuned=$tuned $QOPTS
  done
done
