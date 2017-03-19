#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

$sbin/../bin/linkbench -c $sbin/../config/LinkConfigMonolog.properties -l -L $sbin/../gs.t${num_threads}.q${query_type}.log
