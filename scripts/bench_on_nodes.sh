sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

SERVERS="$sbin/hosts"
DATASET=$1
shift
NUMTHREADS=$1
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

if [ -f "$SERVERS" ]; then
  SERVERLIST=`cat "$SERVERS"`
else
  echo "SERVERS file $SERVERS does not exist."
  exit -1
fi

# By default disable strict SERVER key checking
if [ "$SSH_OPTS" = "" ]; then
  SSH_OPTS="-o StrictHostKeyChecking=no"
fi

i=1
for SERVER in `echo "$SERVERLIST"|sed  "s/#.*$//;/^$/d"`; do
  echo "Launching benchmark on $SERVER..."
  ssh $SSH_OPTS "$SERVER" $sbin/bench_titan_node.sh $i $DATASET $NUMTHREADS $QOPTS 2>&1 | sed "s/^/$SERVER: /" &
  i=$(($i + 1))
done
wait

i=1
mkdir -p $sbin/../nt${NUMTHREADS}
for SERVER in `echo "$SERVERLIST"|sed  "s/#.*$//;/^$/d"`; do
  scp $SSH_OPTS "$SERVER:$sbin/../titan.t${NUMTHREADS}.q${query_type}.log" $sbin/../nt${NUMTHREADS}/titan.n${i}.q${query_type}.log 2>&1 | sed "s/^/$SERVER: /"
  ssh $SSH_OPTS "$SERVER" rm $sbin/../titan.t${NUMTHREADS}.q${query_type}.log 2>&1 | sed "s/^/$SERVER: /"
  i=$(($i + 1))
done
