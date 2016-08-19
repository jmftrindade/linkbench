sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

CLIENTS="$sbin/zipg_clients"
SERVERS="$sbin/zipg_servers"
DATASET=$1
NUMTHREADS=$2

if [ -f "$CLIENTS" ]; then
  CLIENTLIST=`cat "$CLIENTS"`
else
  echo "CLIENTS file $CLIENTS does not exist."
  exit -1
fi

# By default disable strict CLIENT key checking
if [ "$SSH_OPTS" = "" ]; then
  SSH_OPTS="-o StrictHostKeyChecking=no"
fi

i=1
for CLIENT in `echo "$CLIENTLIST"|sed  "s/#.*$//;/^$/d"`; do
  SERVER=$(sed -n "$i{p;q;}" $SERVERS)
  echo "Launching client $CLIENT for server $SERVER..."
  ssh $SSH_OPTS "$CLIENT" $sbin/bench_zipg_node.sh $i $SERVER $DATASET $NUMTHREADS 2>&1 | sed "s/^/$CLIENT: /" &
  i=$(($i + 1))
done
wait

i=1
mkdir -p $sbin/../nt${NUMTHREADS}
for CLIENT in `echo "$CLIENTLIST"|sed  "s/#.*$//;/^$/d"`; do
  scp $SSH_OPTS "$CLIENT:$sbin/../zipg.t${NUMTHREADS}.log" $sbin/../nt${NUMTHREADS}/zipg.n${i}.log 2>&1 | sed "s/^/$CLIENT: /"
  ssh $SSH_OPTS "$CLIENT" rm $sbin/../zipg.t${NUMTHREADS}.log 2>&1 | sed "s/^/$CLIENT: /"
  i=$(($i + 1))
done
