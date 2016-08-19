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
CLIENT=`head -n 1 $CLIENTS`
SERVER=`head -n 1 $SERVERS`
echo "Launching client $CLIENT for server $SERVER with dataset $DATASET..."
ssh $SSH_OPTS "$CLIENT" $sbin/bench_zipg_node.sh $i $SERVER $DATASET $NUMTHREADS 2>&1 | sed "s/^/$CLIENT: /"
scp $SSH_OPTS "$CLIENT:$sbin/../zipg.t${NUMTHREADS}.log" $sbin/../nt${NUMTHREADS}/zipg.n${i}.log 2>&1 | sed "s/^/$CLIENT: /"
ssh $SSH_OPTS "$CLIENT" rm $sbin/../zipg.t${NUMTHREADS}.log 2>&1 | sed "s/^/$CLIENT: /"
