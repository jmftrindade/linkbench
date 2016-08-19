sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

SERVERS="$sbin/hosts"

cmd=$1

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

for SERVER in `echo "$SERVERLIST"|sed  "s/#.*$//;/^$/d"`; do
  ssh $SSH_OPTS "$SERVER" $cmd 2>&1 | sed "s/^/$SERVER: /" &
done
wait
