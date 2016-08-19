echo "Draining Cassandra"
nodetool drain
echo "Stopping Cassandra"
sudo /etc/init.d/cassandra stop
echo "Deleting previous state and commitlogs"
sudo rm -rf /raid0/cassandra/commitlog/*
echo "Starting Cassandra"
sudo /etc/init.d/cassandra start
echo "Sleeping for 2 mins..."
sleep 120
echo "Sleep over, enabling thrift"
nodetool enablethrift
