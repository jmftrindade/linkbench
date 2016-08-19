echo "Draining Cassandra"
nodetool drain
echo "Stopping Cassandra"
sudo /etc/init.d/cassandra stop
echo "Deleting previous state and commitlogs"
sudo rm -rf /raid0/cassandra/commitlog/*
echo "Killing benchmark"
pkill -U ubuntu java
