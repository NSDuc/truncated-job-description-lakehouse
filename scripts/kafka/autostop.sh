set -e

bin/zookeeper-server-stop.sh config/zookeeper.properties &

bin/kafka-server-stop.sh config/server.properties &

sleep 10
