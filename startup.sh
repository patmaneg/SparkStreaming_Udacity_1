# Arraqque servidor
echo "Arrancando zookeeper y kafka"
/usr/bin/zookeeper-server-start /etc/kafka/zookeeper.properties &
/usr/bin/kafka-server-start /etc/kafka/server.properties &


