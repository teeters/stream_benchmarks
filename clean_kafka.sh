/home/kafka/Downloads/kafka_2.11-1.0.0/bin/kafka-configs.sh --zookeeper localhost:2181 --entity-type topics --alter --add-config retention.ms=1000 --entity-name roundtrip_benchmark
echo "Wait one minute..."
sleep 1m
/home/kafka/Downloads/kafka_2.11-1.0.0/bin/kafka-configs.sh --zookeeper localhost:2181 --entity-type topics --alter --delete-config retention.ms --entity-name roundtrip_benchmark
