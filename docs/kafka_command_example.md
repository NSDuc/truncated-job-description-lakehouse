# Kafka Command Example
> or use Kafka unittest

### Start Kafka
```shell
cd /opt/kafka_2.13-3.1.1/ && ./autostart.sh
```

### Get Topic info
```shell
TOPIC="job_descriptions"
/opt/kafka_2.13-3.1.1/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic $TOPIC 
```

### Stop Kafka
```shell
cd /opt/kafka_2.13-3.1.1/ && ./autostop.sh
```
