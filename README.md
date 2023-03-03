# kafka-streams-event-absence-controller

* deviceTopic: the activated devices are sent to this topic each message is <deviceId, "active">, a tombstone is sent if the device is deactivated
* heartbeatTopic: each device sends a heartbeat to this topic. Message format <deviceId, time in milliseconds>
* alertTopic: topic where the alerts about disconnected active devices are sent. The message is resent until the device ir reconnected or deactivated.
* mainHeartbeatTopic: technical topic where the heartbeat control message is sent. Each time a message is sent here it is broadcasted to all partitions in 
broadcastMainHeartbeatTopic.
* broadcastMainHeartbeatTopic: it has the same number of partitions than deviceTopic (needed to trigger the join.

A device should send 1 heartbeat each 10 seconds and in case it sends less than 3 heartbeats in 30 seconds, an alert is sent.

## start

```
    docker-compose down -v
    docker-compose up -d
    mvn clean package
    kafka-topics --bootstrap-server localhost:29092 --topic deviceTopic --create --partitions 3
    kafka-topics --bootstrap-server localhost:29092 --topic heartbeatTopic --create --partitions 3
    kafka-topics --bootstrap-server localhost:29092 --topic alertTopic --create --partitions 3
    kafka-topics --bootstrap-server localhost:29092 --topic broadcastMainHeartbeatTopic --create --partitions 3
    kafka-topics --bootstrap-server localhost:29092 --topic mainHeartbeatTopic --create --partitions 1
    kafka-topics --bootstrap-server localhost:29092 --list  
```

## device alert (1st shell)

```
    java -classpath target/event-absence-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.demo.streams.DeviceAlert 
```

## heartbeat producer (2nd shell)

```
    java -classpath target/event-absence-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.demo.streams.HeartBeatProducer 
```

## device producer (3rd shell)

```
    java -classpath target/event-absence-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.demo.streams.DeviceProducer 
```

## Data is there (4th shell)

1. Add devices
2. Add devices to hearbeat producer

```
    kafka-console-consumer --bootstrap-server localhost:29092 --from-beginning --property print.key=true --topic broadcastMainHeartbeatTopic --partition 0
    kafka-console-consumer --bootstrap-server localhost:29092 --from-beginning --property print.key=true --topic broadcastMainHeartbeatTopic --partition 1
    kafka-console-consumer --bootstrap-server localhost:29092 --from-beginning --property print.key=true --topic broadcastMainHeartbeatTopic --partition 2
    kafka-console-consumer --bootstrap-server localhost:29092 --from-beginning --property print.key=true --topic deviceTopic 
    kafka-console-consumer --bootstrap-server localhost:29092 --from-beginning --property print.key=true --topic heartbeatTopic 
    kafka-console-consumer --bootstrap-server localhost:29092 --from-beginning --property print.key=true --topic alertTopic
```

References:
- https://www.confluent.io/blog/data-enrichment-with-kafka-streams-foreign-key-joins/
- https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=211883356
- Thanks [@sotojuan2](https://github.com/sotojuan2/) and [@LGouellec](https://github.com/LGouellec) for the support
