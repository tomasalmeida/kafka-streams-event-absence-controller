## start

```
    docker-compose down -v
    docker-compose up -d
    mvn clean package
    kafka-topics --bootstrap-server localhost:29092 --topic deviceTopic --create
    kafka-topics --bootstrap-server localhost:29092 --topic heartbeatTopic --create
    kafka-topics --bootstrap-server localhost:29092 --topic alertTopic --create
    kafka-topics --bootstrap-server localhost:29092 --topic mainHeartbeatTopic --create
    kafka-topics --bootstrap-server localhost:29092 --list  
```

## device alert

```
    java -classpath target/event-absence-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.demo.streams.DeviceAlert 
```

## heartbeat producer

```
    java -classpath target/event-absence-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.demo.streams.HeartBeatProducer 
```

## device producer

```
    java -classpath target/event-absence-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.demo.streams.DeviceProducer 
```



## Data is there

1. Add devices
2. Add devices to hearbeat producer

```
    kafka-console-consumer --bootstrap-server localhost:29092 --from-beginning --topic deviceTopic --property print.key=true
    kafka-console-consumer --bootstrap-server localhost:29092 --from-beginning --topic heartbeatTopic --property print.key=true
    kafka-console-consumer --bootstrap-server localhost:29092 --from-beginning --topic alertTopic --property print.key=true
```


https://www.confluent.io/blog/data-enrichment-with-kafka-streams-foreign-key-joins/