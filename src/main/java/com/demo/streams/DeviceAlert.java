package com.demo.streams;

import com.demo.streams.common.PropertiesLoader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

public class DeviceAlert {

    private final Properties properties;
    private final Properties fileProperties;

    private DeviceAlert() throws IOException {
        fileProperties = PropertiesLoader.load(PropertiesLoader.CONFIG.STREAMS);
        properties = streamsConfig();
    }

    public static void main(String[] args) throws Exception {

        final DeviceAlert deviceAlert = new DeviceAlert();
        deviceAlert.run();
    }

    public void run() {

        try (final KafkaStreams streams = new KafkaStreams(buildTopology(), properties)) {

            final CountDownLatch startLatch = new CountDownLatch(1);
            // Attach shutdown handler to catch Control-C.
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    streams.close(Duration.ofSeconds(5));
                    startLatch.countDown();
                }
            });

            // Start the topology.
            streams.start();

            try {
                startLatch.await();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                System.exit(1);
            }
        }
        System.exit(0);
    }

    private Properties streamsConfig() throws IOException {
        Properties fileProperties = PropertiesLoader.load(PropertiesLoader.CONFIG.STREAMS);

        final Properties config = new Properties();
        String client = fileProperties.getProperty("application.id") + "." + System.currentTimeMillis();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, client);
        config.put(StreamsConfig.CLIENT_ID_CONFIG, client);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, fileProperties.getProperty("bootstrap.servers"));
        config.put(StreamsConfig.STATE_DIR_CONFIG, fileProperties.getProperty("state.dir"));
        // Set to earliest so we don't miss any data
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // disable caching to see session merging
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        return config;
    }

    public Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String deviceTopic = fileProperties.getProperty("device.topic.name");
        final String heartbeatTopic = fileProperties.getProperty("heartbeat.topic.name");
        final String alertTopic = fileProperties.getProperty("alert.topic.name");
        final String windowTopic = "mainHeartbeatTopic";

        final KTable<String, String> deviceTable = builder.table(deviceTopic,
                Consumed.with(Serdes.String(), Serdes.String()));

        final KStream<String, Long> heartbeatStream = builder.stream(heartbeatTopic,
                Consumed.with(Serdes.String(), Serdes.Long()));

        final KTable<String, Long> mainHeartbeatTable = builder.table(windowTopic,
                Consumed.with(Serdes.String(), Serdes.Long()));

//        // codigo bueno bueno
////        KTable<String, String> heartbeatsDevicesCountTable =
//        heartbeatStream
//                .groupByKey()
//                .windowedBy(TimeWindows.of(Duration.ofSeconds(30)).grace(Duration.ofSeconds(5)))
//                .count()
//                .suppress(untilWindowCloses(unbounded()))
//                .toStream()
//                .filter(((deviceId, heartbeatsCount) -> heartbeatsCount != null && heartbeatsCount >= 3))
//                .map((windowKey, value) -> KeyValue.pair(windowKey.key(), windowKey.window().endTime().toString()))
//                .peek((key, lastGoodCount) -> System.out.println("Last time 3 heartbeats are sent was " + lastGoodCount + " from " + key))
////                .toTable(Named.as("heartbeatsDevicesCountTable"), Materialized.with(Serdes.String(), Serdes.String()))
//                .to(alertTopic, Produced.with(Serdes.String(), Serdes.String()))
//        ;
//
//        // SELECT * from deviceTable left join heartbeatsDevicesCountTable
//        // problem is no events on device side needs to be send to generate a <deviceId, null>
////        deviceTable
////                .leftJoin(heartbeatsDevicesCountTable, (status, count) -> count)
////                .toStream(Named.as("device-heartbeat-leftjoin-stream"))
//////                .filter(((deviceId, heartbeatsCount) -> heartbeatsCount == null || heartbeatsCount < 3))
////                .peek((key, value) -> System.out.println("Sad to say device " + key + " just send this number of heartbeats: " + value))
////                .mapValues(heartbeatCount -> "Number of received heartbeats from this device is " + heartbeatCount)
////                .to(alertTopic, Produced.with(Serdes.String(), Serdes.String()))
//////                ;


        KTable<String, Long> lastGoodDevicesWindowTable =
                heartbeatStream
                        .groupByKey()
                        .windowedBy(TimeWindows.of(Duration.ofSeconds(30)).grace(Duration.ofSeconds(5)))
                        .count()
                        .suppress(untilWindowCloses(unbounded()))
                        .filter(((deviceId, heartbeatsCount) -> heartbeatsCount != null && heartbeatsCount >= 3))
                        .toStream()
                        .map((windowKey, value) -> KeyValue.pair(windowKey.key(), windowKey.window().endTime().toEpochMilli()))
                        .toTable(Named.as("lastGoodDevicesWindowTable"), Materialized.with(Serdes.String(), Serdes.Long()));

        deviceTable
                .leftJoin(mainHeartbeatTable,
                        deviceValue -> deviceValue,
                        (left, right) -> right,
                        Named.as("device-mainheartbeat-join")
                )
                .leftJoin(lastGoodDevicesWindowTable,
                        (left, right) -> right,
                        Named.as("lastgoodwindow-join"))
                .toStream(Named.as("device-heartbeat-leftjoin-stream"))
                .filter(((deviceId, lastGoodWindow) -> isLastGoodWindowTooOld(lastGoodWindow)))
                .mapValues(this::generateAlertEvent)
                .peek(this::printEvent)
                .to(alertTopic, Produced.with(Serdes.String(), Serdes.String()))
        ;


        Topology topology = builder.build(properties);
        System.out.println(topology.describe());
        return topology;
    }

    private boolean isLastGoodWindowTooOld(Long lastGoodWindow) {
        return lastGoodWindow == null ||
                Instant.now().minusSeconds(30).isAfter(Instant.ofEpochMilli(lastGoodWindow));
    }

    private void printEvent(Object key, Object value) {
        System.out.println("key " + key + ", value " + value);
    }

    private String generateAlertEvent(Object key, Long value) {
        String lastGoodWindow = value != null ? Instant.ofEpochMilli(value).toString() : "never";
        return "Alert: [" + LocalDateTime.now() + "] key " + key + ", last Good Window: " + lastGoodWindow;
    }
}
