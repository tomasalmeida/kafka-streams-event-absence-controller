package com.demo.streams.sessionwindow;

import com.demo.streams.BroadcastingPartitioner;
import com.demo.streams.DeviceAlert;
import com.demo.streams.common.PropertiesLoader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

public class AlertSystemGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlertSystemGenerator.class);

    private final Properties properties;
    private final Properties fileProperties;

    private AlertSystemGenerator() throws IOException {
        fileProperties = PropertiesLoader.load(PropertiesLoader.CONFIG.STREAMS);
        properties = streamsConfig();
    }

    public static void main(String[] args) throws Exception {

        final AlertSystemGenerator deviceAlert = new AlertSystemGenerator();
        deviceAlert.run();
    }

    public void run() {

        try (final KafkaStreams streams = new KafkaStreams(buildTopology(), properties)) {

            final CountDownLatch startLatch = new CountDownLatch(1);

            // Attach shutdown handler to catch Control-C.
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    LOGGER.info("Shutting down gracefully...");
                    try {
                        streams.close(Duration.ofSeconds(5));
                        startLatch.countDown();
                        LOGGER.info("KStreams closed successfully.");
                    } catch (Exception e) {
                        LOGGER.info("Error while closing KStreams", e);
                    }

                }
            });

            // Start the topology.
            streams.cleanUp();
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
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // disable caching to see session merging
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return config;
    }

    public Topology buildTopology() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final String heartbeatTopic = fileProperties.getProperty("heartbeat.topic.name");
        final String alertTopic = fileProperties.getProperty("alert.topic.name");

        final KStream<String, Long> heartbeatStream = streamsBuilder.stream(heartbeatTopic,
                Consumed.with(Serdes.String(), Serdes.Long()));

        // Create a session window with a 10-second inactivity gap
        heartbeatStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(0)))
                .count()
                .suppress(untilWindowCloses(unbounded()))
                .toStream()
                .peek((record, value) -> {
                    // Log the session window start and end times
                    LocalDateTime endTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(record.window().end()), ZoneId.systemDefault());
                    LOGGER.info("Session window for device [{}] last heartbeat at [{}]", record.key(), endTime);
                })
                .map((record, value) -> {
                    LocalDateTime firstTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(record.window().start()), ZoneId.systemDefault());
                    LocalDateTime lastTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(record.window().end()), ZoneId.systemDefault());
                    return new KeyValue<>(record.key(), "count = " + value + " from [" + firstTime + ", " + lastTime + "]");
                })
                .to(alertTopic, Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = streamsBuilder.build(properties);
        System.out.println(topology.describe());
        return topology;
    }
}
