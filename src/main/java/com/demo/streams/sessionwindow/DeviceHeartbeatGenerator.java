package com.demo.streams.sessionwindow;

import com.demo.streams.common.PropertiesLoader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.*;
import java.util.stream.IntStream;

public class DeviceHeartbeatGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceHeartbeatGenerator.class);
    private final KafkaProducer<String, Long> heartbeatProducer;
    private final BufferedReader lineReader;
    private final String topic;

    private DeviceHeartbeatGenerator() throws IOException {
        Properties properties = PropertiesLoader.load(PropertiesLoader.CONFIG.HEARTBEAT);

        topic = properties.getProperty("input.topic.name");
        heartbeatProducer = new KafkaProducer<>(properties);

        lineReader = new BufferedReader(new InputStreamReader(System.in));

        // Add shutdown hook to close the producer
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down gracefully...");
            try {
                heartbeatProducer.close();
                LOGGER.info("Producer closed successfully.");
            } catch (Exception e) {
                LOGGER.info("Error while closing producer", e);
            }
        }));
    }

    public static void main(String[] args) throws Exception {
        final DeviceHeartbeatGenerator heartBeatProducer = new DeviceHeartbeatGenerator();
        heartBeatProducer.mainRun();
    }


    public void mainRun() throws IOException {
        System.out.print("Number of devices to start: ");
        int numberOfDevices = Integer.parseInt(lineReader.readLine().trim());

        List<String> devices = IntStream.range(0, numberOfDevices)
                        .mapToObj(deviceNumber -> "device-" + deviceNumber)
                        .sorted()
                        .toList();
        LOGGER.info("Devices to start: {}", devices);


        Random randomGenerator = new Random();
        while (true) {
            Long now = Calendar.getInstance().getTimeInMillis();
            for (String deviceName : devices) {
                if (randomGenerator.nextInt(100) > 40) { // 60% probability
                    heartbeatProducer.send(new ProducerRecord<>(topic, deviceName, now), this::logMessageSent);
                    LOGGER.info("Sent heartbeat for device [{}] at time [{}]", deviceName, now);
                }
            }
            waitUntilDurationExpires();
        }
    }

    private void waitUntilDurationExpires() {
        try {
            Thread.sleep(Duration.ofSeconds(5).toMillis());
        } catch (final InterruptedException e) {
            LOGGER.info("Ops, sleep was interrupted!", e);
        }
    }

    private void logMessageSent(final RecordMetadata metadata, final Exception exception) {
        if (exception != null) {
            LOGGER.info("Error sending message to topic [{}]", metadata.topic(), exception);
        } else {
            LOGGER.debug("Message acknowledged to topic [{}]", metadata.topic());
        }
    }
}

