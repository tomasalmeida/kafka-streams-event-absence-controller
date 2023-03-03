package com.demo.streams;

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
import java.util.Calendar;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static java.lang.System.exit;

public class HeartBeatProducer extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeartBeatProducer.class.getSimpleName());
    private final KafkaProducer<String, Long> heartbeatProducer;
    private final KafkaProducer<String, Long> mainHeartbeatProducer;
    private final BufferedReader lineReader;
    private final String topic;
    private final Set<String> devices;

    private HeartBeatProducer() throws IOException {
        Properties properties = PropertiesLoader.load(PropertiesLoader.CONFIG.HEARTBEAT);

        topic = properties.getProperty("input.topic.name");
        heartbeatProducer = new KafkaProducer<>(properties);

        properties.put("input.topic.name", "mainHeartbeatTopic");
        mainHeartbeatProducer = new KafkaProducer<>(properties);

        lineReader = new BufferedReader(new InputStreamReader(System.in));
        devices = new HashSet<>();
    }

    public static void main(String[] args) throws Exception {
        HeartBeatProducer heartBeatProducer = new HeartBeatProducer();
        heartBeatProducer.mainRun();
    }


    public void mainRun() throws IOException {
        this.start();
        while (true) {
            System.out.print("[a]dd device or [d]elete device or [c]lose app: ");
            String action = lineReader.readLine();

            if("c".equals(action)){
                heartbeatProducer.close();
                exit(0);
            }

            System.out.print("Add the device name: ");
            String deviceName = lineReader.readLine();
            synchronized (devices) {
                if ("d".equals(action)) {
                    devices.remove(deviceName);
                    LOGGER.info("Deactivating device %s", deviceName);
                } else {
                    devices.add(deviceName);
                    LOGGER.info("Activating device %s", deviceName);
                }
            }
        }
    }

    @Override
    public void run() {
        ProducerRecord<String, Long> producerRecord;
        while (true) {
            synchronized (devices) {
                Long now = Calendar.getInstance().getTimeInMillis();
                for (String deviceName : devices) {
                    heartbeatProducer.send(new ProducerRecord<>(topic, deviceName, now), this::logMessageSent);
                }
                mainHeartbeatProducer.send(new ProducerRecord<>("mainHeartbeatTopic", "active", now));
            }
            waitUntilDurationExpires();
        }
    }

    private void waitUntilDurationExpires() {
        try {
            Thread.sleep(Duration.ofSeconds(10).toMillis());
        } catch (final InterruptedException e) {
            LOGGER.error("Ops, sleep was interrupted!", e);
        }
    }

    private void logMessageSent(final RecordMetadata metadata, final Exception exception) {
        if (exception != null) {
            LOGGER.error("Error sending message to topic [{}]", metadata.topic(), exception);
        } else {
            LOGGER.debug("Message acknowledged to topic [{}]", metadata.topic());
        }
    }
}
 