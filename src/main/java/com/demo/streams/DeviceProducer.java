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
import java.util.Properties;

import static java.lang.System.exit;

public class DeviceProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceProducer.class.getSimpleName());
    private final KafkaProducer<String, String> producer;
    private final BufferedReader lineReader;
    private final String topic;

    private DeviceProducer() throws IOException {
        Properties properties = PropertiesLoader.load(PropertiesLoader.CONFIG.DEVICE);
        topic = properties.getProperty("input.topic.name");
        producer = new KafkaProducer<>(properties);
        lineReader = new BufferedReader(new InputStreamReader(System.in));
    }

    public static void main(String[] args) throws Exception {
        DeviceProducer deviceProducer = new DeviceProducer();
        deviceProducer.run();
    }

    private void run() throws Exception {
        ProducerRecord<String, String> producerRecord;

        while (true) {

            System.out.print("[a]dd device or [d]elete device or [c]lose app: ");
            String action = lineReader.readLine();

            if("c".equals(action)){
                producer.close();
                exit(0);
            }

            System.out.print("Add the device name: ");
            String deviceName = lineReader.readLine();
            if ("d".equals(action)) {
                producerRecord = new ProducerRecord<>(topic, deviceName, null);
                LOGGER.info("Deactivating device {}", deviceName);
            } else {
                producerRecord = new ProducerRecord<>(topic, deviceName, "active");
                LOGGER.info("Activating device {}", deviceName);
            }
            producer.send(producerRecord, this::logMessageSent);
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
 