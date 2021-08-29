package com.example.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerWithCallbackApplication {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(KafkaProducerWithCallbackApplication.class);

        String bootstrapServers = "127.0.0.1:9092";

        // creating producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // creating producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // creating producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("foo", "I'm the first topic.");

        // sending data - async
        kafkaProducer.send(record, (recordMetadata, e) -> {
            if (e == null) {
                logger.info("Received new metadata. \n" +
                        "Topic: " + recordMetadata.topic() + "\n" +
                        "Partition: " + recordMetadata.partition() + "\n" +
                        "Offset: " + recordMetadata.offset() + "\n" +
                        "Timestamp: " + recordMetadata.timestamp());
            } else
                logger.error("Error while producing: " + e);
        });

        // flushing producer
        kafkaProducer.flush();
        kafkaProducer.close();
    }

}
