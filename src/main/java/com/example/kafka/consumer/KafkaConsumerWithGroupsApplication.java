package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerWithGroupsApplication {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(KafkaConsumerWithGroupsApplication.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "foo-group-id-2";
        String topic = "foo";

        // creating consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // creating consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // subscribing consumer to topic
        kafkaConsumer.subscribe(Collections.singleton(topic));

        // polling for new data
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord record : records) {
                logger.info("Key: " + record.key());
                logger.info("Value: " + record.value());
                logger.info("Partition: " + record.partition());
                logger.info("Offset: " + record.offset());
            }
        }
    }
}
