package com.example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerApplication {

    public static void main(String[] args) {

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
        kafkaProducer.send(record);

        // flushing producer
        kafkaProducer.flush();
        kafkaProducer.close();
    }

}
