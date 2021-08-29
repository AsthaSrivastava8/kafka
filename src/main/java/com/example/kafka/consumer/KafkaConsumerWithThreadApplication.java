package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaConsumerWithThreadApplication {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(KafkaConsumerWithThreadApplication.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "foo-group-id-3";
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
        CountDownLatch latch = new CountDownLatch(1);

        Runnable runnable = new ConsumerThread(kafkaConsumer, topic, latch);
        Thread thread = new Thread(runnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ((ConsumerThread) runnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited.");
        }));
    }

    static class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> kafkaConsumer;
        private String topic;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());


        public ConsumerThread(KafkaConsumer<String, String> kafkaConsumer,
                              String topic,
                              CountDownLatch latch) {

            this.kafkaConsumer = kafkaConsumer;
            this.topic = topic;
            this.latch = latch;
        }

        @Override
        public void run() {
            kafkaConsumer.subscribe(Collections.singleton(topic));
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord record : records) {
                        logger.info("Key: " + record.key());
                        logger.info("Value: " + record.value());
                        logger.info("Partition: " + record.partition());
                        logger.info("Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.error("Received shutdown signal!");
            } finally {
                kafkaConsumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            kafkaConsumer.wakeup();
        }
    }
}


