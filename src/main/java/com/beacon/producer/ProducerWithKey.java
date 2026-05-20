package com.beacon.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKey {

    public static void main(String[] args) {

        final String BOOTSTRAP_SERVERS = "localhost:9092";
        // final String BOOTSTRAP_SERVERS = "broker-1:29092";

        Logger logger = LoggerFactory.getLogger(ProducerWithKey.class);

        final String TOPIC_NAME = "beacon_transactions_topics";

        // Setting the properties for the producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class.getName());

        // Array of Philippine cities/areas as keys
        // Key = area → all transactions from same area go to same partition
        String[] philippineAreas = {
                "BGC Taguig", "Makati CBD", "Ortigas Pasig", "Quezon City",
                "Alabang Muntinlupa", "Mandaluyong", "Pasay", "Cebu City",
                "Davao City", "Clark Pampanga", "Iloilo City", "Bacolod",
                "Cagayan de Oro", "Zamboanga City", "General Santos",
                "San Juan Metro Manila", "Paranaque", "Las Pinas",
                "Caloocan", "Valenzuela", "Marikina", "Pasig City",
                "Muntinlupa City", "Taguig City", "Pateros",
                "Antipolo Rizal", "Cainta Rizal", "Taytay Rizal"
        };

        // Create Kafka Producer Object
        KafkaProducer<String, Double> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 25000; i++) {

            // Key = Philippine area name
            // Same area always goes to the same partition (ordering per location)
            String key = philippineAreas[(int) (Math.random() * philippineAreas.length)];

            // Value = property transaction amount in PHP (Philippine Peso)
            // Range: PHP 1,000,000 to PHP 10,000,000
            double value = Math.floor(Math.random() * (10_000_000 - 1_000_000 + 1) + 1_000_000);

            // Create a ProducerRecord with the key and value
            ProducerRecord<String, Double> record = new ProducerRecord<>(TOPIC_NAME, key, value);

            logger.info("Sending transaction from area: " + key + " to Kafka");

            // Send the record
            producer.send(record, ((metadata, exception) -> {
                if (metadata != null) {
                    logger.info("-----------------------------------");
                    logger.info("Area (Key)  : " + record.key());
                    logger.info("Amount (PHP): " + record.value());
                    logger.info("Metadata    : " + metadata.toString());
                    logger.info("Topic       : " + metadata.topic());
                    logger.info("Partition   : " + metadata.partition());
                    logger.info("Offset      : " + metadata.offset());
                    logger.info("Timestamp   : " + metadata.timestamp());
                } else if (exception != null) {
                    logger.error("Error sending message to Kafka", exception);
                }
            }));

            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }

            producer.flush();
        }
    }
}