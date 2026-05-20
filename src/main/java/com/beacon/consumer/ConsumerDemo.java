package com.beacon.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        String BOOTSTRAP_SERVERS = "localhost:9092";
        // String BOOTSTRAP_SERVERS = "broker-1:29092";
        String TOPIC_NAME = "first_topic";
        String GROUP_ID = "beacon-analytics-consumer-group";

        Logger logger= LoggerFactory.getLogger(ConsumerDemo.class);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        //Create the Kafka Consumer Object
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Subscribe to the topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            //Process the Records
            for(ConsumerRecord<String,String> record:records){
                logger.info("----------------------------------------------------------------");
                logger.info("Key: "+record.key());
                logger.info("Value: "+record.value());
                logger.info("Partition: "+record.partition());
                logger.info("Offset: "+record.offset());

            }
        }
    }
}
