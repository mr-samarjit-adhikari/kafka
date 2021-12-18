package com.kafka.beginners.course;

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

/**
 * By running multiple instances of ConsumerDemoGroups , Re-balancing is being observed.
 */
public class ConsumerDemoGroups {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);
    private static final String TOPIC = "first_topic";
    private static final String GROUP_ID = "my_fifth_application";

    public static void main(String[] args) {
        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");  //earliest/latest/none

        //create consumer
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer(properties);

        //subscribe the consumer to Topic
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));

        //poll the consumer
        while(true){
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record:records){
                logger.info("Key: {} ,Value:{}",record.key(),record.value());
                logger.info("Partition: {} , Offset:{}",record.partition(),record.offset());
            }
        }
    }
}
