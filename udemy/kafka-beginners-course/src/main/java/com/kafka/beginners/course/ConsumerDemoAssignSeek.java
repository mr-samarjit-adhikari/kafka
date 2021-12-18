package com.kafka.beginners.course;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Assign and Seek is used for data replay and fetch a specific message
 * GroupId is not needed.
 * No need to subscribe the topic as we are already assigning to the TopicPartition
 */
public class ConsumerDemoAssignSeek {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");  //earliest/latest/none

        //create consumer
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer(properties);

        //Assign
        TopicPartition partition = new TopicPartition(TOPIC,0); //partition 0
        kafkaConsumer.assign(Collections.singleton(partition));

        //Seek the data
        kafkaConsumer.seek(partition,15L); //read from offset 15L

        //poll the consumer
        boolean keepsOnReading = true;
        int numberOfMessageReadSofar = 0;
        while(keepsOnReading){
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record:records){
                numberOfMessageReadSofar += 1;
                logger.info("Key: {} ,Value:{}",record.key(),record.value());
                logger.info("Partition: {} , Offset:{}",record.partition(),record.offset());

                if (numberOfMessageReadSofar>5){
                    keepsOnReading = false;
                    break;
                }
            }

            logger.info("Exiting the applications");
        }
    }
}
