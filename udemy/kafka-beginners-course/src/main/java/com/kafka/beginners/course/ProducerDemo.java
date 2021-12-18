package com.kafka.beginners.course;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    private static final String KAFKA_TOPIC_NAME = "first_topic";

    public static void main(String[] args) {
        //prepare properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //prepare record to be sent
        ProducerRecord producerRecord = new ProducerRecord(KAFKA_TOPIC_NAME,"Hello world from java client "+
                                                            "Producer");
        //send data async
        producer.send(producerRecord);
        //flush the data
        producer.flush();
        producer.close();
    }
}
