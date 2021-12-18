package com.kafka.beginners.course;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final String KAFKA_TOPIC_NAME = "first_topic";
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) {
        //prepare properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //prepare record to be sent
        for(int i=0;i<10;i++) {
            String key = "id_"+i;
            ProducerRecord<String,String> producerRecord = new ProducerRecord(KAFKA_TOPIC_NAME, key,
                                                        "Hello world from java client Producer " + i);
            //send data async
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //this is called both for success and exceptions
                    if (e == null) {
                        logger.info("Record Metadeta Details: \n" +
                                "Key: {}\n"+
                                "Partition: {} \n" +
                                "Offset: {}\n" +
                                "TimeStamp: {}\n", key,recordMetadata.partition(),
                                recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        logger.error("Error ", e);
                    }
                }
            });
            //flush the data
            producer.flush();
        }
        producer.close();
    }
}
