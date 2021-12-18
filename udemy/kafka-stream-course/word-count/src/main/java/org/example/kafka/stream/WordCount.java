package org.example.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {
    public static void main(String[] args) {
        //create new stream Properties - 5 properties
        Properties configProperties = new Properties();
        configProperties.put(StreamsConfig.APPLICATION_ID_CONFIG,"word-count-app");
        configProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9091");
        configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        configProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        //1 stream from builder
        KStream<String,String> wordCountInputSource = builder.stream("word-count-input");
        //2 map the value to lower case

        KTable<String,Long> wordCounts = wordCountInputSource
                .mapValues(textLine->textLine.toLowerCase())
                .flatMapValues((lowerCaseTextInput)-> Arrays.asList(lowerCaseTextInput.split("\\W+")))
                .selectKey((key,value)->value)
                .groupByKey()
                .count();
        //now write it to kafka output topic
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(),Serdes.Long())); //Kafka 3.0

        //bind together
        Topology topology = builder.build();
        KafkaStreams stream = new KafkaStreams(topology,configProperties);
        stream.start();

        //print the topology
        System.out.println(topology.describe());

        //graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }
}
