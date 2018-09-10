package com.milanogc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColour {

    public static final String ANYTHING = "";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-favourite-colour");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream("streams-favourite-colour-input")
                .filter((nullKey, possibleUserAndColor) -> possibleUserAndColor.contains(","))
                .selectKey((nullKey, userAndColor) -> userAndColor.split(",")[0].toLowerCase())
                .mapValues(userAndColor -> userAndColor.split(",")[1].toLowerCase())
                .filter((user, colour) -> Arrays.asList("blue", "green", "red").contains(colour))
                .to("streams-favourite-colour-user-color", Produced.with(Serdes.String(), Serdes.String()));

        builder.<String, String>table("streams-favourite-colour-user-color")
                .groupBy((user, colour) -> KeyValue.pair(colour, ANYTHING))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count-colors-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .to("streams-favourite-colour-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        streams.localThreadsMetadata().forEach(data -> System.out.println(data));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
