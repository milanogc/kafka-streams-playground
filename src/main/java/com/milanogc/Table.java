package com.milanogc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

public class Table {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-table-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    //props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

    final StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> stream = builder.<String, String>stream("streams-table-input", Consumed.with(Serdes.String(), Serdes.String()));
    KStream<String, String>[] branches = stream.branch(
      (key, value) -> !"0".equals(key), // records to keep
      (key, value) -> true // records to delete
    );

    branches[0]
      .mapValues((key, value) -> key)
      .to("streams-table-output", Produced.with(Serdes.String(), Serdes.String()));

    branches[1]
      .flatMap((key, value) ->
        Arrays.stream(value.split(" "))
          .map(v -> KeyValue.pair(v, null))
          .collect(Collectors.toList()))
      .to("streams-table-output", Produced.with(Serdes.String(), null));

    //builder.table("streams-table-output", Materialized.as("table-store"));

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    // only do this in dev - not in prod
    streams.cleanUp();
    streams.start();
    streams.localThreadsMetadata().forEach(data -> System.out.println(data));
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
