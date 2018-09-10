package com.milanogc;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class TableAvro {

  public static void main(String[] args) throws IOException {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-table-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    //props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

    final Schema invoiceKeySchema = new Schema.Parser().parse(TableAvro.class.getClassLoader().getResourceAsStream("invoice_key.avsc"));
    final Schema invoiceValueSchema = new Schema.Parser().parse(TableAvro.class.getClassLoader().getResourceAsStream("invoice_value.avsc"));

    // When you want to override serdes explicitly/selectively
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url","http://localhost:8081");
    final Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
    keyGenericAvroSerde.configure(serdeConfig, true); // `true` for record keys
    final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
    valueGenericAvroSerde.configure(serdeConfig, false); // `false` for record values

    final StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> stream = builder.<String, String>stream("streams-table-input", Consumed.with(Serdes.String(), Serdes.String()));
    KStream<String, String>[] branches = stream.branch(
      (key, value) -> !"0".equals(key), // records to keep
      (key, value) -> true // records to delete
    );

    branches[0]
      .map((key, value) -> {
        GenericRecord recordKey = new GenericData.Record(invoiceKeySchema);
        recordKey.put("invoice_key", key);

        GenericRecord recordValue = new GenericData.Record(invoiceValueSchema);
        recordValue.put("invoice_id", key);
        return KeyValue.pair(recordKey, recordValue);
      })
      .to("streams-table-output", Produced.with(keyGenericAvroSerde, valueGenericAvroSerde));

    branches[1]
      .flatMap((key, value) ->
        Arrays.stream(value.split(" "))
          .map(v -> {
            GenericRecord record = new GenericData.Record(invoiceKeySchema);
            record.put("invoice_key", v);
            return KeyValue.pair(record, null);
          })
          .collect(Collectors.toList()))
      .to("streams-table-output", Produced.with(keyGenericAvroSerde, null));

    //builder.table("streams-table-output", Materialized.as("table-store"));

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    // only do this in dev - not in prod
    streams.cleanUp();
    streams.start();
    streams.localThreadsMetadata().forEach(data -> System.out.println(data));
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
