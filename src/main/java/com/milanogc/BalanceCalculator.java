package com.milanogc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.Properties;

public class BalanceCalculator {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-transaction");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        //props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        final StreamsBuilder builder = new StreamsBuilder();

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        final JsonNode initialBalance = newBalance(0, 0, Instant.ofEpochMilli(0L));

        builder.<String, JsonNode>stream("transaction-input", Consumed.with(Serdes.String(), jsonSerde))
                .groupByKey()
                .aggregate(
                        () -> initialBalance,
                        (key, newTransaction, currentBalance) -> updateBalance(currentBalance, newTransaction),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("transaction-agg")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde))
                .toStream()
                .to("transaction-output", Produced.with(Serdes.String(), jsonSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        streams.localThreadsMetadata().forEach(data -> System.out.println(data));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static JsonNode updateBalance(JsonNode currentBalance, JsonNode newTransaction) {
        Long currentBalanceEpoch = Instant.parse(currentBalance.get("time").asText()).toEpochMilli();
        Long newTransactionEpoch = Instant.parse(newTransaction.get("time").asText()).toEpochMilli();
        return newBalance(
                currentBalance.get("count").asInt() + 1,
                currentBalance.get("balance").asInt() + newTransaction.get("amount").asInt(),
                Instant.ofEpochMilli(Math.max(currentBalanceEpoch, newTransactionEpoch))
        );
    }

    private static JsonNode newBalance(int count, int balance, Instant time) {
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", count);
        newBalance.put("balance", balance);
        newBalance.put("time", time.toString());
        return newBalance;
    }
}
