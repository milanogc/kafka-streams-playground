package com.milanogc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class TransactionsProducer {

    private static final String TOPIC_NAME = "transaction-input";
    private static final String NAMES[] = {"Francisco", "Graça", "Liana", "Mariana", "Melina", "Milano", "Milena"};
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.registerModule(new JavaTimeModule());
        MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Producer producer = new KafkaProducer(props);
        int index = -1;

        while (true) {
            long time = System.currentTimeMillis();

            for (int i = 0; i < 100; i++) {
                index = (index + 1) % NAMES.length;
                Transaction transaction = createTransaction(NAMES[index], ThreadLocalRandom.current().nextInt(0, 100), Instant.now());
                JsonNode jsonNode = MAPPER.valueToTree(transaction);
                System.out.println(jsonNode);
                ProducerRecord<String, JsonNode> record = new ProducerRecord<String, JsonNode>(TOPIC_NAME, transaction.getName(), jsonNode);
                producer.send(record);
            }

            long now = System.currentTimeMillis();

            try {
                Thread.sleep(1000 - (now - time));
            } catch (InterruptedException e) {
                break;
            }
        }

        producer.flush();
        producer.close();
    }

    private static Transaction createTransaction(String name, int amount, Instant time) {
        Transaction transaction = new Transaction();
        transaction.setName(name);
        transaction.setAmount(amount);
        transaction.setTime(time);
        return transaction;
    }

    public static class Transaction {

        private String name;
        private Integer amount;
        private Instant time;

        public Integer getAmount() {
            return amount;
        }

        public void setAmount(Integer amount) {
            this.amount = amount;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Instant getTime() {
            return time;
        }

        public void setTime(Instant time) {
            this.time = time;
        }

    }

}
