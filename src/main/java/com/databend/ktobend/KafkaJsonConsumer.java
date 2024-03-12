package com.databend.ktobend;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class KafkaJsonConsumer {
    private KafkaConsumer<String, String> consumer;

    public KafkaJsonConsumer() {
        // Kafka consumer configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList("json_topic"));
    }

    public KafkaConsumer<String, String> getConsumer() {
        return this.consumer;
    }

    public void closeConsumer() {
        this.consumer.close();
    }
}
