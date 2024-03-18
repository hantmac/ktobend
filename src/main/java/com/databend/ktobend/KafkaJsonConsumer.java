package com.databend.ktobend;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class KafkaJsonConsumer {
    private KafkaConsumer<String, String> consumer;

    public KafkaJsonConsumer(String topic, String groupId) {
        // Kafka consumer configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.getKafkaBootstrapServers());
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "60000");
//        props.put("max.poll.interval.ms", "60000");
        props.put("max.poll.records",Config.getKafkaMaxPollRecords());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaConsumer<String, String> getConsumer() {
        return this.consumer;
    }

    public void commitSync() {
        this.consumer.commitSync();
    }

    public void closeConsumer() {
        this.consumer.close();
    }
}
