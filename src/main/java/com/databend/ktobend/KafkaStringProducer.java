package com.databend.ktobend;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaStringProducer {
    private KafkaProducer<String, String> producer;

    public KafkaStringProducer() {
        // Kafka producer configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.getKafkaBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(props);
    }

    public void sendStringToKafka(String topic, String message) {
        this.producer.send(new ProducerRecord<>(topic, message));
    }

    public void closeProducer() {
        this.producer.close();
    }
}
