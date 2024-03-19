package com.databend.ktobend;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;

public class KafkaStringProducer {
    private KafkaProducer<String, String> producer;

    public KafkaStringProducer() {
        // Kafka producer configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.getKafkaBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // SASL/SCRAM configuration
//        props.put("security.protocol", "SASL_PLAINTEXT");
//        props.put("sasl.mechanism", "SCRAM-SHA-256");
//        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-username\" password=\"my-password\";");


        this.producer = new KafkaProducer<>(props);
    }

    public void sendStringToKafka(String topic, String message) {
        this.producer.send(new ProducerRecord<>(topic, message));
    }

    public void closeProducer() {
        this.producer.close();
    }
}
