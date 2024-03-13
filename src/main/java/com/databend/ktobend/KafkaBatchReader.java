package com.databend.ktobend;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class KafkaBatchReader {

    public KafkaJsonConsumer consumer;
    public Integer batchSize;
    public Duration maxBatchInterval;

    public KafkaBatchReader(KafkaJsonConsumer consumer, Integer batchSize, Duration maxBatchInterval) {
        this.consumer = consumer;
        this.batchSize = batchSize;
        this.maxBatchInterval = maxBatchInterval;
    }

    public ConsumerRecord<String, String> fetchMessageWithTimeout(Duration timeOut) throws Exception {
        try {
            return consumer.getConsumer().poll(timeOut).iterator().next();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public MessagesBatch readBatch() {
        ConsumerRecord<String, String> lastRecord;
        long lastMessageOffset;
        long firstMessageOffset;
        List<String> batch = new ArrayList<>();

        while (true) {
            try {
                ConsumerRecord<String,String> record = fetchMessageWithTimeout(maxBatchInterval);
                System.out.println("Received message: " + record.value() + " from offset: " + record.offset());
                firstMessageOffset = record.offset();
                batch.add(record.value().replace("\n", ""));
                lastRecord = record;
                if (batch.size() > batchSize) {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        lastMessageOffset = lastRecord.offset();
        return new MessagesBatch(batch, firstMessageOffset, lastMessageOffset);
    }

    public void close() {
        consumer.closeConsumer();
    }

}