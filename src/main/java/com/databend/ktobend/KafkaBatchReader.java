package com.databend.ktobend;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        List<String> batchData = new ArrayList<>();
        Set<String> batches = new HashSet<>();

        while (true) {
            try {
                ConsumerRecord<String, String> record = fetchMessageWithTimeout(maxBatchInterval);
                System.out.println("Received message: " + record.value() + " from offset: " + record.offset());
                firstMessageOffset = record.offset();
                String singleMessage = new BatchValue(record.value()).getValueJson();
                String batchName = new BatchValue(record.value()).getBatch();
                batchData.add(singleMessage.replace("\n", ""));
                batches.add(batchName);

                lastRecord = record;
                if (batchData.size() >= batchSize) {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        lastMessageOffset = lastRecord.offset();
        return new MessagesBatch(batchData, batches, firstMessageOffset, lastMessageOffset);
    }

    public void close() {
        consumer.closeConsumer();
    }

}