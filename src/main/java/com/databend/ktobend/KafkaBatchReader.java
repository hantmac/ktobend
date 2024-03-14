package com.databend.ktobend;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

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

    public ConsumerRecord<String, String> fetchMessageWithTimeout(Duration timeOut) {
        ConsumerRecords<String, String> records = consumer.getConsumer().poll(timeOut);
        if (records.isEmpty()) {
            return null;
        } else {
            return records.iterator().next();
        }
    }

    public MessagesBatch readBatch() {
        ConsumerRecord<String, String> lastRecord = null;
        long lastMessageOffset;
        long firstMessageOffset = 0;
        List<String> batchData = new ArrayList<>();
        Set<String> batches = new HashSet<>();

        while (true) {
            try {
                ConsumerRecord<String, String> record = fetchMessageWithTimeout(maxBatchInterval);
                if (record == null) {
                    break;
                }
                System.out.println("Received message: " + record.value() + " from offset: " + record.offset());
                firstMessageOffset = record.offset();
                String batchName = new BatchValue(record.value()).getBatch();
                Boolean isValueArray = new BatchValue(record.value()).isValueArray();
                if (isValueArray) {
                    batchData = new BatchValue(record.value()).getValueList();
                    batches.add(batchName);
                    lastRecord = record;
                    break;
                }
                String singleMessage = new BatchValue(record.value()).getValueJson();
                batchData.add(singleMessage.replace("\n", ""));
                batches.add(batchName);

                lastRecord = record;
                if ((batchData.size() >= batchSize)) {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (lastRecord == null) {
            return new MessagesBatch(batchData, batches, firstMessageOffset, 0);
        }
        lastMessageOffset = lastRecord.offset();
        return new MessagesBatch(batchData, batches, firstMessageOffset, lastMessageOffset);
    }

    public void close() {
        consumer.closeConsumer();
    }

}