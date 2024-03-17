package com.databend.ktobend;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.time.Instant;
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

    public ConsumerRecords<String, String> fetchMessageWithTimeout(Duration timeOut) {
        ConsumerRecords<String, String> records = consumer.getConsumer().poll(timeOut);
        if (records.isEmpty()) {
            return null;
        } else {
            return records;
        }
    }

    public MessagesBatch readBatch() {
        Instant start = Instant.now();
        ConsumerRecord<String, String> lastRecord = null;
        long lastMessageOffset;
        long firstMessageOffset = 0;
        List<String> batchData = new ArrayList<>();
        Set<String> batches = new HashSet<>();
        int internalSize = 0;


        while (internalSize <= batchSize) {
            try {
                ConsumerRecords<String, String> records = fetchMessageWithTimeout(maxBatchInterval);
                if (records == null) {
                    System.out.println("record is null");
                    break;
                }
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("internalsize" + internalSize + " Received data info: " + " from offset: " + record.offset());
                    internalSize++;
                    firstMessageOffset = record.offset();
                    String batchName = new BatchValue(record.value()).getBatch();
                    Boolean isValueArray = new BatchValue(record.value()).isValueArray();
                    if (isValueArray) {
                        batchData.addAll(new BatchValue(record.value()).getValueList());
                    } else {
                        String singleMessage = new BatchValue(record.value()).getValueJson();
                        batchData.add(singleMessage.replace("\n", ""));
                    }
                    batches.add(batchName);
                    lastRecord = record;
                    this.consumer.commitSync();
                }
                Instant end = Instant.now();
                long timeElapsed = Duration.between(start, end).getSeconds();
                if (timeElapsed >= 10) {
                    System.out.println("read batch time elapsed");
                    break;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("batchData size " + batchData.size());
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