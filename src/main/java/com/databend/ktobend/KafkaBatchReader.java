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

    public ConsumerRecords<String, String> fetchMessageWithTimeout(Duration timeOut) {
        ConsumerRecords<String, String> records = consumer.getConsumer().poll(timeOut);
        if (records.isEmpty()) {
            System.out.println("record is empty");
            return null;
        } else {
            return records;
        }
    }

    public MessagesBatch readBatch() {
        ConsumerRecord<String, String> lastRecord = null;
        long lastMessageOffset;
        long firstMessageOffset = 0;
        List<String> batchData = new ArrayList<>();
        Set<String> batches = new HashSet<>();

//        while (true) {
        try {
            ConsumerRecords<String, String> records = fetchMessageWithTimeout(maxBatchInterval);
//                if (records == null) {
//                    System.out.println("data record is null");
//                    break;
//                }
            System.out.println("Received data count: " + records.count());
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received data info: " + " from offset: " + record.offset());
                firstMessageOffset = record.offset();
                String batchName = new BatchValue(record.value()).getBatch();
                Boolean isValueArray = new BatchValue(record.value()).isValueArray();
                if (isValueArray) {
//                    batchData = new BatchValue(record.value()).getValueList();
                    batchData.addAll(new BatchValue(record.value()).getValueList());
                } else {
                    String singleMessage = new BatchValue(record.value()).getValueJson();
                    batchData.add(singleMessage.replace("\n", ""));
                }
                batches.add(batchName);
                lastRecord = record;
                this.consumer.commitSync();
            }
            if ((batchData.size() >= batchSize)) {
                if (lastRecord == null) {
                    return new MessagesBatch(batchData, batches, firstMessageOffset, 0);
                }
                lastMessageOffset = lastRecord.offset();
                return new MessagesBatch(batchData, batches, firstMessageOffset, lastMessageOffset);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
//        }
//        if (lastRecord == null) {
//            return new MessagesBatch(batchData, batches, firstMessageOffset, 0);
//        }
//        lastMessageOffset = lastRecord.offset();
//        return new MessagesBatch(batchData, batches, firstMessageOffset, lastMessageOffset);
        return new MessagesBatch(new ArrayList<>(), batches, firstMessageOffset, 0);
    }

    public void close() {
        consumer.closeConsumer();
    }

}