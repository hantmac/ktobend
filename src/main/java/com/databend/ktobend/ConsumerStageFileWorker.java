package com.databend.ktobend;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ConsumerStageFileWorker {
    public KafkaJsonConsumer consumer;
    public Databendconn databendconn;
    public Integer fileSize;

    public ConsumerStageFileWorker() throws SQLException {
        this.consumer = new KafkaJsonConsumer(Config.getKafkaFileTopic(), Config.getKafkaConsumerGroupIdFile());
        this.fileSize = 10;
        this.databendconn = new Databendconn();
    }

    public void consumeFileAndCopyInto() throws Exception {
        List<String> files = new ArrayList<>();
        Instant start = Instant.now();
        String tableName = null;
        String batchInfo = null;
        List<String> batches = new ArrayList<>();
        while (files.size() <= fileSize) {
            try {
                ConsumerRecords<String, String> records = fetchMessageWithTimeout(java.time.Duration.ofSeconds(10));
                if (records == null) {
                    System.out.println("file records is null");
                    break;
                }
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Received file info: " + record.value() + " from offset: " + record.offset());
                    // split the record into tableName and fileName
                    String[] tableFileInfo = record.value().split(":");
                    tableName = tableFileInfo[0];
                    String fileName = tableFileInfo[1];
                    batches.add(tableFileInfo[2]);
                    files.add(fileName);
                }
                batchInfo = "'" + String.join("','", batches) + "'";
                this.consumer.commitSync();
                Instant end = Instant.now();
                long timeElapsed = Duration.between(start, end).getSeconds();
                if (timeElapsed >= Config.getDatabendInterval()) {
                    System.out.println("Time elapsed: " + timeElapsed + " seconds");
                    break;
                }
            } catch (Exception e) {
                System.err.println("An error occurred while consuming messages: " + e.getMessage());
                e.printStackTrace();
            }
        }
        if (tableName == null || files.isEmpty()) {
            System.out.println("No file to consume");
            return;
        }
        try {
            this.databendconn.copyInto(tableName, files);
            this.databendconn.mergeInto(batchInfo);
        } catch (Exception e) {
            System.err.println("An error occurred while copying data into the table: " + e.getMessage());
            e.printStackTrace();
            // Decide what to do when an exception occurs. For example, you can stop the execution:
            // throw new RuntimeException("An error occurred while copying data into the table", e);
        }
    }

    public ConsumerRecords<String, String> fetchMessageWithTimeout(Duration timeOut) {
        ConsumerRecords<String, String> records = consumer.getConsumer().poll(timeOut);
        if (records.isEmpty()) {
            return null;
        } else {
            return records;
        }
    }

    public void close() {
        this.consumer.closeConsumer();
    }

    public void run() {
        while (true) {
            try {
                consumeFileAndCopyInto();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
