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
        this.fileSize = Config.getDatabendFileBatchSize();
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
                    System.out.println(Instant.now() + ": Received file info: " + record.value() + " from offset: " + record.offset());
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

        int maxRetries = 3; // Set the maximum number of retries
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                this.databendconn.copyInto(tableName, files);
                break; // If successful, break out of the loop
            } catch (Exception e) {
                System.err.println("An error occurred: " + e.getMessage());
                if (attempt == maxRetries - 1) { // If this was the last attempt
                    System.err.println("Failed to execute after " + maxRetries + " attempts.");
                    e.printStackTrace();
                } else {
                    System.err.println("Retrying (" + (attempt + 2) + "/" + maxRetries + ")...");
                }
            }
        }
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                this.databendconn.mergeInto(batchInfo);
                break; // If successful, break out of the loop
            } catch (Exception e) {
                System.err.println("An error occurred: " + e.getMessage());
                if (attempt == maxRetries - 1) { // If this was the last attempt
                    System.err.println("Failed to execute after " + maxRetries + " attempts.");
                    e.printStackTrace();
                } else {
                    System.err.println("Retrying (" + (attempt + 2) + "/" + maxRetries + ")...");
                }
            }
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
