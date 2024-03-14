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
        this.consumer = new KafkaJsonConsumer(Config.getKafkaFileTopic());
        this.fileSize = Config.getDatabendBatchSize();
        this.databendconn = new Databendconn();
    }

    public void consumeFileAndCopyInto() throws Exception {
        List<String> files = new ArrayList<>();
        Instant start = Instant.now();
        while (true) {
            try {
                ConsumerRecord<String, String> record = fetchMessageWithTimeout(java.time.Duration.ofSeconds(100));
                if (record == null) {
                    break;
                }
                System.out.println("Received message: " + record.value() + " from offset: " + record.offset());
                // split the record into tableName and fileName
                String[] tableFileInfo = record.value().split(":");
                String tableName = tableFileInfo[0];
                String fileName = tableFileInfo[1];
                String batchInfo = tableFileInfo[2];
                files.add(fileName);
                Instant end = Instant.now();
                long timeElapsed = Duration.between(start, end).getSeconds();
                // 时间维度和 filesize 两个条件，满足一个就执行 copy and merge into
                if (files.size() >= fileSize || timeElapsed >= Config.getDatabendInterval()){
                    this.databendconn.copyInto(tableName, files);
                    this.databendconn.mergeInto(batchInfo);
                    break;
                }
            } catch (Exception e) {
                throw new Exception(e);
            }
        }
    }

    public ConsumerRecord<String, String> fetchMessageWithTimeout(Duration timeOut) {
        ConsumerRecords<String, String> records = consumer.getConsumer().poll(timeOut);
        if (records.isEmpty()) {
            return null;
        } else {
            return records.iterator().next();
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
